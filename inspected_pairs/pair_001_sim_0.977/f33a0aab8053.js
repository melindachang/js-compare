# backend/app/services/relay_deployer.py
"""
Relay Deployer
==============
SSHes into the user's OCI instance and deploys the order relay
service automatically. Called from relay_routes.py when the user
clicks "Deploy Relay" in the Connections UI.

No manual steps needed on the OCI side after this runs.
"""

import io
import textwrap
import os
import json
import secrets
import time
import threading

import requests
_session = requests.Session()

from pathlib import Path
from typing import Tuple, Optional

from app.event_bus.audit_logger import write_audit_log
from app.event_bus.inapp_events import (
    record_alert,
    record_alert_once,
    clear_alert_once,
    is_alert_active,
)

# --------------------------------------------------
# AUTO RECOVERY STATE
# --------------------------------------------------

_relay_fail_count  = 0
_last_recovery_ts  = 0
_RECOVERY_LOCK     = threading.Lock()

_MAX_FAIL_BEFORE_RECOVERY = 3
_RECOVERY_COOLDOWN_S      = 120

RELAY_CONFIG_PATH = Path.home() / ".scalp-app" / "relay_config.json"

_relay_states = {}   # host -> True/False
_active_relay = None
_state_lock = threading.Lock()
_last_unreachable_log = {}   # host -> last log timestamp

# Edge-triggered in-app alert key for the aggregate "all relays down" state.
# Used by _run_one_relay_check so the bell gets ONE alert when every relay goes
# down (not one per 30s poll), and ONE recovery alert when a relay comes back.
_RELAY_DOWN_KEY = "relay_down"

# --------------------------------------------------
# RELAY SERVICE SOURCE
# --------------------------------------------------

RELAY_SERVICE_SOURCE = """
#!/usr/bin/env python3
# oci_order_relay.py — Scalp Terminal Order Relay (HARDENED 2026)
# Pure stdlib HTTP server with anti-de-scheduling keepwarm

import http.server
import socketserver
import json
import hmac
import os
import sys
import logging
import time
import urllib.parse
import threading as _threading
import hashlib

import requests as _req
_orig = _req.Session.request
def _timed(self, method, url, **kw):
    if kw.get("timeout") is None:
        kw["timeout"] = (3, 8)
    return _orig(self, method, url, **kw)
_req.Session.request = _timed

from kiteconnect import KiteConnect

RELAY_SECRET = os.environ.get("RELAY_SECRET", "")
if not RELAY_SECRET:
    sys.exit("RELAY_SECRET not set")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("relay")


def _verify(headers):
    auth = headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return False
    return hmac.compare_digest(auth[7:].strip(), RELAY_SECRET)


def _kite(api_key, access_token):
    k = KiteConnect(api_key=api_key)
    k.set_access_token(access_token)
    return k


class Handler(http.server.BaseHTTPRequestHandler):

    def log_message(self, fmt, *args):
        pass

    def _json(self, code, data):
        body = json.dumps(data).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _body(self):
        n = int(self.headers.get("Content-Length", 0))
        return json.loads(self.rfile.read(n)) if n else {}

    def do_GET(self):
        if self.path == "/health":
            self._json(200, {
                "status": "ok",
                "relay": "scalp-terminal",
                "ts": int(time.time()),
            })
        else:
            self._json(404, {"error": "not found"})

    def do_POST(self):
        if not _verify(self.headers):
            self._json(403, {"error": "forbidden"})
            return
        try:
            body = self._body()
        except Exception as e:
            self._json(400, {"error": f"bad request: {e}"})
            return

        api_key      = body.get("api_key", "")
        access_token = body.get("access_token", "")
        if not api_key or not access_token:
            self._json(400, {"error": "missing credentials"})
            return

        k    = _kite(api_key, access_token)
        path = self.path

        try:
            if path == "/relay/place_order":
                kw = {f: body[f] for f in (
                    "variety", "exchange", "tradingsymbol",
                    "transaction_type", "quantity", "order_type", "product"
                )}
                for opt in ("price", "trigger_price", "tag"):
                    if body.get(opt) is not None:
                        kw[opt] = body[opt]
                oid = k.place_order(**kw)
                log.info("[ORDER] %s %s qty=%s id=%s",
                         body.get("tradingsymbol"), body.get("transaction_type"),
                         body.get("quantity"), oid)
                self._json(200, {"order_id": oid})

            elif path == "/relay/place_gtt":
                res = k.place_gtt(
                    trigger_type=body["trigger_type"],
                    tradingsymbol=body["tradingsymbol"],
                    exchange=body["exchange"],
                    trigger_values=body["trigger_values"],
                    last_price=body["last_price"],
                    orders=body["orders"],
                )
                tid = res.get("trigger_id", res) if isinstance(res, dict) else res
                log.info("[GTT] %s gtt_id=%s", body.get("tradingsymbol"), tid)
                self._json(200, {"trigger_id": tid})

            elif path == "/relay/cancel_order":
                k.cancel_order(variety=body["variety"], order_id=body["order_id"])
                self._json(200, {"cancelled": body["order_id"]})

            else:
                self._json(404, {"error": "unknown endpoint"})

        except Exception as e:
            log.error("[ERROR] %s: %s", path, e)
            self._json(500, {"error": str(e)})

    def do_DELETE(self):
        if not _verify(self.headers):
            self._json(403, {"error": "forbidden"})
            return
        if not self.path.startswith("/relay/gtt/"):
            self._json(404, {"error": "not found"})
            return
        try:
            raw = self.path.split("?", 1)
            tid = int(raw[0].split("/")[-1])
            qs  = urllib.parse.parse_qs(raw[1]) if len(raw) > 1 else {}
            _kite(
                qs.get("api_key", [""])[0],
                qs.get("access_token", [""])[0],
            ).delete_gtt(tid)
            self._json(200, {"cancelled": tid})
        except Exception as e:
            self._json(500, {"error": str(e)})


from threading import BoundedSemaphore

_MAX_CONCURRENT = 10
_sema = BoundedSemaphore(_MAX_CONCURRENT)


class ThreadingRelay(socketserver.ThreadingMixIn, http.server.HTTPServer):
    daemon_threads      = True
    allow_reuse_address = True

    def process_request_thread(self, request, client_address):
        acquired = _sema.acquire(blocking=False)
        if not acquired:
            try:
                response = (
                    b"HTTP/1.1 503 Service Unavailable\\r\\n"
                    b"Content-Type: application/json\\r\\n"
                    b"Content-Length: 23\\r\\n"
                    b"Connection: close\\r\\n"
                    b"\\r\\n"
                    b"{\\"error\\":\\"server_busy\\"}"
                )
                request.sendall(response)
            except Exception:
                pass
            request.close()
            return
        try:
            super().process_request_thread(request, client_address)
        finally:
            _sema.release()


def _keepwarm():
    # Pure sleep — keeps the process visible to the OS scheduler so OCI
    # free-tier CPU throttle cannot de-schedule it for minutes at a time.
    # Zero file I/O, zero CPU cost. time.sleep(5) is all that's needed.
    while True:
        time.sleep(5)


if __name__ == "__main__":
    _threading.Thread(target=_keepwarm, daemon=True, name="keepwarm").start()
    server = ThreadingRelay(("0.0.0.0", 8001), Handler)
    log.info("Scalp relay listening on 0.0.0.0:8001 (hardened mode)")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
"""


# --------------------------------------------------
# RELAY CONFIG CACHE
# --------------------------------------------------

_relay_cfg: Optional[dict] = None


def _load_relay_config() -> Optional[dict]:
    global _relay_cfg

    if _relay_cfg is not None:
        return _relay_cfg if _relay_cfg else None

    if not RELAY_CONFIG_PATH.exists():
        _relay_cfg = False
        return None

    try:
        cfg = json.loads(RELAY_CONFIG_PATH.read_text())
        if cfg.get("enabled") and cfg.get("relays") and cfg.get("secret"):
            _relay_cfg = cfg
            write_audit_log(f"[RELAY] Order relay ENABLED → {cfg['url']}")
            return _relay_cfg
        else:
            write_audit_log("[RELAY] relay_config.json present but disabled or incomplete")
            _relay_cfg = False
            return None
    except Exception as e:
        write_audit_log(f"[RELAY] Failed to load relay_config.json ERR={e}")
        _relay_cfg = False
        return None



def _invalidate_relay_cache():
    global _relay_cfg
    _relay_cfg = None
    write_audit_log("[RELAY] Config cache invalidated — will reload on next order")


# --------------------------------------------------
# HEALTH CHECK
# --------------------------------------------------

_HEALTH_TIMEOUT_S = 5   # generous — survives OCI jitter without false positives


def _check_relay_health_with_retry(url: str) -> bool:
    try:
        resp = _session.get(f"{url}/health", timeout=_HEALTH_TIMEOUT_S)
        if resp.status_code == 200:
            body = resp.json()
            return (
                body.get("relay") == "scalp-terminal"
                or body.get("status") == "ok"
            )
        if resp.status_code == 503:
            write_audit_log("[RELAY] Health check — relay returned 503 (overloaded)")
    except Exception as e:
        import time

        now = time.time()
        last = _last_unreachable_log.get(url, 0)

        if now - last > 30:
            write_audit_log(f"[RELAY] Health check failed: {e}")
            _last_unreachable_log[url] = now
    return False


# --------------------------------------------------
# TELEGRAM HELPERS
# --------------------------------------------------

def _get_telegram_credentials() -> tuple:
    try:
        from app.api.telegram_api import TELEGRAM_CONFIG
        if TELEGRAM_CONFIG:
            t = TELEGRAM_CONFIG.get("bot_token", "").strip()
            c = TELEGRAM_CONFIG.get("chat_id", "").strip()
            if t and c:
                return t, c
    except Exception:
        pass

    try:
        from app.utils.app_paths import APP_HOME
        for p in [APP_HOME / "telegram_config.json",
                   APP_HOME / "config" / "telegram_config.json"]:
            if not p.exists():
                continue
            try:
                cfg = json.loads(p.read_text())
                t = cfg.get("bot_token", "").strip()
                c = cfg.get("chat_id", "").strip()
                if t and c:
                    return t, c
            except Exception:
                continue
    except Exception:
        pass

    return None, None


def _send_relay_telegram(message: str):
    bot_token, chat_id = _get_telegram_credentials()
    if not bot_token or not chat_id:
        return

    try:
        resp = requests.post(
            f"https://api.telegram.org/bot{bot_token}/sendMessage",
            json={"chat_id": chat_id, "text": message, "parse_mode": "HTML"},
            timeout=10,
        )
        if resp.ok:
            write_audit_log("[RELAY] Telegram message delivered")
        else:
            write_audit_log(f"[RELAY] Telegram API {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        write_audit_log(f"[RELAY] Telegram send failed: {e}")


def _notify_relay_down(host: str):
    write_audit_log("[RELAY] Sending relay-down Telegram alert...")
    _send_relay_telegram(
        f"🔴 <b>Order Relay Unreachable</b>\n\n"
        f"Host: <code>{host}</code>\n"
        f"Auto-recovery in progress. Direct fallback active — orders NOT dropped.\n\n"
        f"If not recovered in 5 min, check OCI console (instance must be Running)."
    )


def _notify_relay_recovered(host: str):
    write_audit_log("[RELAY] Sending relay-recovered Telegram alert...")
    _send_relay_telegram(
        f"✅ <b>Order Relay Recovered</b>\n\n"
        f"Host: <code>{host}</code>\n"
        f"Relay is healthy again — orders will route normally."
    )


# --------------------------------------------------
# TRANSITION TRACKER
# --------------------------------------------------

def _handle_health_result(is_active: bool, host: str):
    global _relay_states, _active_relay

    previous = _relay_states.get(host)
    _relay_states[host] = is_active

    # Identify primary
    try:
        cfg = json.loads(RELAY_CONFIG_PATH.read_text())
        relays = cfg.get("relays", [])
        primary_host = next((r["host"] for r in relays if r.get("is_primary")), None)
    except:
        primary_host = None

    # INIT
    if previous is None:
        write_audit_log(f"[RELAY_MONITOR] INIT {host} → {'UP' if is_active else 'DOWN'}")

        if is_active and _active_relay is None:
            _active_relay = host
            write_audit_log(f"[RELAY] Initial active relay → {host}")

        return

    # 🔴 DOWN
    if previous is True and not is_active:
        write_audit_log(f"[RELAY_MONITOR] DOWN {host}")

        # PRIMARY DOWN
        if host == primary_host:
            write_audit_log("[RELAY_FAILOVER] PRIMARY DOWN → switching to secondary")

            # find secondary
            for h, state in _relay_states.items():
                if h != primary_host and state:
                    _active_relay = h
                    _send_relay_telegram(
                        f"🔴 <b>Primary Relay Down</b>\n\n"
                        f"Primary: <code>{primary_host}</code>\n"
                        f"Using Secondary: <code>{h}</code>\n\n"
                        f"Orders continue via backup."
                    )
                    return

            # no secondary
            _active_relay = None
            _send_relay_telegram(
                f"🚨 <b>ALL RELAYS DOWN</b>\n\n"
                f"Primary: <code>{primary_host}</code>\n"
                f"No backup available.\n\n"
                f"Orders WILL FAIL. Check immediately."
            )

        return

    # 🟢 UP
    if previous is False and is_active:
        write_audit_log(f"[RELAY_MONITOR] UP {host}")

        # PRIMARY RECOVERY
        if host == primary_host:
            if _active_relay != primary_host:
                _active_relay = primary_host
                _send_relay_telegram(
                    f"✅ <b>Primary Relay Restored</b>\n\n"
                    f"Primary: <code>{primary_host}</code>\n"
                    f"Switched back from secondary.\n\n"
                    f"Normal routing resumed."
                )
            return

        # secondary up → ignore (no noise)
        return


# --------------------------------------------------
# BACKGROUND MONITOR
# --------------------------------------------------

MONITOR_INTERVAL_S = 30  # 30s is plenty — recovery fires after 3 consecutive failures


def start_relay_monitor():
    t = threading.Thread(target=_relay_monitor_loop, daemon=True, name="RelayMonitor")
    t.start()
    write_audit_log("[RELAY_MONITOR] Background monitor started")


def _relay_monitor_loop():
    time.sleep(3)
    while True:
        try:
            _run_one_relay_check()
        except Exception as e:
            write_audit_log(f"[RELAY_MONITOR][ERROR] {e}")
        time.sleep(MONITOR_INTERVAL_S)


def _run_one_relay_check():
    if not RELAY_CONFIG_PATH.exists():
        return

    try:
        cfg = json.loads(RELAY_CONFIG_PATH.read_text())
    except Exception:
        return

    if not cfg.get("enabled"):
        return

    relays = cfg.get("relays", [])
    if not relays:
        return

    global _relay_fail_count, _last_recovery_ts

    any_active = False

    # 🔥 PRIORITIZE PRIMARY FIRST
    primary = None
    secondary = []

    for r in relays:
        if r.get("is_primary"):
            primary = r
        else:
            secondary.append(r)

    ordered_relays = []
    if primary:
        ordered_relays.append(primary)
    ordered_relays.extend(secondary)

    for relay in ordered_relays:
        url = relay["url"]
        host = relay.get("host", url)

        is_active = _check_relay_health_with_retry(url)
        _handle_health_result(is_active, host)

        import time

        if is_active:
            any_active = True
            _last_unreachable_log.pop(host, None)  # reset when recovered
        else:
            now = time.time()
            last = _last_unreachable_log.get(host, 0)

            # 🔥 log only once every 30 seconds
            if now - last > 30:
                write_audit_log(f"[RELAY_MONITOR] {host} unreachable")
                _last_unreachable_log[host] = now

    if any_active:
        _relay_fail_count = 0
        # If we were previously in the "all relays down" alerted state, this is
        # the recovery edge: fire ONE in-app recovery alert and re-arm the key.
        if is_alert_active(_RELAY_DOWN_KEY):
            record_alert(
                "RELAY_UP",
                "Order relay back online — live orders can route to the broker again.",
                severity="info",
            )
            clear_alert_once(_RELAY_DOWN_KEY)
        return

    # ALL relays failed
    _relay_fail_count += 1
    write_audit_log(f"[RELAY_MONITOR] ALL RELAYS DOWN fail_count={_relay_fail_count}")

    # 🔔 In-app alert — edge-triggered: fires ONCE when all relays first go
    # down, then suppressed (this runs every 30s) until a relay recovers in the
    # any_active branch above, which clears the key and re-arms it.
    record_alert_once(
        _RELAY_DOWN_KEY,
        "RELAY_DOWN",
        "All order relays are DOWN — live orders cannot reach the broker. "
        "Auto-recovery is attempting to restart the relay; a direct fallback "
        "may apply. Verify any open positions.",
        severity="error",
    )

    if _relay_fail_count >= _MAX_FAIL_BEFORE_RECOVERY:
        write_audit_log("[RELAY_MONITOR] Triggering recovery for ALL relays")

        for relay in relays:
            _attempt_relay_recovery(
                host=relay.get("host"),
                ssh_username=relay.get("ssh_username"),
                ssh_private_key_text=relay.get("ssh_key"),
                instance_id=relay.get("instance_id"),
            )

        _relay_fail_count = 0


# --------------------------------------------------
# STATUS CHECK
# --------------------------------------------------

def get_relay_status() -> dict:
    if not RELAY_CONFIG_PATH.exists():
        return {"configured": False, "active": False}

    try:
        cfg = json.loads(RELAY_CONFIG_PATH.read_text())
    except Exception:
        return {"configured": False, "active": False}

    if not cfg.get("enabled"):
        return {"configured": True, "active": False, "host": cfg.get("host")}

    statuses = []

    for relay in cfg.get("relays", []):
        url = relay["url"]
        host = relay.get("host")

        active = False
        try:
            resp = _session.get(f"{url}/health", timeout=_HEALTH_TIMEOUT_S)
            if resp.ok:
                active = True
        except:
            pass

        statuses.append({
            "host": host,
            "active": active
        })

    return {
        "configured": True,
        "active": any(r["active"] for r in statuses),
        "relays": statuses
    }


def disable_relay():
    if not RELAY_CONFIG_PATH.exists():
        return
    cfg = json.loads(RELAY_CONFIG_PATH.read_text())
    cfg["enabled"] = False
    RELAY_CONFIG_PATH.write_text(json.dumps(cfg, indent=2))

    try:
        from app.execution.zerodha_executor import _invalidate_relay_cache
        _invalidate_relay_cache()
    except Exception:
        pass

    # Reset per-relay health state so monitor re-establishes baseline
    with _state_lock:
        _relay_states.clear()
        global _active_relay
        _active_relay = None
    # Clear any active relay-down alert so a re-enable starts clean.
    clear_alert_once(_RELAY_DOWN_KEY)


# --------------------------------------------------
# DEPLOY
# --------------------------------------------------

def deploy_relay(
    host: str,
    ssh_username: str,
    ssh_private_key_text: str,
    instance_id: str = None,
    progress_callback=None,
) -> Tuple[bool, str]:

    def progress(msg: str):
        write_audit_log(f"[RELAY_DEPLOY] {msg}")
        if progress_callback:
            progress_callback(msg)

    try:
        import paramiko
    except ImportError:
        return False, "paramiko not installed. Run: pip install paramiko --break-system-packages"

    relay_secret = secrets.token_hex(32)

    progress(f"Connecting to {host}...")

    try:
        pkey = _load_private_key(ssh_private_key_text)
    except Exception as e:
        return False, f"Could not parse SSH private key: {e}"

    client = None
    last_connect_err = None

    for attempt in range(1, 6):
        _client = paramiko.SSHClient()
        _client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            _client.connect(
                hostname=host,
                username=ssh_username,
                pkey=pkey,
                timeout=60,
                banner_timeout=60,
                auth_timeout=30,
                look_for_keys=False,
                allow_agent=False,
            )
            client = _client
            last_connect_err = None
            break
        except paramiko.AuthenticationException:
            _client.close()
            return False, (
                "SSH authentication failed. "
                "Make sure you are using the correct private key (.key file) "
                "for this OCI instance, and that the SSH username is correct "
                "(use 'opc' for Oracle Linux, 'ubuntu' for Ubuntu)."
            )
        except Exception as e:
            _client.close()
            last_connect_err = e
            if attempt < 6:
                progress(f"SSH connect attempt {attempt} failed ({e}), retrying in 8s...")
                time.sleep(8)

    if client is None:
        return False, (
            f"Could not connect to {host} after 5 attempts: {last_connect_err}. "
            "Checklist:\n"
            "1. IP address is correct (Networking tab in OCI console)\n"
            "2. Port 22 Ingress Rule exists in your OCI Security List\n"
            "3. Instance status is Running (not Stopped)\n"
            "4. SSH username is 'opc' for Oracle Linux, 'ubuntu' for Ubuntu"
        )

    progress("Connected. Setting up relay service...")

    try:
        progress("Uploading relay service...")

        ok, _, err = _run(
            client,
            "sudo mkdir -p /opt/scalp-relay && "
            f"sudo chown {ssh_username}:{ssh_username} /opt/scalp-relay",
            timeout=15,
        )
        if not ok:
            return False, f"Failed to create relay directory: {err}"

        sftp = client.open_sftp()
        with sftp.open("/tmp/oci_order_relay.py", "w") as f:
            f.write(RELAY_SERVICE_SOURCE)
        sftp.close()

        ok, _, err = _run(
            client,
            "sudo mv /tmp/oci_order_relay.py /opt/scalp-relay/oci_order_relay.py",
            timeout=10,
        )
        if not ok:
            return False, f"Failed to move relay file: {err}"

        progress("Checking Python installation...")

        _, py3_path, _ = _run(client, "which python3 || which python3.9 || which python3.11", timeout=10)
        py3_bin = py3_path.strip().split("\n")[0].strip() or "python3"

        _, pip_path, _ = _run(client, "which pip3 || which pip", timeout=10)
        pip_present = bool(pip_path.strip())

        _, venv_check, _ = _run(client, f"{py3_bin} -m venv --help 2>&1 | head -1", timeout=10)
        venv_present = "usage" in venv_check.lower() or "optional" in venv_check.lower()

        progress(f"Python: {py3_bin}  pip: {'yes' if pip_present else 'no'}  venv: {'yes' if venv_present else 'no'}")

        if not pip_present or not venv_present:
            _, pkg_out, _ = _run(client, "which dnf || which yum || which apt-get", timeout=10)
            pkg_bin = pkg_out.strip().split("\n")[0].strip()

            if "apt" in pkg_bin:
                ok, _, err = _run(
                    client,
                    "sudo apt-get update -qq && sudo apt-get install -y -qq python3 python3-pip python3-venv",
                    timeout=150,
                )
                if not ok:
                    return False, f"Failed to install system packages: {err}"
            elif "dnf" in pkg_bin or "yum" in pkg_bin:
                if not pip_present:
                    ok, _, err = _run(
                        client,
                        f"sudo {pkg_bin} install -y -q python3-pip "
                        "--disablerepo='*' --enablerepo='ol*_baseos*,ol*_appstream*,baseos,appstream' "
                        "--setopt=timeout=20 --setopt=retries=1",
                        timeout=90,
                    )
                    if not ok:
                        progress("dnf pip install failed, bootstrapping pip via get-pip.py...")
                        ok, _, err = _run(
                            client,
                            "curl -sS https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py && "
                            f"sudo {py3_bin} /tmp/get-pip.py --quiet",
                            timeout=60,
                        )
                        if not ok:
                            return False, f"Failed to install pip: {err}"
            else:
                return False, (
                    "Could not detect a package manager (apt/dnf/yum). "
                    "Make sure your OCI instance is running Ubuntu 22.04 or Oracle Linux 8/9."
                )
        else:
            progress("Python and pip already installed — skipping system packages")

        progress("Creating Python virtual environment...")
        ok, _, err = _run(client, f"{py3_bin} -m venv /opt/scalp-relay/venv", timeout=30)
        if not ok:
            _, pkg_out, _ = _run(client, "which dnf || which yum || which apt-get", timeout=10)
            pkg_bin = pkg_out.strip().split("\n")[0].strip()
            if "apt" in pkg_bin:
                _run(client, "sudo apt-get install -y -qq python3-venv", timeout=60)
            ok, _, err = _run(client, f"{py3_bin} -m venv /opt/scalp-relay/venv", timeout=30)
            if not ok:
                return False, f"Failed to create Python venv: {err}"

        progress("Installing relay Python packages...")
        ok, _, err = _run(
            client,
            "/opt/scalp-relay/venv/bin/pip install --quiet kiteconnect requests",
            timeout=120,
        )
        if not ok:
            return False, f"Failed to install Python packages: {err}"

        progress("Creating system service...")

        # CRITICAL: No RuntimeMaxSec here.
        # The previous version had RuntimeMaxSec=10800 which caused systemd
        # to kill the relay process after exactly 3 hours — this was the
        # root cause of the recurring 2h38m uptime pattern.
        service_content = textwrap.dedent(f"""
            [Unit]
            Description=Scalp Terminal Order Relay
            After=network.target

            [Service]
            Type=simple
            User={ssh_username}
            WorkingDirectory=/opt/scalp-relay
            Environment=RELAY_SECRET={relay_secret}
            ExecStart=/opt/scalp-relay/venv/bin/python3 /opt/scalp-relay/oci_order_relay.py
            Restart=always
            RestartSec=2
            TimeoutStartSec=10
            TimeoutStopSec=5
            LimitNOFILE=65535
            MemoryHigh=400M
            MemoryMax=600M
            CPUQuota=80%

            [Install]
            WantedBy=multi-user.target
        """).lstrip()

        with client.open_sftp() as sftp:
            with sftp.open("/tmp/scalp-relay.service", "w") as f:
                f.write(service_content)

        # Enable sysrq for force-reboot fallback in auto-recovery
        _run(client, "echo 'kernel.sysrq = 1' | sudo tee -a /etc/sysctl.conf", timeout=10)
        _run(client, "sudo sysctl -p", timeout=10)

        _run(client, "sudo systemctl stop scalp-relay 2>/dev/null || true", timeout=15)

        ok, _, err = _run(
            client,
            "sudo cp /tmp/scalp-relay.service /etc/systemd/system/scalp-relay.service "
            "&& sudo systemctl daemon-reload "
            "&& sudo systemctl enable scalp-relay "
            "&& sudo systemctl start scalp-relay",
            timeout=30,
        )
        if not ok:
            return False, f"Failed to start relay service: {err}"

        progress("Opening firewall port 8001...")

        _, fw_out, _ = _run(
            client,
            "sudo systemctl is-active firewalld 2>/dev/null || echo inactive",
            timeout=10,
        )
        if fw_out.strip() == "active":
            _run(
                client,
                "sudo firewall-cmd --permanent --add-port=8001/tcp && sudo firewall-cmd --reload",
                timeout=20,
            )
            progress("Opened port 8001 via firewalld")
        else:
            _run(
                client,
                "sudo iptables -C INPUT -p tcp --dport 8001 -j ACCEPT 2>/dev/null "
                "|| sudo iptables -A INPUT -p tcp --dport 8001 -j ACCEPT",
                timeout=10,
            )
            progress("Opened port 8001 via iptables")

        # --------------------------------------------------
        # WATCHDOG (INSTALLED BUT DISABLED)
        # --------------------------------------------------
        # We keep watchdog files for future use, but DO NOT enable it.
        # Backend handles failover → avoids flapping / false restarts.

        progress("Installing watchdog (disabled — backend managed)...")

        watchdog_script = (
            "#!/bin/bash\n"
            "LOG=/var/log/scalp-relay-watchdog.log\n"
            "\n"
            "check() {\n"
            "  for i in {1..3}; do\n"
            "    CODE=$(curl -s --max-time 5 -o /dev/null -w '%{http_code}' http://localhost:8001/health 2>/dev/null)\n"
            "    [ \"$CODE\" = \"200\" ] && return 0\n"
            "    sleep 1\n"
            "  done\n"
            "  return 1\n"
            "}\n"
            "\n"
            "if ! check; then\n"
            "  echo \"$(date): relay unhealthy — restarting\" >> \"$LOG\"\n"
            "  /bin/systemctl restart scalp-relay\n"
            "  echo \"$(date): restart issued\" >> \"$LOG\"\n"
            "fi\n"
        )

        watchdog_service = (
            "[Unit]\n"
            "Description=Scalp Terminal Relay Watchdog\n"
            "\n"
            "[Service]\n"
            "Type=oneshot\n"
            "ExecStart=/opt/scalp-relay/watchdog.sh\n"
        )

        watchdog_timer = (
            "[Unit]\n"
            "Description=Scalp Terminal Relay Watchdog Timer (every 60s)\n"
            "\n"
            "[Timer]\n"
            "OnBootSec=60\n"
            "OnUnitActiveSec=60\n"
            "AccuracySec=5\n"
            "\n"
            "[Install]\n"
            "WantedBy=timers.target\n"
        )

        timer_ok  = True
        timer_err = ""

        try:
            sftp = client.open_sftp()
            sftp.open("/tmp/scalp-relay-watchdog.sh",      "w").write(watchdog_script)
            sftp.open("/tmp/scalp-relay-watchdog.service", "w").write(watchdog_service)
            sftp.open("/tmp/scalp-relay-watchdog.timer",   "w").write(watchdog_timer)
            sftp.close()
        except Exception as e:
            timer_ok = False; timer_err = f"SFTP write: {e}"

        if timer_ok:
            ok, _, e = _run(
                client,
                "sudo cp /tmp/scalp-relay-watchdog.sh /opt/scalp-relay/watchdog.sh "
                "&& sudo chmod +x /opt/scalp-relay/watchdog.sh "
                "&& sudo cp /tmp/scalp-relay-watchdog.service "
                "    /etc/systemd/system/scalp-relay-watchdog.service "
                "&& sudo cp /tmp/scalp-relay-watchdog.timer "
                "    /etc/systemd/system/scalp-relay-watchdog.timer "
                "&& sudo systemctl daemon-reload",
                timeout=15,
            )
            if not ok:
                timer_ok = False; timer_err = f"install: {e}"

        # 🔥 CRITICAL: ALWAYS DISABLE watchdog (no auto-restart)
        _run(
            client,
            "sudo systemctl stop scalp-relay-watchdog.timer 2>/dev/null || true "
            "&& sudo systemctl disable scalp-relay-watchdog.timer 2>/dev/null || true",
            timeout=10,
        )

        # Clean any old keepalive mechanisms
        _run(
            client,
            "sudo systemctl disable --now scalp-relay-keepalive.service 2>/dev/null || true "
            "&& sudo systemctl disable --now scalp-relay-keepalive.timer 2>/dev/null || true "
            "&& sudo rm -f /etc/systemd/system/scalp-relay-keepalive.* "
            "/opt/scalp-relay/keepalive.sh 2>/dev/null || true",
            timeout=10,
        )

        # Clean cron leftovers
        _run(
            client,
            "crontab -l 2>/dev/null | grep -v scalp-relay | crontab - 2>/dev/null || true",
            timeout=10,
        )

        if not timer_ok:
            progress(f"Warning: watchdog install failed ({timer_err}) — relay works fine without it")
        else:
            progress("Watchdog installed but DISABLED — backend controls failover (recommended)")

        progress("Waiting for relay to start...")
        time.sleep(4)

        ok, out, err = _run(
            client,
            "curl -s --max-time 5 http://localhost:8001/health",
            timeout=15,
        )

        if not ok or "scalp-terminal" not in out:
            _, logs, _ = _run(
                client,
                "sudo journalctl -u scalp-relay --since '1 min ago' --no-pager -n 20",
                timeout=10,
            )
            return False, f"Relay started but health check failed. Service logs:\n{logs}"

        progress("Relay is healthy on OCI instance.")

    except Exception as e:
        return False, f"Deployment error: {e}"

    finally:
        client.close()

    progress("Saving relay configuration...")

    # 🔥 BUILD RELAY CONFIG ENTRY
    relay_entry = {
        "host": host,
        "url": f"http://{host}:8001",
        "secret": relay_secret,
        "ssh_username": ssh_username,
        "ssh_key": ssh_private_key_text,
        "instance_id": instance_id,
    }

    # 🔥 LOAD EXISTING CONFIG
    if RELAY_CONFIG_PATH.exists():
        cfg = json.loads(RELAY_CONFIG_PATH.read_text())
    else:
        cfg = {"enabled": True, "relays": []}

    # 🔥 REPLACE OR ADD RELAY
    updated = False
    for r in cfg["relays"]:
        if r["host"] == host:
            r.update(relay_entry)
            updated = True
            break

    if not updated:
        cfg["relays"].append(relay_entry)

    # 🔥 MARK PRIMARY (FIRST ONE)
    for i, r in enumerate(cfg["relays"]):
        r["is_primary"] = (i == 0)

    # 🔥 SAVE
    cfg["enabled"] = True
    RELAY_CONFIG_PATH.write_text(json.dumps(cfg, indent=2))

    # 🔥 IMPORTANT: INVALIDATE CACHE
    try:
        from app.execution.zerodha_executor import _invalidate_relay_cache
        _invalidate_relay_cache()
    except:
        pass

    return True, (
        f"Relay deployed successfully at {host}. "
        f"All order placement will now route through your static IP."
    )


# --------------------------------------------------
# AUTO RECOVERY
# --------------------------------------------------

def _attempt_relay_recovery(host, ssh_username, ssh_private_key_text, instance_id=None):
    """
    Recovery ladder (fastest to most disruptive):
      1. systemctl restart scalp-relay  — ~5s downtime, preferred
      2. graceful OS reboot             — ~2-3 min, last resort
      3. sysrq force reboot             — emergency fallback
    """
    write_audit_log("[RELAY_RECOVERY] Attempting recovery...")


    if not ssh_username or not ssh_private_key_text:
        write_audit_log("[RELAY_RECOVERY] No SSH credentials in config — cannot recover")
        return

    try:
        import paramiko

        pkey = _load_private_key(ssh_private_key_text)
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        write_audit_log("[RELAY_RECOVERY] Connecting via SSH...")
        client.connect(
            hostname=host,
            username=ssh_username,
            pkey=pkey,
            timeout=20,
            banner_timeout=20,
            auth_timeout=10,
            look_for_keys=False,
            allow_agent=False,
        )

        # Step 1: service restart (fast, preferred)
        write_audit_log("[RELAY_RECOVERY] Trying systemctl restart scalp-relay...")
        ok, _, err = _run(client, "sudo systemctl restart scalp-relay", timeout=15)
        if ok:
            time.sleep(5)
            ok2, out, _ = _run(
                client, "curl -s --max-time 4 http://localhost:8001/health", timeout=10
            )
            if ok2 and "scalp-terminal" in out:
                write_audit_log("[RELAY_RECOVERY] Service restart successful")
                client.close()
                return
            write_audit_log("[RELAY_RECOVERY] Still unhealthy after restart — escalating to reboot")
        else:
            write_audit_log(f"[RELAY_RECOVERY] systemctl restart failed: {err}")

        # Step 2: OCI reboot (fast trigger)
        if instance_id:
            try:
                import subprocess
                subprocess.run(
                    [
                        "oci", "compute", "instance", "action",
                        "--instance-id", instance_id,
                        "--action", "RESET",
                        "--force"
                    ],
                    timeout=10,
                )
                write_audit_log(f"[RELAY_RECOVERY] OCI reboot triggered for {host}")
                return
            except Exception as e:
                write_audit_log(f"[RELAY_RECOVERY] OCI reboot failed: {e}")

        # Step 3: graceful OS reboot
        write_audit_log("[RELAY_RECOVERY] Trying graceful reboot...")
        try:
            _run(client, "sudo shutdown -r now", timeout=5)
            client.close()
            write_audit_log("[RELAY_RECOVERY] Graceful reboot issued — instance back in ~2 min")
            return
        except Exception as e:
            write_audit_log(f"[RELAY_RECOVERY] Graceful reboot failed: {e}")

        # Step 4: sysrq force reboot
        write_audit_log("[RELAY_RECOVERY] Trying force reboot (sysrq)...")
        try:
            _run(client, "echo b | sudo tee /proc/sysrq-trigger", timeout=5)
            client.close()
            write_audit_log("[RELAY_RECOVERY] Force reboot issued")
            return
        except Exception as e:
            write_audit_log(f"[RELAY_RECOVERY] Force reboot failed: {e}")

        client.close()

    except Exception as e:
        write_audit_log(f"[RELAY_RECOVERY] SSH connection failed: {e}")

    write_audit_log("[RELAY_RECOVERY] All recovery methods failed")


def _get_instance_ocid_from_config() -> str:
    if not RELAY_CONFIG_PATH.exists():
        raise ValueError("relay_config.json not found")
    cfg = json.loads(RELAY_CONFIG_PATH.read_text())
    instance_id = cfg.get("instance_id")
    if not instance_id:
        raise ValueError("instance_id missing in relay_config.json")
    return instance_id


# --------------------------------------------------
# HELPERS
# --------------------------------------------------

def _run(client, cmd: str, timeout: int = 30):
    import time as _time
    try:
        transport = client.get_transport()
        channel = transport.open_session()
        channel.set_combine_stderr(False)
        channel.settimeout(5.0)
        channel.exec_command(cmd)

        deadline = _time.time() + timeout
        stdout_buf = b""
        stderr_buf = b""

        while True:
            while channel.recv_ready():
                chunk = channel.recv(4096)
                if chunk:
                    stdout_buf += chunk
            while channel.recv_stderr_ready():
                chunk = channel.recv_stderr(4096)
                if chunk:
                    stderr_buf += chunk
            if channel.exit_status_ready():
                break
            if _time.time() > deadline:
                channel.close()
                return False, "", f"Command timed out after {timeout}s: {cmd[:80]}"
            _time.sleep(0.5)

        while channel.recv_ready():
            stdout_buf += channel.recv(4096)
        while channel.recv_stderr_ready():
            stderr_buf += channel.recv_stderr(4096)

        exit_code = channel.recv_exit_status()
        out = stdout_buf.decode("utf-8", errors="replace").strip()
        err = stderr_buf.decode("utf-8", errors="replace").strip()
        return exit_code == 0, out, err

    except Exception as e:
        return False, "", str(e)


def _load_private_key(key_text: str):
    import paramiko
    key_text = key_text.strip()

    if hasattr(paramiko.pkey.PKey, "from_private_key"):
        try:
            return paramiko.pkey.PKey.from_private_key(io.StringIO(key_text))
        except Exception:
            pass

    key_classes = [paramiko.RSAKey, paramiko.ECDSAKey, paramiko.Ed25519Key]
    dss = getattr(paramiko, "DSSKey", None)
    if dss:
        key_classes.append(dss)

    last_err = None
    for key_class in key_classes:
        try:
            return key_class.from_private_key(io.StringIO(key_text))
        except Exception as e:
            last_err = e
    raise ValueError(
        "Could not parse SSH key. Paste the full key including "
        "-----BEGIN ... PRIVATE KEY----- and -----END ... PRIVATE KEY----- lines. "
        f"Last error: {last_err}"
    )