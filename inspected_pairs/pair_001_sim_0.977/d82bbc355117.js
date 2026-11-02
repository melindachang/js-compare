"""
Billing Engine — APScheduler daily job untuk auto-billing WL licenses.
Berjalan sebagai systemd service terpisah di Central_Server.
"""

import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta

import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

from license_server.license_manager import LicenseManager

load_dotenv()

logger = logging.getLogger(__name__)

BOT_TOKEN: str = os.environ.get("BOT_TOKEN", "")
TELEGRAM_API_URL = "https://api.telegram.org/bot{token}/sendMessage"

GRACE_PERIOD_DAYS = 3  # hari sebelum suspend


# ------------------------------------------------------------------
# Telegram helper
# ------------------------------------------------------------------

async def _send_telegram(http: httpx.AsyncClient, chat_id: int, text: str) -> None:
    """Kirim pesan Telegram via bot pusat. Non-fatal jika gagal."""
    if not BOT_TOKEN:
        logger.warning("_send_telegram: BOT_TOKEN not set, skipping notification.")
        return
    try:
        url = TELEGRAM_API_URL.format(token=BOT_TOKEN)
        resp = await http.post(url, json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"})
        resp.raise_for_status()
    except Exception:
        logger.warning("_send_telegram: failed to send to chat_id=%d — non-fatal", chat_id, exc_info=True)


# ------------------------------------------------------------------
# Grace period suspension check
# ------------------------------------------------------------------

async def _check_and_suspend_grace(
    license_manager: LicenseManager,
    http: httpx.AsyncClient,
    wl_id: str,
    admin_telegram_id: int,
) -> bool:
    """
    Cek apakah WL sudah grace_period > GRACE_PERIOD_DAYS.
    Jika ya: UPDATE status ke 'suspended', kirim notifikasi, return True.
    Return False jika belum perlu suspend.
    """
    client = await license_manager._get_client()

    # Ambil entry pertama (terlama) di wl_billing_history dengan status 'failed'
    res = (
        await client.table("wl_billing_history")
        .select("created_at")
        .eq("wl_id", wl_id)
        .eq("status", "failed")
        .order("created_at", desc=False)
        .limit(1)
        .execute()
    )

    rows = res.data or []
    if not rows:
        return False

    first_failed_at_str: str = rows[0]["created_at"]
    # Parse ISO 8601 timestamp dari Supabase
    first_failed_at = datetime.fromisoformat(first_failed_at_str.replace("Z", "+00:00"))
    now = datetime.now(timezone.utc)
    days_in_grace = (now - first_failed_at).total_seconds() / 86400

    if days_in_grace <= GRACE_PERIOD_DAYS:
        return False

    # Suspend
    try:
        await client.table("wl_licenses").update({"status": "suspended"}).eq("wl_id", wl_id).execute()
        logger.info("_check_and_suspend_grace: wl_id=%s suspended after %.1f days in grace_period", wl_id, days_in_grace)
    except Exception:
        logger.exception("_check_and_suspend_grace: failed to suspend wl_id=%s", wl_id)
        return False

    # Kirim notifikasi
    msg = (
        f"🚫 *Lisensi Whitelabel Disuspend*\n\n"
        f"WL ID: `{wl_id}`\n"
        f"Lisensi Anda telah disuspend karena saldo tidak mencukupi selama lebih dari {GRACE_PERIOD_DAYS} hari.\n\n"
        f"Silakan top-up saldo USDT untuk mengaktifkan kembali."
    )
    await _send_telegram(http, admin_telegram_id, msg)
    return True


# ------------------------------------------------------------------
# Main billing cycle
# ------------------------------------------------------------------

async def run_billing_cycle() -> None:
    """
    Jalankan satu siklus billing:
    1. Query semua WL dengan status IN ('active', 'grace_period') dan expires_at <= NOW()
    2. Untuk setiap WL yang sudah grace_period > 3 hari: suspend
    3. Untuk WL lainnya: call debit_billing(), kirim warning jika jadi grace_period
    4. Log ringkasan: total, sukses, gagal, suspended
    """
    logger.info("run_billing_cycle: starting billing cycle at %s UTC", datetime.now(timezone.utc).isoformat())

    license_manager = LicenseManager()
    client = await license_manager._get_client()
    http = httpx.AsyncClient(timeout=15.0)

    total = 0
    success_count = 0
    failed_count = 0
    suspended_count = 0

    try:
        now_iso = datetime.now(timezone.utc).isoformat()

        res = (
            await client.table("wl_licenses")
            .select("wl_id, admin_telegram_id, status")
            .in_("status", ["active", "grace_period"])
            .lte("expires_at", now_iso)
            .execute()
        )

        licenses = res.data or []
        total = len(licenses)
        logger.info("run_billing_cycle: found %d WL(s) due for billing", total)

        for row in licenses:
            wl_id: str = row["wl_id"]
            admin_telegram_id: int = row["admin_telegram_id"]
            current_status: str = row["status"]

            # Cek grace period suspension terlebih dahulu
            if current_status == "grace_period":
                suspended = await _check_and_suspend_grace(license_manager, http, wl_id, admin_telegram_id)
                if suspended:
                    suspended_count += 1
                    continue

            # Jalankan billing
            try:
                result = await license_manager.debit_billing(wl_id)
                new_status: str = result.get("new_status", "")

                if result.get("success"):
                    success_count += 1
                    logger.info(
                        "run_billing_cycle: wl_id=%s billed successfully, new expires_at=%s",
                        wl_id,
                        result.get("expires_at"),
                    )
                else:
                    failed_count += 1
                    logger.warning(
                        "run_billing_cycle: wl_id=%s billing failed, status=%s balance_before=%.2f",
                        wl_id,
                        new_status,
                        result.get("balance_before", 0),
                    )

                    # Kirim warning Telegram jika status menjadi grace_period
                    if new_status == "grace_period":
                        msg = (
                            f"⚠️ *Peringatan Lisensi Whitelabel*\n\n"
                            f"WL ID: `{wl_id}`\n"
                            f"Saldo tidak mencukupi untuk perpanjangan lisensi.\n"
                            f"Saldo saat ini: `{result.get('balance_before', 0):.2f} USDT`\n\n"
                            f"Lisensi Anda masuk masa tenggang (*grace period*). "
                            f"Segera top-up dalam {GRACE_PERIOD_DAYS} hari untuk menghindari suspensi."
                        )
                        await _send_telegram(http, admin_telegram_id, msg)

            except Exception:
                failed_count += 1
                logger.exception("run_billing_cycle: unexpected error billing wl_id=%s — skipping", wl_id)

    except Exception:
        logger.exception("run_billing_cycle: failed to query licenses")
    finally:
        await http.aclose()

    logger.info(
        "run_billing_cycle: SUMMARY — total=%d success=%d failed=%d suspended=%d",
        total,
        success_count,
        failed_count,
        suspended_count,
    )


# ------------------------------------------------------------------
# Scheduler setup
# ------------------------------------------------------------------

def create_scheduler() -> AsyncIOScheduler:
    """Buat dan konfigurasi AsyncIOScheduler dengan CronTrigger daily 00:00 UTC."""
    scheduler = AsyncIOScheduler()
    scheduler.add_job(
        run_billing_cycle,
        trigger=CronTrigger(hour=0, minute=0, timezone="UTC"),
        id="billing_cycle",
        name="Daily Billing Cycle",
        misfire_grace_time=3600,  # toleransi 1 jam jika scheduler sempat mati
    )
    return scheduler


# ------------------------------------------------------------------
# Entry point
# ------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    required_vars = ["SUPABASE_URL", "SUPABASE_SERVICE_KEY", "MASTER_SEED_MNEMONIC"]
    for var in required_vars:
        if not os.environ.get(var):
            raise RuntimeError(f"Environment variable {var!r} is not set.")

    if not BOT_TOKEN:
        logger.warning("BOT_TOKEN not set — Telegram notifications will be disabled.")

    scheduler = create_scheduler()
    scheduler.start()
    logger.info("Billing scheduler started. Next run: daily at 00:00 UTC.")

    try:
        asyncio.get_event_loop().run_forever()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Billing scheduler shutting down.")
        scheduler.shutdown()
