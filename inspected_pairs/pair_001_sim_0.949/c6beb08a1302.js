"""
Notification Tools — TelegramPublisher.

Publishes trading signals, evaluation summaries, strategy updates, and
degradation alerts to a Telegram channel. Telegram is treated as a
notification sink, not the source of record. All deliveries are logged to
telegram_deliveries.jsonl regardless of success or failure.

Idempotency: a prediction_id can be published only once. If an existing
delivery record exists for the same prediction_id + message_type, the call
is skipped and the existing record is returned.

Required env vars:
    TELEGRAM_BOT_TOKEN  — bot token from @BotFather
    TELEGRAM_CHANNEL_ID — target channel (e.g. -100xxxxxxxxx)

Optional:
    TELEGRAM_THREAD_ID  — message thread id for forum channels
"""

import json
import re
import uuid
from datetime import UTC, datetime
from html import escape, unescape
from typing import Any

import structlog

log = structlog.get_logger()

# Message type constants
MSG_SIGNAL = "signal"
MSG_EVALUATION = "evaluation"
MSG_STRATEGY_UPDATE = "strategy_update"
MSG_DEGRADATION_ALERT = "degradation_alert"


# =============================================================================
# TELEGRAM PUBLISHER
# =============================================================================


class TelegramPublisher:
    """
    Publishes structured messages to a Telegram channel.

    All public methods are async, persist delivery metadata, and retry once
    on transient HTTP errors before marking the delivery as failed.
    """

    def __init__(self):
        from config.settings import get_config

        cfg = get_config()
        self._token = cfg.telegram.bot_token
        self._channel = cfg.telegram.channel_id
        self._thread_id = cfg.telegram.thread_id or None
        self._enabled = cfg.telegram.is_configured()

    # -------------------------------------------------------------------------
    # PUBLIC MESSAGE TYPES
    # -------------------------------------------------------------------------

    async def publish_signal(self, prediction: dict) -> dict:
        """
        Publish an actionable BUY/SELL signal to Telegram.

        Args:
            prediction: Full prediction record (must include signal, symbol, etc.)

        Returns:
            Delivery record persisted to telegram_deliveries.jsonl.
        """
        prediction_id = prediction.get("prediction_id", str(uuid.uuid4())[:8])
        text = _format_signal_message(prediction)
        return await self._deliver(
            message_type=MSG_SIGNAL,
            reference_id=prediction_id,
            text=text,
            metadata={"symbol": prediction.get("symbol"), "signal": prediction.get("signal")},
        )

    async def publish_evaluation_summary(self, evaluation: dict) -> dict:
        """
        Publish a prediction evaluation result to Telegram.

        Args:
            evaluation: Scored evaluation dict from evaluate_prediction().
        """
        prediction_id = evaluation.get("prediction_id", str(uuid.uuid4())[:8])
        text = _format_evaluation_message(evaluation)
        return await self._deliver(
            message_type=MSG_EVALUATION,
            reference_id=prediction_id,
            text=text,
            metadata={
                "symbol": evaluation.get("symbol"),
                "outcome_score": evaluation.get("outcome_score"),
            },
        )

    async def publish_strategy_update(self, version: dict) -> dict:
        """
        Publish a strategy parameter change notification.

        Args:
            version: Strategy version record with prev_params, new_params, reason.
        """
        version_id = version.get("version_id", str(uuid.uuid4())[:8])
        text = _format_strategy_update_message(version)
        return await self._deliver(
            message_type=MSG_STRATEGY_UPDATE,
            reference_id=version_id,
            text=text,
            metadata={"lifecycle": version.get("lifecycle")},
        )

    async def publish_degradation_alert(self, kpis: dict, reasons: list[str]) -> dict:
        """
        Publish a performance degradation alert.

        Args:
            kpis: Rolling KPI dict from compute_rolling_kpis().
            reasons: Human-readable list of threshold violations.
        """
        alert_id = str(uuid.uuid4())[:8]
        text = _format_degradation_alert(kpis, reasons)
        return await self._deliver(
            message_type=MSG_DEGRADATION_ALERT,
            reference_id=alert_id,
            text=text,
            metadata={"kpi_window": kpis.get("window"), "reason_count": len(reasons)},
        )

    # -------------------------------------------------------------------------
    # DELIVERY ENGINE
    # -------------------------------------------------------------------------

    async def _deliver(
        self,
        message_type: str,
        reference_id: str,
        text: str,
        metadata: dict | None = None,
    ) -> dict:
        """Send a message and persist the delivery record."""
        from memory.store import get_memory_store

        store = get_memory_store()

        # Idempotency guard: skip if already delivered successfully
        existing = await self._find_existing_delivery(store, message_type, reference_id)
        if existing:
            log.info(
                "telegram.delivery_skipped_duplicate",
                message_type=message_type,
                reference_id=reference_id,
            )
            return existing

        delivery_id = str(uuid.uuid4())[:8]
        telegram_message_id = None
        success = False
        error = None

        if not self._enabled:
            log.warning(
                "telegram.not_configured",
                message_type=message_type,
                reference_id=reference_id,
            )
            error = "telegram_not_configured"
        else:
            # Attempt delivery with one retry
            for attempt in range(2):
                try:
                    telegram_message_id = await self._send_message(text)
                    success = True
                    break
                except Exception as exc:
                    error = str(exc)
                    log.warning(
                        "telegram.send_failed",
                        attempt=attempt + 1,
                        message_type=message_type,
                        reference_id=reference_id,
                        error=error,
                    )

        delivery = {
            "delivery_id": delivery_id,
            "message_type": message_type,
            "reference_id": reference_id,
            "sent_at": datetime.now(UTC).isoformat(),
            "success": success,
            "telegram_message_id": telegram_message_id,
            "error": error,
            "channel_id": self._channel,
            "metadata": metadata or {},
            "text_preview": text[:200],
        }

        await store.save_telegram_delivery(delivery)

        if success:
            log.info(
                "telegram.delivered",
                message_type=message_type,
                reference_id=reference_id,
                telegram_message_id=telegram_message_id,
            )
        else:
            log.error(
                "telegram.delivery_failed",
                message_type=message_type,
                reference_id=reference_id,
                error=error,
            )

        return delivery

    async def _send_message(self, text: str) -> int | None:
        """
        POST to Telegram Bot API sendMessage endpoint.
        Returns the Telegram message_id on success.
        """
        import urllib.error
        import urllib.request

        url = f"https://api.telegram.org/bot{self._token}/sendMessage"
        payload = self._build_payload(text=text, parse_mode="HTML")
        try:
            body = self._post_json(url, payload, urllib.request, urllib.error)
        except RuntimeError as exc:
            error_text = str(exc).lower()
            if "parse entities" in error_text or "can't parse" in error_text:
                fallback_payload = self._build_payload(
                    text=_plain_text_fallback(text),
                    parse_mode=None,
                )
                body = self._post_json(url, fallback_payload, urllib.request, urllib.error)
            else:
                raise

        if body.get("ok"):
            return body["result"]["message_id"]
        raise RuntimeError(f"Telegram API error: {body}")

    def _build_payload(self, text: str, parse_mode: str | None) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "chat_id": self._channel,
            "text": text,
        }
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if self._thread_id:
            payload["message_thread_id"] = int(self._thread_id)
        return payload

    def _post_json(self, url: str, payload: dict[str, Any], request_mod, error_mod) -> dict:
        data = json.dumps(payload).encode("utf-8")
        req = request_mod.Request(
            url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with request_mod.urlopen(req, timeout=10) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except error_mod.HTTPError as exc:
            body_text = exc.read().decode("utf-8", errors="replace")
            try:
                body = json.loads(body_text)
            except json.JSONDecodeError:
                raise RuntimeError(f"Telegram HTTP {exc.code}: {body_text}") from exc
            description = body.get("description") or body
            raise RuntimeError(f"Telegram HTTP {exc.code}: {description}") from exc

    async def _find_existing_delivery(
        self, store, message_type: str, reference_id: str
    ) -> dict | None:
        deliveries = await store.get_telegram_deliveries()
        for d in reversed(deliveries):
            if (
                d.get("message_type") == message_type
                and d.get("reference_id") == reference_id
                and d.get("success")
            ):
                return d
        return None


# =============================================================================
# MESSAGE FORMATTERS
# =============================================================================


def _format_signal_message(prediction: dict) -> str:
    signal = _html_safe(prediction.get("signal", "?"))
    symbol = _html_safe(prediction.get("symbol", "?"))
    tf = _html_safe(prediction.get("timeframe", "?"))
    conf = prediction.get("confidence", "?")
    entry = prediction.get("entry_price", "?")
    sl = prediction.get("stop_loss", "?")
    tp1 = prediction.get("take_profit_1", "?")
    tp2 = prediction.get("take_profit_2", "?")
    rr = prediction.get("risk_reward_ratio", "?")
    regime = _html_safe(prediction.get("regime", ""))
    confluence = _html_safe(prediction.get("confluence_analysis", ""))
    pid = _html_safe(prediction.get("prediction_id", "?"))
    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")

    direction_icon = "🟢" if signal == "BUY" else "🔴" if signal == "SELL" else "⚪"

    lines = [
        f"{direction_icon} <b>{signal} {symbol}</b> [{tf}]",
        f"🕐 {now}",
        f"Confidence: <b>{conf}%</b>",
        "",
        f"Entry:  <code>{entry}</code>",
        f"SL:     <code>{sl}</code>",
        f"TP1:    <code>{tp1}</code>",
        f"TP2:    <code>{tp2}</code>",
        f"R/R:    {rr}",
    ]
    sizing = prediction.get("position_sizing") or {}
    if sizing.get("size_units"):
        lines += [
            f"Size:   <code>{sizing.get('size_units')}</code> "
            f"(~{sizing.get('notional')}, {sizing.get('notional_pct')}% eq)",
            f"Risk:   {sizing.get('risk_amount')} ({sizing.get('risk_pct_effective')}% eq)",
        ]
    if regime:
        lines += ["", f"Regime: {regime}"]
    if confluence:
        lines += ["", f"Confluence: {str(confluence)[:300]}"]
    lines += ["", f"<i>ID: {pid}</i>"]
    return "\n".join(lines)


def _format_evaluation_message(evaluation: dict) -> str:
    pid = _html_safe(evaluation.get("prediction_id", "?"))
    symbol = _html_safe(evaluation.get("symbol", "?"))
    signal = _html_safe(evaluation.get("signal", "?"))
    direction_ok = evaluation.get("direction_correct")
    tp1 = evaluation.get("tp1_reached")
    tp2 = evaluation.get("tp2_reached")
    sl_first = evaluation.get("sl_reached_first")
    mfe = evaluation.get("mfe_pct", "?")
    mae = evaluation.get("mae_pct", "?")
    score = evaluation.get("outcome_score", "?")
    notes = _html_safe(evaluation.get("evaluation_notes", ""))

    outcome_icon = "✅" if (tp1 or direction_ok) and not sl_first else "❌"

    lines = [
        f"{outcome_icon} <b>Evaluation — {symbol} {signal}</b>",
        f"Score: <b>{score}</b>",
        "",
        f"Direction correct: {'✓' if direction_ok else '✗'}",
        f"TP1 reached:       {'✓' if tp1 else '✗'}",
        f"TP2 reached:       {'✓' if tp2 else '✗'}",
        f"SL hit first:      {'✓' if sl_first else '✗'}",
        "",
        f"MFE: {mfe}%  |  MAE: {mae}%",
    ]
    if notes:
        lines += ["", notes]
    lines += ["", f"<i>Prediction ID: {pid}</i>"]
    return "\n".join(lines)


def _format_strategy_update_message(version: dict) -> str:
    version_id = _html_safe(version.get("version_id", "?"))
    lifecycle = _html_safe(version.get("lifecycle", "active"))
    reason = _html_safe(version.get("reason", ""))
    prev = version.get("prev_params", {})
    new = version.get("new_params", {})
    validation = version.get("validation_summary", "")

    lines = [
        "⚙️ <b>Strategy Update</b>",
        f"Version: <code>{version_id}</code>  →  <b>{lifecycle.upper()}</b>",
    ]
    if reason:
        lines += ["", f"Reason: {reason}"]

    changed_keys = set(list(prev.keys()) + list(new.keys()))
    changed = {k: (prev.get(k), new.get(k)) for k in changed_keys if prev.get(k) != new.get(k)}
    if changed:
        lines.append("")
        lines.append("Changed parameters:")
        for k, (old_val, new_val) in changed.items():
            lines.append(f"  {_html_safe(k)}: {_html_safe(old_val)} → {_html_safe(new_val)}")

    if validation:
        lines += ["", f"Validation: {_html_safe(str(validation)[:400])}"]

    return "\n".join(lines)


def _format_degradation_alert(kpis: dict, reasons: list[str]) -> str:
    window = kpis.get("window", "?")
    da = kpis.get("directional_accuracy")
    tp1 = kpis.get("tp1_reach_rate")
    fp = kpis.get("false_positive_rate")
    score = kpis.get("avg_outcome_score")

    lines = [
        "🚨 <b>Performance Degradation Alert</b>",
        f"Window: last {window} evaluated predictions",
        "",
        "Triggered thresholds:",
    ]
    for r in reasons:
        lines.append(f"  • {_html_safe(r)}")

    lines += [
        "",
        "Current KPIs:",
        f"  Directional accuracy: {da:.1%}" if da is not None else "  Directional accuracy: N/A",
        f"  TP1 reach rate:       {tp1:.1%}" if tp1 is not None else "  TP1 reach rate: N/A",
        f"  False-positive rate:  {fp:.1%}" if fp is not None else "  False-positive rate: N/A",
        f"  Avg outcome score:    {score:.3f}" if score is not None else "  Avg outcome score: N/A",
        "",
        "Adaptation cycle will be triggered.",
    ]
    return "\n".join(lines)


def _html_safe(value: Any) -> str:
    return escape(str(value), quote=False)


def _plain_text_fallback(text: str) -> str:
    text = re.sub(r"</?(?:b|i|code)>", "", text)
    return unescape(text)


# =============================================================================
# CONVENIENCE FUNCTIONS (for use as agent tools if needed)
# =============================================================================


async def get_failed_deliveries(limit: int = 20) -> list[dict]:
    """Return recent failed Telegram deliveries for replay."""
    from memory.store import get_memory_store

    store = get_memory_store()
    deliveries = await store.get_telegram_deliveries()
    failed = [d for d in deliveries if not d.get("success")]
    return failed[-limit:] if limit > 0 else failed
