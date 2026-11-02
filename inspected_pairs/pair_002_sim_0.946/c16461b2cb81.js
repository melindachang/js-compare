"""Best-effort outbound notice delivery across Telegram, email, SMS, and WhatsApp.

All channels are optional — a channel is skipped when its credentials are not
configured.  Failures are logged but never raised; a notice is always
persisted before any push is attempted.

Channels:
- **Telegram** (existing): crew who completed /pair receive push messages.
- **Email** (SMTP): crew with an ``email`` field get a formatted email.
- **SMS** (Africa's Talking): crew with a ``phone_number`` in E.164 format
  receive a condensed SMS — useful for critical ops comms in regions with
  intermittent data coverage.
- **WhatsApp** (Africa's Talking): crew with an opt-in ``whatsapp_number``
  receive the full message — kept separate from SMS so the two don't both fire.
"""

from __future__ import annotations

import logging
import smtplib
import ssl
import uuid
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import httpx
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.models import Crew, User
from app.models.notice import Notice
from app.models.user import UserRole

log = logging.getLogger("ratiba.notify")

_SEVERITY_ICON = {"INFO": "ℹ️", "IMPORTANT": "❗", "CRITICAL": "🚨"}
_CATEGORY_LABEL = {
    "OPERATIONAL": "Operational",
    "FLEET": "Fleet notice",
    "SAFETY": "Safety",
    "SOCIAL": "Crew room",
}


def _format_long(notice: Notice) -> str:
    """Full formatted body for Telegram + email."""
    icon = _SEVERITY_ICON.get(notice.severity.value, "ℹ️")
    cat = _CATEGORY_LABEL.get(notice.category.value, notice.category.value)
    lines = [f"{icon} {cat}: {notice.title}", "", notice.body]
    if notice.image_url:
        lines += ["", notice.image_url]
    if notice.requires_ack:
        lines += ["", "Please acknowledge in the Ratiba app."]
    return "\n".join(lines)


def _format_sms(notice: Notice) -> str:
    """Condensed SMS — 160 char target."""
    icon = _SEVERITY_ICON.get(notice.severity.value, "ℹ️")
    body = f"{icon} {notice.title}: {notice.body}"
    if len(body) > 155:
        body = body[:152] + "…"
    return body


# ---------------------------------------------------------------------------
# Telegram
# ---------------------------------------------------------------------------


def _push_telegram(chat_ids: list[int], text: str, token: str) -> int:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    sent = 0
    with httpx.Client(timeout=10.0) as client:
        for chat_id in chat_ids:
            try:
                resp = client.post(url, json={"chat_id": chat_id, "text": text})
                if resp.status_code == 200:
                    sent += 1
                else:
                    log.warning("telegram push %s → %s", chat_id, resp.status_code)
            except Exception as exc:
                log.warning("telegram push failed for %s: %r", chat_id, exc)
    return sent


# ---------------------------------------------------------------------------
# Email (SMTP)
# ---------------------------------------------------------------------------


def _push_email(addresses: list[str], notice: Notice, from_addr: str) -> int:
    settings = get_settings()
    host = settings.smtp_host.strip()
    if not host or not from_addr:
        return 0

    subject = (
        f"[Ratiba] {_CATEGORY_LABEL.get(notice.category.value, notice.category.value)}: "
        f"{notice.title}"
    )
    body = _format_long(notice)
    sent = 0
    try:
        context = ssl.create_default_context()
        if settings.smtp_use_tls:
            server: smtplib.SMTP = smtplib.SMTP(host, settings.smtp_port, timeout=15)
            server.ehlo()
            server.starttls(context=context)
        else:
            server = smtplib.SMTP(host, settings.smtp_port, timeout=15)
        if settings.smtp_user:
            server.login(settings.smtp_user, settings.smtp_password)
        for addr in addresses:
            try:
                msg = MIMEMultipart("alternative")
                msg["Subject"] = subject
                msg["From"] = from_addr
                msg["To"] = addr
                msg.attach(MIMEText(body, "plain", "utf-8"))
                server.sendmail(from_addr, addr, msg.as_string())
                sent += 1
            except Exception as exc:
                log.warning("email send failed for %s: %r", addr, exc)
        server.quit()
    except Exception as exc:
        log.warning("smtp connection failed: %r", exc)
    return sent


# ---------------------------------------------------------------------------
# Africa's Talking SMS
# ---------------------------------------------------------------------------

_AT_SMS_URL = "https://api.africastalking.com/version1/messaging"


def _push_sms(phone_numbers: list[str], text: str) -> int:
    settings = get_settings()
    api_key = settings.at_api_key.strip()
    username = settings.at_username.strip()
    if not api_key or not username:
        return 0

    recipients = ",".join(phone_numbers)
    sent = 0
    try:
        with httpx.Client(timeout=15.0) as client:
            resp = client.post(
                _AT_SMS_URL,
                headers={
                    "Accept": "application/json",
                    "apiKey": api_key,
                },
                data={
                    "username": username,
                    "to": recipients,
                    "message": text,
                    "from": settings.at_sender_id,
                },
            )
            if resp.status_code == 201:
                body = resp.json()
                result = body.get("SMSMessageData", {})
                recipients_data = result.get("Recipients", [])
                sent = sum(1 for r in recipients_data if r.get("status") == "Success")
                failed = len(recipients_data) - sent
                if failed:
                    log.warning("AT SMS: %d/%d failed", failed, len(recipients_data))
            else:
                log.warning("AT SMS API → %s: %s", resp.status_code, resp.text[:200])
    except Exception as exc:
        log.warning("AT SMS request failed: %r", exc)
    return sent


# ---------------------------------------------------------------------------
# Africa's Talking WhatsApp
# ---------------------------------------------------------------------------

# AT's WhatsApp ("Chat") send endpoint. Confirm the exact payload against the
# current Africa's Talking WhatsApp API docs before go-live, as with SMS.
_AT_WHATSAPP_URL = "https://chat.africastalking.com/whatsapp/message/send"


def _push_whatsapp(phone_numbers: list[str], text: str) -> int:
    """Send a WhatsApp message via Africa's Talking. No-ops unless AT_API_KEY,
    AT_USERNAME and AT_WHATSAPP_NUMBER are all configured. Best-effort: a failure
    on one recipient never blocks the others."""
    settings = get_settings()
    api_key = settings.at_api_key.strip()
    username = settings.at_username.strip()
    wa_number = settings.at_whatsapp_number.strip()
    if not api_key or not username or not wa_number:
        return 0

    sent = 0
    try:
        with httpx.Client(timeout=15.0) as client:
            for phone in phone_numbers:
                try:
                    resp = client.post(
                        _AT_WHATSAPP_URL,
                        headers={
                            "Accept": "application/json",
                            "Content-Type": "application/json",
                            "apiKey": api_key,
                        },
                        json={
                            "username": username,
                            "waNumber": wa_number,
                            "phoneNumber": phone,
                            "body": {"message": text},
                        },
                    )
                    if resp.status_code in (200, 201):
                        sent += 1
                    else:
                        log.warning("AT WhatsApp → %s: %s", resp.status_code, resp.text[:200])
                except Exception as exc:
                    log.warning("AT WhatsApp send failed for %s: %r", phone, exc)
    except Exception as exc:
        log.warning("AT WhatsApp request failed: %r", exc)
    return sent


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def push_notice(session: Session, notice: Notice) -> int:
    """Push ``notice`` to all crew via every configured channel.

    Returns the total number of successful individual deliveries across
    all channels.  Partial failures (one channel down) never block others.
    """
    settings = get_settings()
    text_long = _format_long(notice)
    text_sms = _format_sms(notice)

    crew_rows = session.scalars(
        select(Crew).where(Crew.operator_id == notice.operator_id).where(Crew.active.is_(True))
    ).all()

    telegram_ids: list[int] = [
        c.telegram_chat_id for c in crew_rows if c.telegram_chat_id is not None
    ]
    email_addrs: list[str] = [c.email for c in crew_rows if c.email]
    phone_numbers: list[str] = [c.phone_number for c in crew_rows if c.phone_number]
    whatsapp_numbers: list[str] = [c.whatsapp_number for c in crew_rows if c.whatsapp_number]

    total = 0

    if telegram_ids and settings.telegram_bot_token.strip():
        total += _push_telegram(telegram_ids, text_long, settings.telegram_bot_token.strip())

    if email_addrs and settings.smtp_host.strip():
        total += _push_email(email_addrs, notice, settings.smtp_from.strip())

    if phone_numbers and settings.at_api_key.strip():
        total += _push_sms(phone_numbers, text_sms)

    if whatsapp_numbers and settings.at_whatsapp_number.strip():
        total += _push_whatsapp(whatsapp_numbers, text_long)

    return total


def _send_email_text(addresses: list[str], subject: str, body: str, from_addr: str) -> int:
    settings = get_settings()
    host = settings.smtp_host.strip()
    if not host or not from_addr:
        return 0
    sent = 0
    try:
        context = ssl.create_default_context()
        if settings.smtp_use_tls:
            server: smtplib.SMTP = smtplib.SMTP(host, settings.smtp_port, timeout=15)
            server.ehlo()
            server.starttls(context=context)
        else:
            server = smtplib.SMTP(host, settings.smtp_port, timeout=15)
        if settings.smtp_user:
            server.login(settings.smtp_user, settings.smtp_password)
        for addr in addresses:
            try:
                msg = MIMEMultipart("alternative")
                msg["Subject"] = subject
                msg["From"] = from_addr
                msg["To"] = addr
                msg.attach(MIMEText(body, "plain", "utf-8"))
                server.sendmail(from_addr, addr, msg.as_string())
                sent += 1
            except Exception as exc:
                log.warning("email send failed for %s: %r", addr, exc)
        server.quit()
    except Exception as exc:
        log.warning("smtp connection failed: %r", exc)
    return sent


def notify_crew_member(session: Session, *, crew_id: uuid.UUID, subject: str, body: str) -> int:
    """Best-effort direct message to one crew member across every configured
    channel (email / SMS / Telegram). Returns successful deliveries; silently
    no-ops when a channel's credentials aren't set, so call sites needn't guard.
    """
    settings = get_settings()
    crew = session.get(Crew, crew_id)
    if crew is None:
        return 0
    total = 0
    if crew.telegram_chat_id is not None and settings.telegram_bot_token.strip():
        total += _push_telegram(
            [crew.telegram_chat_id], f"{subject}\n\n{body}", settings.telegram_bot_token.strip()
        )
    if crew.email and settings.smtp_host.strip():
        total += _send_email_text(
            [crew.email], f"[Ratiba] {subject}", body, settings.smtp_from.strip()
        )
    if crew.phone_number and settings.at_api_key.strip():
        total += _push_sms([crew.phone_number], f"{subject}: {body}")
    if crew.whatsapp_number and settings.at_whatsapp_number.strip():
        total += _push_whatsapp([crew.whatsapp_number], f"{subject}\n\n{body}")
    return total


def notify_staff(session: Session, *, operator_id: uuid.UUID, subject: str, body: str) -> int:
    """Email every active staff member of an operator (dashboard logins, not
    pilots). Best-effort; no-ops when SMTP isn't configured."""
    settings = get_settings()
    addresses = list(
        session.scalars(
            select(User.email)
            .where(User.operator_id == operator_id)
            .where(User.is_active.is_(True))
            .where(User.role != UserRole.PILOT)
        ).all()
    )
    if not addresses or not settings.smtp_host.strip():
        return 0
    return _send_email_text(addresses, f"[Ratiba] {subject}", body, settings.smtp_from.strip())
