"""
Notifier channel tests using `responses` to mock HTTP.

The primary goal is anti-regression: ensure that every notifier function
sends requests to URLs that contain the *real* variable values, not literal
braces like `{token}` or `{key}`.

Channels with known f-string bugs in the current source are marked
``xfail(strict=False)`` so CI doesn't block.  Once the companion f-string
fix PR is merged, those tests will automatically become xpass - the xfail
marker should then be removed.
"""
import responses as resp_lib

# ---------------------------------------------------------------------------
# WeChat Work bot (_wecom_bot) — currently correct, no xfail
# ---------------------------------------------------------------------------

@resp_lib.activate
def test_wecom_bot(monkeypatch):
    webhook = "https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=fake"
    monkeypatch.setenv("WECOM_BOT_WEBHOOK", webhook)
    resp_lib.add(resp_lib.POST, webhook, json={"errcode": 0}, status=200)

    from notifier import _wecom_bot
    _wecom_bot("Hello Title", "Hello Body", "https://example.com")

    assert len(resp_lib.calls) == 1
    url = resp_lib.calls[0].request.url
    assert "{" not in url and "}" not in url

    import json
    body = json.loads(resp_lib.calls[0].request.body)
    content = body["markdown"]["content"]
    assert "Hello Title" in content
    assert "Hello Body" in content
    assert "{title}" not in content
    assert "{body}" not in content


# ---------------------------------------------------------------------------
# ServerChan (_serverchan) — f-string bug: `f"...{{key}}..."` → xfail
# ---------------------------------------------------------------------------

@resp_lib.activate
def test_serverchan(monkeypatch):
    monkeypatch.setenv("SERVERCHAN_KEY", "abc123")
    expected_url = "https://sctapi.ftqq.com/abc123.send"
    resp_lib.add(resp_lib.POST, expected_url, json={"code": 0}, status=200)

    from notifier import _serverchan
    _serverchan("My Title", "My Body", "https://example.com")

    assert len(resp_lib.calls) == 1
    url = resp_lib.calls[0].request.url
    assert "{key}" not in url
    assert "{" not in url and "}" not in url
    assert "abc123" in url


# ---------------------------------------------------------------------------
# Telegram (_telegram) — f-string bug: `{{token}}` and `{{title}}/{{body}}` → xfail
# ---------------------------------------------------------------------------

@resp_lib.activate
def test_telegram(monkeypatch):
    monkeypatch.setenv("TELEGRAM_BOT_TOKEN", "tok")
    monkeypatch.setenv("TELEGRAM_CHAT_ID", "cid")
    expected_url = "https://api.telegram.org/bottok/sendMessage"
    resp_lib.add(resp_lib.POST, expected_url, json={"ok": True}, status=200)

    from notifier import _telegram
    _telegram("My Title", "My Body", "https://example.com")

    assert len(resp_lib.calls) == 1
    url = resp_lib.calls[0].request.url
    assert "{token}" not in url
    assert "{" not in url and "}" not in url
    assert "tok" in url

    import json
    body = json.loads(resp_lib.calls[0].request.body)
    assert "My Title" in body["text"]
    assert "{title}" not in body["text"]


# ---------------------------------------------------------------------------
# Bark (_bark) — f-string bug: `{{key}}` in URL → xfail
# ---------------------------------------------------------------------------

@resp_lib.activate
def test_bark(monkeypatch):
    monkeypatch.setenv("BARK_KEY", "bk")
    expected_url = "https://api.day.app/bk"
    resp_lib.add(resp_lib.POST, expected_url, json={"code": 200}, status=200)

    from notifier import _bark
    _bark("My Title", "My Body", "https://example.com")

    assert len(resp_lib.calls) == 1
    url = resp_lib.calls[0].request.url
    assert "{key}" not in url
    assert "{" not in url and "}" not in url
    assert "bk" in url


# ---------------------------------------------------------------------------
# Webhook (_webhook) — currently correct, no xfail
# ---------------------------------------------------------------------------

@resp_lib.activate
def test_webhook(monkeypatch):
    monkeypatch.setenv("WEBHOOK_URL", "https://example.com/hook")
    resp_lib.add(resp_lib.POST, "https://example.com/hook", json={}, status=200)

    from notifier import _webhook
    _webhook("Test Title", "Test Body", "https://example.com/view")

    assert len(resp_lib.calls) == 1
    url = resp_lib.calls[0].request.url
    assert "{" not in url and "}" not in url

    import json
    body = json.loads(resp_lib.calls[0].request.body)
    assert body["title"] == "Test Title"
    assert body["body"] == "Test Body"
    assert body["url"] == "https://example.com/view"


# ---------------------------------------------------------------------------
# PushPlus (_pushplus) — currently correct, no xfail
# ---------------------------------------------------------------------------

@resp_lib.activate
def test_pushplus(monkeypatch):
    monkeypatch.setenv("PUSHPLUS_TOKEN", "ppt")
    resp_lib.add(resp_lib.POST, "http://www.pushplus.plus/send", json={"code": 200}, status=200)

    from notifier import _pushplus
    _pushplus("PP Title", "PP Body", "https://example.com")

    assert len(resp_lib.calls) == 1
    url = resp_lib.calls[0].request.url
    assert "{" not in url and "}" not in url

    import json
    body = json.loads(resp_lib.calls[0].request.body)
    assert body["token"] == "ppt"
    assert body["title"] == "PP Title"
