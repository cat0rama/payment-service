"""Тесты SSRF-guard. Используют IP-литералы, чтобы не было реальных DNS-запросов."""

import pytest

from app.config import settings
from app.url_guard import UnsafeWebhookURL, validate_webhook_url


@pytest.mark.parametrize(
    "url",
    [
        "https://127.0.0.1/hook",  # loopback
        "https://10.0.0.5/hook",  # приватный
        "https://192.168.1.1/hook",  # приватный
        "https://172.16.0.1/hook",  # приватный
        "https://169.254.169.254/meta",  # link-local / метаданные облака
        "https://[::1]/hook",  # ipv6 loopback
        "https://0.0.0.0/hook",  # unspecified-адрес
    ],
)
def test_blocks_private_and_internal_hosts(url, monkeypatch):
    # фиксируем продовый дефолт: проверяем, что приватные хосты блокируются, когда
    # private-цели запрещены. (в полном прогоне интеграционный conftest глобально
    # ставит WEBHOOK_ALLOW_PRIVATE_HOSTS=true в общий singleton settings.)
    monkeypatch.setattr(settings, "webhook_allow_private_hosts", False)
    with pytest.raises(UnsafeWebhookURL):
        validate_webhook_url(url)


def test_blocks_disallowed_scheme(monkeypatch):
    # проверяем продовый дефолт: разрешён только https. (локальный .env может
    # расширять схемы до "https,http", поэтому фиксируем значение явно.)
    monkeypatch.setattr(settings, "webhook_allowed_schemes", {"https"})
    with pytest.raises(UnsafeWebhookURL):
        validate_webhook_url("http://93.184.216.34/hook")
    with pytest.raises(UnsafeWebhookURL):
        validate_webhook_url("ftp://93.184.216.34/hook")


def test_allows_public_ip():
    # 93.184.216.34 - публичный адрес (example.com); исключения быть не должно.
    validate_webhook_url("https://93.184.216.34/hook")


def test_allow_private_override_bypasses_check():
    # allow_private=True используется в local/dev/test, чтобы разрешить loopback-цели.
    validate_webhook_url("https://127.0.0.1/hook", allow_private=True)


def test_rejects_url_without_host():
    with pytest.raises(UnsafeWebhookURL):
        validate_webhook_url("https:///nohost")
