"""SSRF-guard для webhook-URL, присланных пользователем.

Цель webhook задаёт потенциальный злоумышленник: без проверки клиент мог бы
указать внутреннюю инфраструктуру (``http://localhost``, ``http://postgres:5432``,
эндпоинт метаданных облака ``http://169.254.169.254``, ...) и заставить наш
consumer слать запросы от его имени. Этот модуль отклоняет запрещённые схемы
и хосты, которые резолвятся в непубличные адреса.
"""

import asyncio
import ipaddress
import socket
from urllib.parse import urlparse

from app.core.config import settings


class UnsafeWebhookURL(Exception):
    """Бросается, когда webhook-URL запрещён к вызову."""


def _is_blocked_ip(ip: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    )


def validate_webhook_url(url: str, *, allow_private: bool | None = None) -> None:
    """Проверить webhook-URL, бросая :class:`UnsafeWebhookURL`, если он небезопасен.

    Проверяет схему URL и резолвит хост, отклоняя любой адрес, который
    приватный, loopback, link-local, reserved, multicast или unspecified.
    """
    if allow_private is None:
        allow_private = settings.webhook_allow_private_hosts

    parsed = urlparse(url)
    if parsed.scheme not in settings.webhook_allowed_schemes:
        raise UnsafeWebhookURL(
            f"URL scheme {parsed.scheme!r} is not allowed "
            f"(allowed: {sorted(settings.webhook_allowed_schemes)})"
        )

    host = parsed.hostname
    if not host:
        raise UnsafeWebhookURL("URL has no host")

    if allow_private:
        return

    try:
        infos = socket.getaddrinfo(host, parsed.port, proto=socket.IPPROTO_TCP)
    except socket.gaierror as exc:
        raise UnsafeWebhookURL(f"cannot resolve host {host!r}: {exc}") from exc

    for *_, sockaddr in infos:
        ip = ipaddress.ip_address(sockaddr[0])
        if _is_blocked_ip(ip):
            raise UnsafeWebhookURL(f"host {host!r} resolves to non-public address {ip}")


async def validate_webhook_url_async(
    url: str, *, allow_private: bool | None = None
) -> None:
    """Асинхронная обёртка: выполняет (блокирующий) DNS-резолвинг в пуле потоков,
    чтобы не блокировать event loop."""
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(
        None, lambda: validate_webhook_url(url, allow_private=allow_private)
    )
