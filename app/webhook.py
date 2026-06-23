import asyncio
import hashlib
import hmac
import json
import logging
import time

import httpx

from app.config import settings
from app.models import Payment
from app.url_guard import validate_webhook_url_async

logger = logging.getLogger("webhook")


class WebhookDeliveryError(Exception):
    """Бросается, когда webhook не удалось доставить после всех попыток."""


def build_webhook_payload(payment: Payment) -> dict:
    return {
        "event": "payment.processed",
        "payment_id": str(payment.id),
        "status": payment.status.value,
        "amount": str(payment.amount),
        "currency": payment.currency.value,
        "description": payment.description,
        "metadata": payment.payment_metadata,
        "failure_reason": payment.failure_reason,
        "processed_at": payment.processed_at.isoformat()
        if payment.processed_at
        else None,
    }


def sign_payload(body: bytes, timestamp: int, secret: str | None = None) -> str:
    """Вернуть hex HMAC-SHA256 от ``"{timestamp}.{body}"``.

    Привязка timestamp к подписи позволяет получателю отбивать повторы (replay).
    """
    key = (secret if secret is not None else settings.webhook_signing_secret).encode()
    signed = f"{timestamp}.".encode() + body
    return hmac.new(key, signed, hashlib.sha256).hexdigest()


class WebhookSender:
    """Доставка webhook-уведомлений: HMAC-подпись, SSRF-проверка и повторы.

    Выделено в отдельный класс, чтобы обработку платежа можно было тестировать,
    подменив отправителя, и чтобы вся логика доставки жила в одном месте.
    """

    async def deliver(self, payment: Payment) -> None:
        """Отправить результат POST-запросом на ``webhook_url`` платежа с повторами.

        Тело подписывается HMAC (``X-Webhook-Signature``), а URL повторно
        проверяется SSRF-guard'ом прямо перед отправкой. Повторяет
        ``webhook_max_attempts`` раз с экспоненциальным backoff; бросает
        :class:`WebhookDeliveryError`, если все попытки провалились.
        """
        # повторная проверка в момент доставки (защита от dns-rebinding между
        # созданием платежа и обработкой).
        await validate_webhook_url_async(payment.webhook_url)

        payload = build_webhook_payload(payment)
        # сериализуем один раз, чтобы подписанные байты совпадали с отправляемыми.
        body = json.dumps(payload, separators=(",", ":")).encode()
        timestamp = int(time.time())
        signature = sign_payload(body, timestamp)
        headers = {
            "Content-Type": "application/json",
            "X-Webhook-Timestamp": str(timestamp),
            "X-Webhook-Signature": f"t={timestamp},v1={signature}",
        }

        last_error: Exception | None = None

        async with httpx.AsyncClient(
            timeout=settings.webhook_timeout_seconds
        ) as client:
            for attempt in range(1, settings.webhook_max_attempts + 1):
                try:
                    response = await client.post(
                        payment.webhook_url, content=body, headers=headers
                    )
                    response.raise_for_status()
                    logger.info(
                        "Webhook delivered for payment %s (attempt %d, status %d)",
                        payment.id,
                        attempt,
                        response.status_code,
                    )
                    return
                except Exception as exc:  # noqa: BLE001 - повторяем при любой ошибке
                    last_error = exc
                    logger.warning(
                        "Webhook delivery failed for payment %s (attempt %d/%d): %s",
                        payment.id,
                        attempt,
                        settings.webhook_max_attempts,
                        exc,
                    )
                    if attempt < settings.webhook_max_attempts:
                        backoff = settings.webhook_backoff_base_seconds * 2 ** (
                            attempt - 1
                        )
                        await asyncio.sleep(backoff)

        raise WebhookDeliveryError(
            f"Failed to deliver webhook for payment {payment.id} "
            f"after {settings.webhook_max_attempts} attempts"
        ) from last_error
