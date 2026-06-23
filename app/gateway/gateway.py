import asyncio
import random

from app.core.config import settings
from app.db.models import Payment


class PaymentGateway:
    """Эмуляция внешнего платёжного шлюза.

    Выделено в отдельный класс, чтобы обработку платежа можно было тестировать с
    детерминированным «шлюзом» (подменив этот объект), не трогая логику consumer'а.
    """

    async def charge(self, payment: Payment) -> bool:
        """Сымитировать обращение к шлюзу: случайная задержка и исход.

        Возвращает True при успешной оплате, False — при отказе.
        """
        delay = random.uniform(
            settings.processing_min_seconds, settings.processing_max_seconds
        )
        await asyncio.sleep(delay)
        return random.random() < settings.processing_success_rate
