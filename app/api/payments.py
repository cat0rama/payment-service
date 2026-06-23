import uuid

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Response, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.auth import require_api_key
from app.database import get_session
from app.metrics import payments_created_total
from app.schemas import PaymentCreate, PaymentCreatedResponse, PaymentResponse
from app.services import PaymentService
from app.url_guard import UnsafeWebhookURL, validate_webhook_url_async

router = APIRouter(prefix="/api/v1/payments", tags=["payments"])


@router.post(
    "",
    response_model=PaymentCreatedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(require_api_key)],
    summary="Create a payment",
)
async def create_payment(
    data: PaymentCreate,
    response: Response,
    idempotency_key: str = Header(..., alias="Idempotency-Key", min_length=1),
    session: AsyncSession = Depends(get_session),
) -> PaymentCreatedResponse:
    try:
        await validate_webhook_url_async(str(data.webhook_url))
    except UnsafeWebhookURL as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid webhook_url: {exc}",
        ) from exc

    payment, created = await PaymentService(session).create_payment(
        data, idempotency_key
    )
    # эхо ключа и признак повтора, чтобы клиент по заголовкам видел идемпотентность.
    response.headers["Idempotency-Key"] = idempotency_key
    response.headers["Idempotent-Replayed"] = "false" if created else "true"
    if created:
        payments_created_total.inc()
    else:
        # идемпотентный повтор: возвращаем уже существующий платёж.
        response.status_code = status.HTTP_200_OK
    return PaymentCreatedResponse.model_validate(payment)


@router.get(
    "",
    response_model=list[PaymentResponse],
    dependencies=[Depends(require_api_key)],
    summary="List processed payments",
)
async def list_payments(
    limit: int = Query(default=100, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
) -> list[PaymentResponse]:
    """Вернуть обработанные платежи (succeeded или failed), новые первыми."""
    payments = await PaymentService(session).list_payments(limit=limit, offset=offset)
    return [PaymentResponse.model_validate(p) for p in payments]


@router.get(
    "/{payment_id}",
    response_model=PaymentResponse,
    dependencies=[Depends(require_api_key)],
    summary="Get payment details",
)
async def get_payment(
    payment_id: uuid.UUID,
    session: AsyncSession = Depends(get_session),
) -> PaymentResponse:
    payment = await PaymentService(session).get_payment(payment_id)
    if payment is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found"
        )
    return PaymentResponse.model_validate(payment)
