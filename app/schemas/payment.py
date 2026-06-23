import uuid
from datetime import datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

from app.db.models import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    amount: Decimal = Field(
        ..., gt=0, max_digits=20, decimal_places=2, examples=["199.99"]
    )
    currency: Currency = Field(..., examples=["RUB"])
    description: str | None = Field(default=None, max_length=1024)
    metadata: dict[str, Any] | None = Field(default=None)
    webhook_url: HttpUrl = Field(
        ..., examples=["https://example.com/webhooks/payments"]
    )

    @field_validator("amount")
    @classmethod
    def quantize_amount(cls, value: Decimal) -> Decimal:
        return value.quantize(Decimal("0.01"))


class PaymentCreatedResponse(BaseModel):
    """Возвращается из POST /payments (202 Accepted)."""

    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(
        ..., validation_alias="id", serialization_alias="payment_id"
    )
    status: PaymentStatus
    created_at: datetime


class PaymentResponse(BaseModel):
    """Полное представление платежа для GET /payments/{id}."""

    model_config = ConfigDict(from_attributes=True)

    payment_id: uuid.UUID = Field(
        ..., validation_alias="id", serialization_alias="payment_id"
    )
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, Any] | None = Field(
        default=None,
        validation_alias="payment_metadata",
        serialization_alias="metadata",
    )
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    failure_reason: str | None
    created_at: datetime
    processed_at: datetime | None
