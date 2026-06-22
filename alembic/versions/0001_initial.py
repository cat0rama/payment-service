"""начальные таблицы payments и outbox

Revision ID: 0001_initial
Revises:
Create Date: 2026-06-15

"""

from collections.abc import Sequence

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision: str = "0001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    currency_enum = postgresql.ENUM(
        "RUB", "USD", "EUR", name="currency_enum", create_type=False
    )
    payment_status_enum = postgresql.ENUM(
        "pending", "succeeded", "failed", name="payment_status_enum", create_type=False
    )
    outbox_status_enum = postgresql.ENUM(
        "pending", "published", name="outbox_status_enum", create_type=False
    )

    bind = op.get_bind()
    currency_enum.create(bind, checkfirst=True)
    payment_status_enum.create(bind, checkfirst=True)
    outbox_status_enum.create(bind, checkfirst=True)

    op.create_table(
        "payments",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("amount", sa.Numeric(20, 2), nullable=False),
        sa.Column("currency", currency_enum, nullable=False),
        sa.Column("description", sa.String(length=1024), nullable=True),
        sa.Column("metadata", postgresql.JSONB(), nullable=True),
        sa.Column("status", payment_status_enum, nullable=False),
        sa.Column("idempotency_key", sa.String(length=255), nullable=False),
        sa.Column("webhook_url", sa.String(length=2048), nullable=False),
        sa.Column(
            "webhook_delivered",
            sa.Boolean(),
            server_default=sa.text("false"),
            nullable=False,
        ),
        sa.Column("failure_reason", sa.String(length=1024), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("processed_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_index(
        "ix_payments_idempotency_key", "payments", ["idempotency_key"], unique=True
    )

    op.create_table(
        "outbox",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("aggregate_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("event_type", sa.String(length=255), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("status", outbox_status_enum, nullable=False),
        sa.Column("retries", sa.Integer(), nullable=False, server_default="0"),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_outbox_aggregate_id", "outbox", ["aggregate_id"], unique=False)
    op.create_index("ix_outbox_status", "outbox", ["status"], unique=False)


def downgrade() -> None:
    op.drop_index("ix_outbox_status", table_name="outbox")
    op.drop_index("ix_outbox_aggregate_id", table_name="outbox")
    op.drop_table("outbox")
    op.drop_index("ix_payments_idempotency_key", table_name="payments")
    op.drop_table("payments")

    bind = op.get_bind()
    postgresql.ENUM(name="outbox_status_enum").drop(bind, checkfirst=True)
    postgresql.ENUM(name="payment_status_enum").drop(bind, checkfirst=True)
    postgresql.ENUM(name="currency_enum").drop(bind, checkfirst=True)
