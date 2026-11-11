from __future__ import annotations

from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Index, String, Text, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from m8flow_core.models.base import Base
from m8flow_core.models.tenant_scoped import M8fTenantScopedMixin, TenantScoped


class MessageSubscriptionModel(M8fTenantScopedMixin, TenantScoped, Base):
    __tablename__ = "message_subscription"
    __table_args__ = (
        UniqueConstraint(
            "m8f_tenant_id",
            "process_instance_id",
            "task_guid",
            name="m8f_message_subscription_task_key",
        ),
        Index(
            "m8f_message_subscription_correlation",
            "m8f_tenant_id",
            "message_hash",
            "correlation_hash",
        ),
        Index(
            "m8f_message_subscription_definition",
            "m8f_tenant_id",
            "message_hash",
            "bpmn_process_definition_id",
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    process_instance_id: Mapped[int] = mapped_column(
        ForeignKey("process_instance.id", ondelete="CASCADE")
    )
    bpmn_process_definition_id: Mapped[int | None] = mapped_column(
        ForeignKey("bpmn_process_definition.id")
    )
    task_guid: Mapped[str] = mapped_column(String(36))
    message_name: Mapped[str] = mapped_column(String(255))
    message_hash: Mapped[str] = mapped_column(String(64))
    message_fields: Mapped[str] = mapped_column(Text)
    message_fields_hash: Mapped[str] = mapped_column(String(64))
    correlation_values: Mapped[str] = mapped_column(Text)
    correlation_hash: Mapped[str] = mapped_column(String(64))


class MessageDeliveryModel(M8fTenantScopedMixin, TenantScoped, Base):
    __tablename__ = "message_delivery"
    __table_args__ = (
        UniqueConstraint(
            "m8f_tenant_id", "delivery_hash", name="m8f_message_delivery_id_key"
        ),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    delivery_id: Mapped[str] = mapped_column(String(255))
    delivery_hash: Mapped[str] = mapped_column(String(64))
    request_hash: Mapped[str] = mapped_column(String(64))
    message_name: Mapped[str] = mapped_column(String(255))
    process_instance_id: Mapped[int] = mapped_column(
        ForeignKey("process_instance.id", ondelete="CASCADE"), index=True
    )
    task_guid: Mapped[str] = mapped_column(String(36))
    user_id: Mapped[int] = mapped_column(ForeignKey("user.id"))
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
