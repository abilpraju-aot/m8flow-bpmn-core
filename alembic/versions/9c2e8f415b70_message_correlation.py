"""Add durable message subscriptions and delivery receipts."""

import sqlalchemy as sa
from alembic import op

revision = "9c2e8f415b70"
down_revision = "k2l3m4n5o6p7"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "process_instance",
        sa.Column(
            "message_correlation_enabled",
            sa.Boolean(),
            nullable=False,
            server_default=sa.false(),
        ),
    )
    op.create_table(
        "message_subscription",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("m8f_tenant_id", sa.String(255), nullable=False),
        sa.Column("process_instance_id", sa.Integer(), nullable=False),
        sa.Column("bpmn_process_definition_id", sa.Integer(), nullable=True),
        sa.Column("task_guid", sa.String(36), nullable=False),
        sa.Column("message_name", sa.String(255), nullable=False),
        sa.Column("message_hash", sa.String(64), nullable=False),
        sa.Column("message_fields", sa.Text(), nullable=False),
        sa.Column("message_fields_hash", sa.String(64), nullable=False),
        sa.Column("correlation_values", sa.Text(), nullable=False),
        sa.Column("correlation_hash", sa.String(64), nullable=False),
        sa.ForeignKeyConstraint(["m8f_tenant_id"], ["m8flow_tenant.id"]),
        sa.ForeignKeyConstraint(
            ["process_instance_id"], ["process_instance.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(
            ["bpmn_process_definition_id"], ["bpmn_process_definition.id"]
        ),
        sa.UniqueConstraint(
            "m8f_tenant_id",
            "process_instance_id",
            "task_guid",
            name="m8f_message_subscription_task_key",
        ),
    )
    op.create_index(
        op.f("ix_message_subscription_m8f_tenant_id"),
        "message_subscription",
        ["m8f_tenant_id"],
    )
    op.create_index(
        "m8f_message_subscription_correlation",
        "message_subscription",
        ["m8f_tenant_id", "message_hash", "correlation_hash"],
    )
    op.create_index(
        "m8f_message_subscription_definition",
        "message_subscription",
        ["m8f_tenant_id", "message_hash", "bpmn_process_definition_id"],
    )
    op.create_table(
        "message_delivery",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("m8f_tenant_id", sa.String(255), nullable=False),
        sa.Column("delivery_id", sa.String(255), nullable=False),
        sa.Column("delivery_hash", sa.String(64), nullable=False),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column("message_name", sa.String(255), nullable=False),
        sa.Column("process_instance_id", sa.Integer(), nullable=False),
        sa.Column("task_guid", sa.String(36), nullable=False),
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(["m8f_tenant_id"], ["m8flow_tenant.id"]),
        sa.ForeignKeyConstraint(
            ["process_instance_id"], ["process_instance.id"], ondelete="CASCADE"
        ),
        sa.ForeignKeyConstraint(["user_id"], ["user.id"]),
        sa.UniqueConstraint(
            "m8f_tenant_id", "delivery_hash", name="m8f_message_delivery_id_key"
        ),
    )
    op.create_index(
        op.f("ix_message_delivery_m8f_tenant_id"), "message_delivery", ["m8f_tenant_id"]
    )
    op.create_index(
        op.f("ix_message_delivery_process_instance_id"),
        "message_delivery",
        ["process_instance_id"],
    )


def downgrade() -> None:
    op.drop_table("message_delivery")
    op.drop_table("message_subscription")
    op.drop_column("process_instance", "message_correlation_enabled")
