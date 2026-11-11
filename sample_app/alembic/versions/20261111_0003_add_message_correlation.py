"""Add the core's message subscriptions and delivery receipts."""

import sqlalchemy as sa
from alembic import op
from m8flow_core.models import Base

revision = "20261111_0003"
down_revision = "20260706_0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    bind = op.get_bind()
    instance_columns = sa.inspect(bind).get_columns("process_instance")
    # On a fresh database the sample app baseline uses the current ORM model.
    if not any(
        col["name"] == "message_correlation_enabled" for col in instance_columns
    ):
        op.add_column(
            "process_instance",
            sa.Column(
                "message_correlation_enabled",
                sa.Boolean(),
                nullable=False,
                server_default=sa.false(),
            ),
        )
    for table in ("message_subscription", "message_delivery"):
        Base.metadata.tables[table].create(bind, checkfirst=True)


def downgrade() -> None:
    op.drop_table("message_delivery")
    op.drop_table("message_subscription")
    op.drop_column("process_instance", "message_correlation_enabled")
