"""Keep message subscriptions and workflow snapshots in the same transaction."""

from __future__ import annotations

from typing import Any

from SpiffWorkflow.bpmn.workflow import BpmnWorkflow
from SpiffWorkflow.util.task import TaskState
from sqlalchemy import delete, select, update
from sqlalchemy.orm import Session

from m8flow_core.errors import ValidationError
from m8flow_core.models.bpmn_process import BpmnProcessModel
from m8flow_core.models.message import MessageSubscriptionModel
from m8flow_core.models.process_instance import (
    ProcessInstanceModel,
    ProcessInstanceStatus,
)
from m8flow_core.services.message_correlation import (
    MESSAGE_CORRELATION_EXTENSION,
    canonical_json,
    correlation_values,
    text_hash,
)


def lock_message_enabled_instance(
    session: Session, tenant_id: str, process_instance_id: int
) -> ProcessInstanceModel | None:
    """Serialize writes to an opted-in instance, including scheduler writes."""
    model = ProcessInstanceModel
    where = (
        model.m8f_tenant_id == tenant_id,
        model.id == process_instance_id,
        model.message_correlation_enabled.is_(True),
    )
    with session.no_autoflush:
        statement = select(model).where(*where)
        if session.scalar(select(model.id).where(*where)) is None:
            return None
        if session.get_bind().dialect.name == "sqlite":
            # SQLite has no row locks. Acquire its write lock and start the
            # database transaction before a delivery opens a savepoint.
            session.execute(
                update(model)
                .where(*where)
                .values(message_correlation_enabled=model.message_correlation_enabled)
                .execution_options(synchronize_session=False)
            )
        instance = session.scalar(
            statement.with_for_update().execution_options(populate_existing=True)
        )
        if instance is not None and instance.bpmn_process_id is not None:
            # The runtime snapshot is addressed by this row, which may have been
            # loaded in this session before another writer advanced the instance.
            session.scalar(
                select(BpmnProcessModel)
                .where(
                    BpmnProcessModel.id == instance.bpmn_process_id,
                    BpmnProcessModel.m8f_tenant_id == tenant_id,
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
        return instance


def message_catch_rules(task_spec: Any) -> dict[str, Any] | None:
    return getattr(task_spec, "extensions", {}).get(MESSAGE_CORRELATION_EXTENSION)


def sync_message_subscriptions(
    session: Session, instance: ProcessInstanceModel, workflow: BpmnWorkflow
) -> None:
    if workflow.completed or instance.status == ProcessInstanceStatus.terminated.value:
        remove_message_subscriptions(session, instance)
        return
    model = MessageSubscriptionModel
    rows = {
        row.task_guid: row
        for row in session.scalars(
            select(model)
            .where(
                model.m8f_tenant_id == instance.m8f_tenant_id,
                model.process_instance_id == instance.id,
            )
            .with_for_update()
        )
    }
    for task in workflow.get_tasks(state=TaskState.WAITING):
        rules = message_catch_rules(task.task_spec)
        if rules is None:
            continue
        message_fields = sorted(rules["bindings"])
        data_fields = [rules["bindings"][field] for field in message_fields]
        values = correlation_values(data_fields, task.data)
        if values is None:
            raise ValidationError(
                f"BPMN catch {task.task_spec.name!r} requires non-null scalar "
                f"correlation values for {', '.join(data_fields)}"
            )
        fields = canonical_json(message_fields)
        task_guid = str(task.id)
        row = rows.pop(task_guid, None)
        if row is None:
            row = model(
                m8f_tenant_id=instance.m8f_tenant_id,
                process_instance_id=instance.id,
                bpmn_process_definition_id=instance.bpmn_process_definition_id,
                task_guid=task_guid,
                message_name=rules["message_name"],
                message_hash=text_hash(rules["message_name"]),
                message_fields=fields,
                message_fields_hash=text_hash(fields),
            )
            session.add(row)
        row.correlation_values = values
        row.correlation_hash = correlation_hash(fields, values)
    for row in rows.values():
        session.delete(row)


def remove_message_subscriptions(
    session: Session, instance: ProcessInstanceModel
) -> None:
    if instance.message_correlation_enabled:
        session.execute(
            delete(MessageSubscriptionModel).where(
                MessageSubscriptionModel.m8f_tenant_id == instance.m8f_tenant_id,
                MessageSubscriptionModel.process_instance_id == instance.id,
            )
        )


def correlation_hash(fields: str, values: str) -> str:
    return text_hash(fields + values)
