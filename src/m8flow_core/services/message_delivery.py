"""Correlate an external message to one persisted BPMN wait."""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

from SpiffWorkflow.bpmn.util.event import BpmnEvent
from SpiffWorkflow.exceptions import WorkflowException
from SpiffWorkflow.util.task import TaskState
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from m8flow_core.errors import InvalidStateError, NotFoundError, ValidationError
from m8flow_core.models.message import MessageDeliveryModel, MessageSubscriptionModel
from m8flow_core.models.process_instance import (
    ProcessInstanceModel,
    ProcessInstanceStatus,
)
from m8flow_core.models.process_instance_event import ProcessLifecycleEventType
from m8flow_core.services.authorization import (
    PROCESS_MESSAGE_DELIVER_COMMAND,
    require_command_authorization,
)
from m8flow_core.services.message_correlation import (
    canonical_json,
    correlation_values,
    text_hash,
)
from m8flow_core.services.message_subscriptions import (
    correlation_hash,
    lock_message_enabled_instance,
    message_catch_rules,
)
from m8flow_core.services.process_instances import (
    get_process_instance,
    record_process_instance_event,
)
from m8flow_core.services.tenant_users import ensure_user_belongs_to_tenant
from m8flow_core.services.workflow_runtime import (
    _persist_advanced_workflow,
    _restore_workflow,
    _service_task_execution_context,
)

_ACTIVE_STATUSES = (
    ProcessInstanceStatus.running.value,
    ProcessInstanceStatus.waiting.value,
    ProcessInstanceStatus.user_input_required.value,
)


def deliver_message(
    session: Session,
    *,
    tenant_id: str,
    delivery_id: str,
    message_name: str,
    user_id: int,
    payload: dict[str, Any],
    bpmn_process_definition_id: int | None = None,
    process_instance_id: int | None = None,
) -> ProcessInstanceModel:
    ensure_user_belongs_to_tenant(session, tenant_id=tenant_id, user_id=user_id)
    require_command_authorization(
        session,
        tenant_id=tenant_id,
        actor_user_id=user_id,
        command_key=PROCESS_MESSAGE_DELIVER_COMMAND,
        resource_id=tenant_id,
    )
    for label, value in (("delivery_id", delivery_id), ("message_name", message_name)):
        if (
            not isinstance(value, str)
            or not value.strip()
            or len(value) > 255
            or "\x00" in value
        ):
            raise ValidationError(f"{label} must contain 1 to 255 characters")
    for target_id in (bpmn_process_definition_id, process_instance_id):
        if target_id is not None and (type(target_id) is not int or target_id < 1):
            raise ValidationError("Message target IDs must be positive integers")
    try:
        normalized_payload = json.loads(canonical_json(payload))
    except (TypeError, ValueError, RecursionError) as exc:
        raise ValidationError("Message payload must be a JSON object") from exc
    if not isinstance(payload, dict) or normalized_payload != payload:
        raise ValidationError("Message payload must be a JSON object")

    delivery_hash = text_hash(delivery_id)
    request_hash = text_hash(
        canonical_json(
            [
                message_name,
                normalized_payload,
                bpmn_process_definition_id,
                process_instance_id,
            ]
        )
    )
    receipt = _get_receipt(session, tenant_id, delivery_hash)
    if receipt is not None:
        return _replay(session, tenant_id, receipt, delivery_id, request_hash)

    candidates = _find_subscriptions(
        session,
        tenant_id,
        message_name,
        normalized_payload,
        bpmn_process_definition_id,
        process_instance_id,
    )
    if len(candidates) != 1:
        receipt = _get_receipt(session, tenant_id, delivery_hash)
        if receipt is not None:
            return _replay(session, tenant_id, receipt, delivery_id, request_hash)
        if candidates:
            raise InvalidStateError(
                f"Multiple waits match BPMN message {message_name!r}"
            )
        raise _no_match(message_name)

    candidate = candidates[0]
    instance = lock_message_enabled_instance(
        session, tenant_id, candidate.process_instance_id
    )
    receipt = _get_receipt(session, tenant_id, delivery_hash)
    if receipt is not None:
        return _replay(session, tenant_id, receipt, delivery_id, request_hash)

    model = MessageSubscriptionModel
    current = session.scalar(
        select(model)
        .where(
            model.m8f_tenant_id == tenant_id,
            model.process_instance_id == candidate.process_instance_id,
            model.task_guid == candidate.task_guid,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        instance is None
        or instance.status not in _ACTIVE_STATUSES
        or current is None
        or current.message_name != message_name
        or current.correlation_values
        != correlation_values(json.loads(current.message_fields), normalized_payload)
    ):
        receipt = _get_receipt(session, tenant_id, delivery_hash, current=True)
        if receipt is not None:
            return _replay(session, tenant_id, receipt, delivery_id, request_hash)
        raise _no_match(message_name)

    occurred_at = datetime.now(UTC)
    receipt_inserted = False
    try:
        with session.begin_nested():
            session.add(
                MessageDeliveryModel(
                    m8f_tenant_id=tenant_id,
                    delivery_id=delivery_id,
                    delivery_hash=delivery_hash,
                    request_hash=request_hash,
                    message_name=message_name,
                    process_instance_id=instance.id,
                    task_guid=current.task_guid,
                    user_id=user_id,
                    occurred_at=occurred_at,
                )
            )
            session.flush()
            receipt_inserted = True
            _advance_message_wait(
                session, instance, current, normalized_payload, user_id, occurred_at
            )
    except IntegrityError:
        if not receipt_inserted:
            receipt = _get_receipt(session, tenant_id, delivery_hash, current=True)
            if receipt is not None:
                return _replay(session, tenant_id, receipt, delivery_id, request_hash)
        raise
    return instance


def _find_subscriptions(
    session: Session,
    tenant_id: str,
    message_name: str,
    payload: dict[str, Any],
    definition_id: int | None,
    instance_id: int | None,
) -> list[MessageSubscriptionModel]:
    model = MessageSubscriptionModel
    instance = ProcessInstanceModel
    where = [
        model.m8f_tenant_id == tenant_id,
        model.message_hash == text_hash(message_name),
        instance.id == model.process_instance_id,
        instance.m8f_tenant_id == tenant_id,
        instance.status.in_(_ACTIVE_STATUSES),
    ]
    if definition_id is not None:
        where.append(model.bpmn_process_definition_id == definition_id)
    if instance_id is not None:
        where.append(model.process_instance_id == instance_id)
    shapes = session.execute(
        select(model.message_fields_hash, model.message_fields).where(*where).distinct()
    ).all()
    hashes = []
    for _, fields in shapes:
        values = correlation_values(json.loads(fields), payload)
        if values is not None:
            hashes.append(correlation_hash(fields, values))
    if not hashes:
        return []
    candidates = session.scalars(
        select(model)
        .where(*where, model.correlation_hash.in_(hashes))
        .limit(2)
        .execution_options(populate_existing=True)
    )
    return list(candidates)


def _get_receipt(
    session: Session, tenant_id: str, delivery_hash: str, *, current: bool = False
) -> MessageDeliveryModel | None:
    model = MessageDeliveryModel
    statement = select(model).where(
        model.m8f_tenant_id == tenant_id, model.delivery_hash == delivery_hash
    )
    if current:
        statement = statement.with_for_update()
    return session.scalar(statement)


def _replay(
    session: Session,
    tenant_id: str,
    receipt: MessageDeliveryModel,
    delivery_id: str,
    request_hash: str,
) -> ProcessInstanceModel:
    if receipt.delivery_id != delivery_id or receipt.request_hash != request_hash:
        raise ValidationError("Message delivery ID already has different inputs")
    return get_process_instance(
        session, tenant_id=tenant_id, process_instance_id=receipt.process_instance_id
    )


def _advance_message_wait(
    session: Session,
    instance: ProcessInstanceModel,
    subscription: MessageSubscriptionModel,
    payload: dict[str, Any],
    user_id: int,
    occurred_at: datetime,
) -> None:
    serialized_state = instance.workflow_state_json
    if serialized_state is None:
        raise InvalidStateError("The message subscription has no workflow snapshot")
    try:
        workflow = _restore_workflow(
            serialized_state,
            service_task_context=_service_task_execution_context(
                tenant_id=instance.m8f_tenant_id,
                process_instance_id=instance.id,
                process_definition_id=instance.bpmn_process_definition_id,
            ),
        )
        task = workflow.get_task_from_id(UUID(subscription.task_guid))
        rules = message_catch_rules(task.task_spec)
        if (
            task.state != TaskState.WAITING
            or rules is None
            or rules["message_name"] != subscription.message_name
            or correlation_values(
                [rules["bindings"][field] for field in sorted(rules["bindings"])],
                task.data,
            )
            != subscription.correlation_values
        ):
            raise _no_match(subscription.message_name)
        task.set_data(message_payload=payload)
        event = BpmnEvent(task.task_spec.event_definition, payload=payload)
        # The workflow-level event manager can wake multiple catches. Apply the
        # event only to the task selected and validated above.
        task.task_spec.catch(task, event)
        task.task_spec._update(task)
        workflow.run_all(halt_on_manual=True)
    except WorkflowException as exc:
        raise InvalidStateError(f"BPMN message continuation failed: {exc}") from exc

    _persist_advanced_workflow(
        session, process_instance=instance, workflow=workflow, occurred_at=occurred_at
    )
    if instance.status == ProcessInstanceStatus.complete.value:
        record_process_instance_event(
            session,
            tenant_id=instance.m8f_tenant_id,
            process_instance_id=instance.id,
            event_type=ProcessLifecycleEventType.process_instance_completed,
            occurred_at=occurred_at,
            task_guid=subscription.task_guid,
            user_id=user_id,
        )


def _no_match(message_name: str) -> NotFoundError:
    return NotFoundError(f"No wait matches BPMN message {message_name!r}")
