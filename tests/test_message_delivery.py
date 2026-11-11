from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from dataclasses import replace
from pathlib import Path
from threading import Event, get_ident

import pytest
import SpiffWorkflow
from lxml import etree
from sqlalchemy import event, func, select

from m8flow_core import api
from m8flow_core.db import build_session_factory
from m8flow_core.models.message import MessageDeliveryModel, MessageSubscriptionModel
from m8flow_core.models.tenant import M8flowTenantModel
from m8flow_core.models.user import UserModel
from m8flow_core.services.authorization import (
    ROLE_ADMIN,
    ensure_v1_role,
    grant_permission_to_user,
)
from m8flow_core.services.workflow_runtime import _restore_workflow

FIXTURE = Path(__file__).parent / "fixtures" / "external_message.bpmn"
BPMN = FIXTURE.read_text(encoding="utf-8")
MESSAGE = "payment.confirmed"
TENANT = "message-tenant"


def _seed_actor(session, tenant_id: str, *, can_deliver: bool = True) -> int:
    session.add(M8flowTenantModel(id=tenant_id, slug=tenant_id, name=tenant_id))
    actor = UserModel(
        username=tenant_id,
        service=f"https://id.example/realms/{tenant_id}",
        service_id=tenant_id,
        email=f"{tenant_id}@example.test",
        display_name=tenant_id,
    )
    session.add(actor)
    session.flush()
    ensure_v1_role(
        session, tenant_id=tenant_id, role_name=ROLE_ADMIN, user_ids=[actor.id]
    )
    if can_deliver:
        grant_permission_to_user(
            session,
            user_id=actor.id,
            permission="execute",
            command=api.PROCESS_MESSAGE_DELIVER_COMMAND,
            resource_type="tenant",
            resource_id=tenant_id,
        )
    return actor.id


def _import(
    session,
    user_id,
    xml=BPMN,
    *,
    tenant=TENANT,
    identifier="payment",
    digest=None,
):
    return api.execute_command(
        session,
        api.ImportBpmnProcessDefinitionCommand(
            tenant,
            identifier,
            user_id,
            xml,
            process_xml_digest=digest,
        ),
    )


def _start(session, definition, user_id, *, case="case-a", region="west"):
    return api.execute_command(
        session,
        api.InitializeProcessInstanceFromDefinitionCommand(
            definition.m8f_tenant_id,
            definition.id,
            user_id,
            submission_metadata={"case_id": case, "region_code": region},
        ),
    )


def _delivery(user_id, *, key="delivery-a", case="case-a", region="west", **target):
    return api.DeliverMessageCommand(
        TENANT,
        key,
        MESSAGE,
        user_id,
        {"case_id": case, "region": region, "amount": 42},
        **target,
    )


def _instance(session, instance_id):
    return api.execute_query(session, api.GetProcessInstanceQuery(TENANT, instance_id))


@pytest.fixture
def payment(session):
    actor_id = _seed_actor(session, TENANT)
    definition = _import(session, actor_id)
    session.commit()
    return actor_id, definition


def test_standard_bpmn_fixture_is_xsd_valid():
    schema_file = Path(SpiffWorkflow.__file__).parent / "bpmn/parser/schema/BPMN20.xsd"
    etree.XMLSchema(etree.parse(str(schema_file))).assertValid(
        etree.parse(str(FIXTURE))
    )


def test_delivers_to_only_the_instance_matching_every_property(
    engine,
    session,
    payment,
):
    actor_id, definition = payment
    first = _start(session, definition, actor_id)
    second = _start(session, definition, actor_id, region="east")
    session.commit()
    factory = build_session_factory(engine)
    for command, selected, waiting in (
        (_delivery(actor_id), first.id, second.id),
        (_delivery(actor_id, key="delivery-b", region="east"), second.id, None),
    ):
        with factory.begin() as transaction:
            result = api.execute_command(transaction, command)
            assert result.id == selected
            assert result.status == api.ProcessInstanceStatus.complete
            (end,) = _restore_workflow(result.workflow_state_json).get_tasks(
                spec_name="EndEvent_1"
            )
            assert end.data["message_payload"] == command.payload
            assert end.data["region_code"] == command.payload["region"]
            if waiting is not None:
                assert _instance(transaction, waiting).status == "waiting"
    with factory.begin() as transaction:
        replay = api.execute_command(transaction, _delivery(actor_id))
        assert replay.id == first.id
        with pytest.raises(api.ValidationError, match="different inputs"):
            api.execute_command(transaction, _delivery(actor_id, region="east"))
        assert transaction.scalar(select(func.count(MessageDeliveryModel.id))) == 2
        assert transaction.scalar(select(func.count(MessageSubscriptionModel.id))) == 0


def test_early_and_incomplete_messages_can_be_retried(session, payment):
    actor_id, definition = payment
    command = _delivery(actor_id, case=101)
    with pytest.raises(api.NotFoundError):
        api.execute_command(session, command)
    instance = _start(session, definition, actor_id, case=101)
    for payload in ({"case_id": 101}, {"case_id": "101", "region": "west"}):
        with pytest.raises(api.NotFoundError):
            api.execute_command(session, replace(command, payload=payload))
    assert api.execute_command(session, command).id == instance.id


def test_completing_a_human_task_opens_a_subscription(session, payment):
    actor_id, _ = payment
    source = BPMN.replace(
        'sourceRef="StartEvent_1" targetRef="Catch_Payment"/>',
        """sourceRef="StartEvent_1" targetRef="Task_EnterCase"/>
        <userTask id="Task_EnterCase"/>
        <sequenceFlow id="Flow_Case" sourceRef="Task_EnterCase"
            targetRef="Catch_Payment"/>""",
    )
    definition = _import(session, actor_id, source)
    instance = api.execute_command(
        session,
        api.InitializeProcessInstanceFromDefinitionCommand(
            TENANT, definition.id, actor_id
        ),
    )
    command = _delivery(actor_id)
    with pytest.raises(api.NotFoundError):
        api.execute_command(session, command)
    (work_item,) = api.execute_query(
        session, api.GetPendingTasksQuery(TENANT, actor_id)
    )
    api.execute_command(session, api.ClaimTaskCommand(TENANT, work_item.id, actor_id))
    api.execute_command(
        session,
        api.CompleteTaskCommand(
            TENANT,
            work_item.id,
            actor_id,
            task_payload={"case_id": "case-a", "region_code": "west"},
        ),
    )
    assert api.execute_command(session, command).id == instance.id


def test_ambiguous_matches_and_target_filters(session, payment):
    actor_id, definition = payment
    second_definition = _import(
        session, actor_id, identifier="second-payment", digest="alternate-definition"
    )
    assert second_definition.id != definition.id
    first = _start(session, definition, actor_id)
    second = _start(session, second_definition, actor_id)
    command = _delivery(actor_id)
    with pytest.raises(api.InvalidStateError, match="Multiple waits"):
        api.execute_command(session, command)
    with pytest.raises(api.NotFoundError):
        api.execute_command(
            session,
            replace(
                command,
                bpmn_process_definition_id=definition.id,
                process_instance_id=second.id,
            ),
        )
    assert (
        api.execute_command(
            session, replace(command, bpmn_process_definition_id=definition.id)
        ).id
        == first.id
    )
    assert (
        api.deliver_message(
            session,
            tenant_id=TENANT,
            delivery_id="second",
            message_name=MESSAGE,
            user_id=actor_id,
            payload=command.payload,
            process_instance_id=second.id,
        ).id
        == second.id
    )


def _parallel_xml(*, second_catch: bool, second_case: str | None = None) -> str:
    if second_catch:
        branch = """
            <intermediateCatchEvent id="Branch_B">
              <messageEventDefinition messageRef="PaymentConfirmed"/>
            </intermediateCatchEvent>"""
        flow = """
            <messageFlow id="MessageFlow_B" sourceRef="ExternalSender"
                targetRef="Branch_B" messageRef="PaymentConfirmed"/>"""
    else:
        branch = '<userTask id="Branch_B"/>'
        flow = ""
    if second_case is not None:
        branch += f"""
            <scriptTask id="Set_Case">
              <script>case_id = {second_case!r}</script>
            </scriptTask>
            <sequenceFlow id="Set_Case_Flow" sourceRef="Set_Case" targetRef="Branch_B"/>
        """
    branch_entry = "Branch_B" if second_case is None else "Set_Case"
    return (
        BPMN.replace(
            'sourceRef="StartEvent_1" targetRef="Catch_Payment"/>',
            f'''sourceRef="StartEvent_1" targetRef="Split"/>
            <parallelGateway id="Split"/>
            <sequenceFlow id="Split_A" sourceRef="Split" targetRef="Catch_Payment"/>
            <sequenceFlow id="Split_B" sourceRef="Split" targetRef="{branch_entry}"/>
            {branch}
            <sequenceFlow id="Join_B" sourceRef="Branch_B" targetRef="Join"/>
            <parallelGateway id="Join"/>
            <sequenceFlow id="Join_End" sourceRef="Join" targetRef="EndEvent_1"/>
            ''',
        )
        .replace(
            'sourceRef="Catch_Payment" targetRef="EndEvent_1"',
            'sourceRef="Catch_Payment" targetRef="Join"',
        )
        .replace(
            '<correlationKey id="PaymentKey"', flow + '<correlationKey id="PaymentKey"'
        )
    )


def test_multiple_matching_catches_in_one_instance_are_ambiguous(session, payment):
    actor_id, _ = payment
    definition = _import(session, actor_id, _parallel_xml(second_catch=True))
    instance = _start(session, definition, actor_id)
    with pytest.raises(api.InvalidStateError, match="Multiple waits"):
        api.execute_command(
            session, _delivery(actor_id, process_instance_id=instance.id)
        )
    assert session.scalar(select(func.count(MessageSubscriptionModel.id))) == 2
    assert session.scalar(select(func.count(MessageDeliveryModel.id))) == 0


def test_two_different_keys_in_one_instance_wake_one_catch_each(session, payment):
    actor_id, _ = payment
    definition = _import(
        session, actor_id, _parallel_xml(second_catch=True, second_case="case-b")
    )
    instance = _start(session, definition, actor_id)
    assert api.execute_command(session, _delivery(actor_id)).status == "waiting"
    assert session.scalar(select(func.count(MessageSubscriptionModel.id))) == 1
    result = api.execute_command(
        session, _delivery(actor_id, case="case-b", key="delivery-b")
    )
    assert result.id == instance.id
    assert result.status == "complete"


def test_delivery_requires_tenant_membership_and_permission(session, payment):
    actor_id, definition = payment
    first = _start(session, definition, actor_id)
    other_tenant = "message-other"
    other_actor = _seed_actor(session, other_tenant, can_deliver=False)
    other_definition = _import(session, other_actor, tenant=other_tenant)
    other = _start(session, other_definition, other_actor)
    command = _delivery(actor_id)
    with pytest.raises(api.AuthorizationError):
        api.execute_command(session, replace(command, user_id=other_actor))
    with pytest.raises(api.AuthorizationError):
        api.execute_command(
            session, replace(command, tenant_id=other_tenant, user_id=other_actor)
        )
    assert api.execute_command(session, command).id == first.id
    assert other.status == "waiting"
    grant_permission_to_user(
        session,
        user_id=other_actor,
        permission="execute",
        command=api.PROCESS_MESSAGE_DELIVER_COMMAND,
        resource_type="tenant",
        resource_id=other_tenant,
    )
    assert (
        api.execute_command(
            session, replace(command, tenant_id=other_tenant, user_id=other_actor)
        ).id
        == other.id
    )


def test_lifecycle_pauses_and_reactivates_waits(session, payment):
    actor_id, definition = payment
    instance = _start(session, definition, actor_id)
    command = _delivery(actor_id)
    for deactivate, reactivate in (
        (api.SuspendProcessInstanceCommand, api.ResumeProcessInstanceCommand),
        (api.ErrorProcessInstanceCommand, api.RetryProcessInstanceCommand),
    ):
        api.execute_command(session, deactivate(TENANT, instance.id, actor_id))
        with pytest.raises(api.NotFoundError):
            api.execute_command(session, command)
        api.execute_command(session, reactivate(TENANT, instance.id, actor_id))
    assert api.execute_command(session, command).id == instance.id
    another = _start(session, definition, actor_id)
    api.execute_command(
        session, api.TerminateProcessInstanceCommand(TENANT, another.id, actor_id)
    )
    assert session.scalar(select(func.count(MessageSubscriptionModel.id))) == 0


def test_running_instance_keeps_its_original_rules_after_reimport(session, payment):
    actor_id, definition = payment
    original = _start(session, definition, actor_id)
    updated = _import(
        session,
        actor_id,
        BPMN.replace(">region</messagePath>", ">region_v2</messagePath>"),
        digest=definition.process_xml_digest,
    )
    assert updated.id == definition.id
    latest = _start(session, updated, actor_id)
    assert api.execute_command(session, _delivery(actor_id)).id == original.id
    command = _delivery(actor_id, key="updated")
    assert (
        api.execute_command(
            session,
            replace(command, payload={"case_id": "case-a", "region_v2": "west"}),
        ).id
        == latest.id
    )


@pytest.mark.parametrize("use_connection", [False, True], ids=["session", "connection"])
def test_delivery_rollback_preserves_wait_and_releases_delivery_id(
    engine,
    session,
    payment,
    use_connection,
):
    actor_id, definition = payment
    instance_id = _start(session, definition, actor_id).id
    session.commit()
    command = _delivery(actor_id)
    factory = build_session_factory(engine)
    open_scope = engine.connect if use_connection else factory
    with open_scope() as connection:
        transaction = connection.begin()
        assert api.execute_command(connection, command).status == "complete"
        transaction.rollback()
    with open_scope() as connection, connection.begin():
        assert _instance(connection, instance_id).status == "waiting"
        assert (
            api.execute_command(
                connection, replace(command, payload={**command.payload, "amount": 10})
            ).status
            == "complete"
        )


def test_failed_continuation_can_retry_without_a_receipt(session, payment):
    actor_id, _ = payment
    source = BPMN.replace(
        'sourceRef="Catch_Payment" targetRef="EndEvent_1"/>',
        """sourceRef="Catch_Payment" targetRef="Script_Check"/>
        <scriptTask id="Script_Check">
          <script>assert not message_payload.get("fail")</script>
        </scriptTask>
        <sequenceFlow id="Flow_Check" sourceRef="Script_Check" targetRef="EndEvent_1"/>
        """,
    )
    instance = _start(session, _import(session, actor_id, source), actor_id)
    command = _delivery(actor_id)
    with pytest.raises(api.InvalidStateError, match="continuation failed"):
        api.execute_command(
            session, replace(command, payload={**command.payload, "fail": True})
        )
    assert instance.status == "waiting"
    assert session.scalar(select(func.count(MessageDeliveryModel.id))) == 0
    assert api.execute_command(session, command).status == "complete"


@contextmanager
def _observe_competing_sql(engine, *fragments):
    caller = get_ident()
    reached = Event()

    def observe(_connection, _cursor, sql, _parameters, _context, _many):
        if get_ident() != caller and all(fragment in sql for fragment in fragments):
            reached.set()

    event.listen(engine, "before_cursor_execute", observe)
    try:
        yield reached
    finally:
        event.remove(engine, "before_cursor_execute", observe)


def _instance_lock_sql(engine):
    if engine.dialect.name == "sqlite":
        return ("UPDATE process_instance SET message_correlation_enabled",)
    return ("FROM process_instance", "FOR UPDATE")


@pytest.mark.parametrize("same_id", [True, False], ids=["replay", "competing-id"])
def test_concurrent_delivery_consumes_a_wait_once(engine, session, payment, same_id):
    actor_id, definition = payment
    instance_id = _start(session, definition, actor_id).id
    session.commit()
    command = _delivery(actor_id)
    factory = build_session_factory(engine)

    def compete():
        with factory.begin() as competitor:
            incoming = command if same_id else replace(command, delivery_id="other")
            return api.execute_command(competitor, incoming).id

    with (
        _observe_competing_sql(engine, *_instance_lock_sql(engine)) as locking,
        ThreadPoolExecutor(max_workers=1) as worker,
    ):
        with factory.begin() as first:
            assert api.execute_command(first, command).id == instance_id
            future = worker.submit(compete)
            assert locking.wait(timeout=10)
        if same_id:
            assert future.result(timeout=10) == instance_id
        else:
            with pytest.raises(api.NotFoundError):
                future.result(timeout=10)
    assert session.scalar(select(func.count(MessageDeliveryModel.id))) == 1


def test_concurrent_reuse_of_delivery_id_for_different_instances(
    engine, session, payment
):
    if engine.dialect.name != "postgresql":
        pytest.skip("requires independent row locks on both process instances")
    actor_id, definition = payment
    _start(session, definition, actor_id)
    other_id = _start(session, definition, actor_id, region="east").id
    session.commit()
    factory = build_session_factory(engine)

    def compete():
        with factory.begin() as competitor:
            return api.execute_command(competitor, _delivery(actor_id, region="east"))

    with (
        _observe_competing_sql(engine, "INSERT INTO message_delivery") as inserting,
        ThreadPoolExecutor(max_workers=1) as worker,
    ):
        with factory.begin() as first:
            api.execute_command(first, _delivery(actor_id))
            future = worker.submit(compete)
            assert inserting.wait(timeout=10)
        with pytest.raises(api.ValidationError, match="different inputs"):
            future.result(timeout=10)
    assert _instance(session, other_id).status == "waiting"
    assert session.scalar(select(func.count(MessageDeliveryModel.id))) == 1


def test_task_completion_uses_the_snapshot_after_a_concurrent_delivery(
    engine,
    session,
    payment,
):
    actor_id, _ = payment
    definition = _import(session, actor_id, _parallel_xml(second_catch=False))
    instance = _start(session, definition, actor_id)
    (item,) = api.execute_query(session, api.GetPendingTasksQuery(TENANT, actor_id))
    api.execute_command(session, api.ClaimTaskCommand(TENANT, item.id, actor_id))
    instance_id, item_id = instance.id, item.id
    session.commit()
    factory = build_session_factory(engine)

    def complete():
        with factory.begin() as competitor:
            return api.execute_command(
                competitor, api.CompleteTaskCommand(TENANT, item_id, actor_id)
            ).completed

    with (
        _observe_competing_sql(engine, *_instance_lock_sql(engine)) as locking,
        ThreadPoolExecutor(max_workers=1) as worker,
    ):
        with factory.begin() as first:
            assert api.execute_command(first, _delivery(actor_id)).id == instance_id
            future = worker.submit(complete)
            assert locking.wait(timeout=10)
        assert future.result(timeout=10)
    with factory.begin() as transaction:
        assert _instance(transaction, instance_id).status == "complete"


def test_import_rejects_unsupported_correlation_expressions(session, payment):
    actor_id, _ = payment
    invalid = BPMN.replace(">region</messagePath>", ">payload/region</messagePath>")
    with pytest.raises(api.ValidationError, match="single field name"):
        _import(session, actor_id, invalid)
