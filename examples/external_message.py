"""Run with `uv run python examples/external_message.py`."""

from pathlib import Path

from m8flow_core import api
from m8flow_core.db import build_engine, build_session_factory, create_schema
from m8flow_core.models.tenant import M8flowTenantModel
from m8flow_core.models.user import UserModel
from m8flow_core.services.authorization import (
    ROLE_ADMIN,
    ensure_v1_role,
    grant_permission_to_user,
)


def main() -> None:
    tenant_id = "message-demo"
    bpmn = Path(__file__).resolve().parents[1] / "tests/fixtures/external_message.bpmn"
    engine = build_engine("sqlite+pysqlite:///:memory:")
    create_schema(engine)
    sessions = build_session_factory(engine)
    with sessions.begin() as session:
        session.add(M8flowTenantModel(id=tenant_id, slug=tenant_id, name=tenant_id))
        actor = UserModel(
            username="message-integration",
            service="https://id.example/realms/message-demo",
            service_id="message-integration",
            email="message-integration@example.test",
            display_name="Message integration",
        )
        session.add(actor)
        session.flush()
        ensure_v1_role(
            session, tenant_id=tenant_id, role_name=ROLE_ADMIN, user_ids=[actor.id]
        )
        grant_permission_to_user(
            session,
            user_id=actor.id,
            command=api.PROCESS_MESSAGE_DELIVER_COMMAND,
            permission="execute",
            resource_type="tenant",
            resource_id=tenant_id,
        )
        definition = api.execute_command(
            session,
            api.ImportBpmnProcessDefinitionCommand(
                tenant_id, "payment-confirmation", actor.id, bpmn.read_bytes()
            ),
        )
        waiting = {}
        for region in ("west", "east"):
            instance = api.execute_command(
                session,
                api.InitializeProcessInstanceFromDefinitionCommand(
                    tenant_id,
                    definition.id,
                    actor.id,
                    submission_metadata={"case_id": "case-42", "region_code": region},
                ),
            )
            waiting[region] = instance.id
        actor_id = actor.id

    for region in ("east", "west"):
        with sessions.begin() as session:
            instance = api.execute_command(
                session,
                api.DeliverMessageCommand(
                    tenant_id=tenant_id,
                    delivery_id=f"payment-case-42-{region}",
                    message_name="payment.confirmed",
                    user_id=actor_id,
                    payload={"case_id": "case-42", "region": region, "amount": 125},
                ),
            )
            assert instance.id == waiting[region]
            assert instance.status == api.ProcessInstanceStatus.complete
        print(f"Committed payment confirmation for {region}, instance {instance.id}")
    engine.dispose()


if __name__ == "__main__":
    main()
