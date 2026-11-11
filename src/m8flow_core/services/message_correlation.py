"""Compile the supported BPMN correlation expressions into field lookups."""

from __future__ import annotations

import hashlib
import json
import math
import re
from dataclasses import dataclass
from io import BytesIO
from typing import TYPE_CHECKING, Any

from defusedxml import ElementTree
from defusedxml.common import DefusedXmlException

from m8flow_core.errors import ValidationError

if TYPE_CHECKING:
    from xml.etree.ElementTree import Element  # nosec B405

BPMN = "{http://www.omg.org/spec/BPMN/20100524/MODEL}"
MESSAGE_CORRELATION_EXTENSION = "__m8f_message_correlation"
_FIELD_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_XPATH = "http://www.w3.org/1999/XPath"


@dataclass(frozen=True)
class MessageCatch:
    message_id: str
    message_name: str
    bindings: dict[str, str]


def compile_message_catches(xml: bytes) -> dict[str, dict[str, MessageCatch]]:
    try:
        parsed = ElementTree.iterparse(
            BytesIO(xml), events=("start", "end", "start-ns")
        )
        namespaces: dict[Element, dict[str, str]] = {}
        scope: list[dict[str, str]] = [{}]
        declarations: dict[str, str] = {}
        for event, value in parsed:
            if event == "start-ns":
                prefix, uri = value
                declarations[prefix] = uri
            elif event == "start":
                if declarations:
                    scope.append(scope[-1] | declarations)
                    declarations.clear()
                else:
                    scope.append(scope[-1])
                namespaces[value] = scope[-1]
            else:
                scope.pop()
        root = parsed.root
    except (ElementTree.ParseError, DefusedXmlException, ValueError) as exc:
        raise ValidationError(f"BPMN source is not parseable: {exc}") from exc

    if root is None:
        return {}

    def reference(node: Element, value: str | None) -> str:
        prefix, separator, local_id = (value or "").strip().rpartition(":")
        if not local_id or (
            separator
            and (
                not prefix
                or prefix not in namespaces[node]
                or namespaces[node][prefix] != root.get("targetNamespace")
            )
        ):
            raise ValidationError(f"Expected a local BPMN reference, got {value!r}")
        return local_id

    default_language = root.get("expressionLanguage", _XPATH)
    messages = {node.get("id"): node for node in root.findall(BPMN + "message")}
    properties = {
        node.get("id"): node for node in root.findall(BPMN + "correlationProperty")
    }
    collaborations = root.findall(BPMN + "collaboration")
    compiled: dict[str, dict[str, MessageCatch]] = {}
    for process in root.findall(BPMN + "process"):
        process_id = process.get("id")
        subscriptions = process.findall(BPMN + "correlationSubscription")
        if process_id is None or not subscriptions:
            continue
        subscriptions_by_key = {}
        for subscription in subscriptions:
            subscription_key = reference(
                subscription, subscription.get("correlationKeyRef")
            )
            if subscription_key in subscriptions_by_key:
                raise ValidationError(
                    f"Duplicate correlation subscription for {subscription_key!r}"
                )
            subscriptions_by_key[subscription_key] = subscription

        flows_and_keys: dict[str, list[tuple[Element, Element]]] = {}
        for collaboration in collaborations:
            participant_refs = [
                reference(node, node.get("processRef"))
                for node in collaboration.findall(BPMN + "participant")
                if node.get("processRef")
            ]
            if process_id not in participant_refs:
                continue
            keys = [
                key
                for key in collaboration.findall(BPMN + "correlationKey")
                if key.get("id") in subscriptions_by_key
            ]
            for flow in collaboration.findall(BPMN + "messageFlow"):
                target = reference(flow, flow.get("targetRef"))
                flows_and_keys.setdefault(target, []).extend(
                    (flow, key) for key in keys
                )

        gateways = {
            node.get("id") for node in process.findall(BPMN + "eventBasedGateway")
        }
        gateway_targets = {
            node.get("targetRef")
            for node in process.findall(BPMN + "sequenceFlow")
            if node.get("sourceRef") in gateways
        }
        catches = {}
        for catch in process.findall(BPMN + "intermediateCatchEvent"):
            message_events = catch.findall(BPMN + "messageEventDefinition")
            if not message_events:
                continue
            catch_id = catch.get("id")
            if (
                not catch_id
                or len(message_events) != 1
                or catch_id in gateway_targets
                or any(
                    node.tag.endswith("EventDefinition") and node != message_events[0]
                    for node in catch
                )
                or catch.find(BPMN + "eventDefinitionRef") is not None
            ):
                raise ValidationError(
                    f"Unsupported correlated message catch {catch_id!r}"
                )
            message_event = message_events[0]
            message_id = reference(message_event, message_event.get("messageRef"))
            message = messages.get(message_id)
            message_name = message.get("name") if message is not None else None
            if not message_name or not message_name.strip() or len(message_name) > 255:
                raise ValidationError(
                    f"Catch {catch_id!r} must reference a named BPMN message"
                )
            flow_keys = {
                key.attrib["id"]: key
                for flow, key in flows_and_keys.get(catch_id, [])
                if flow.get("messageRef") is None
                or reference(flow, flow.get("messageRef")) == message_id
            }
            applicable = {
                key_id: key
                for key_id, key in flow_keys.items()
                if all(
                    (prop := properties.get(reference(node, node.text))) is not None
                    and any(
                        reference(retrieval, retrieval.get("messageRef")) == message_id
                        for retrieval in prop.findall(
                            BPMN + "correlationPropertyRetrievalExpression"
                        )
                    )
                    for node in key.findall(BPMN + "correlationPropertyRef")
                )
            }
            if len(applicable) != 1:
                raise ValidationError(
                    f"Catch {catch_id!r} requires one correlation key on an "
                    "incoming collaboration message flow"
                )
            key_id, key = next(iter(applicable.items()))
            subscription = subscriptions_by_key[key_id]
            property_ids = [
                reference(node, node.text)
                for node in key.findall(BPMN + "correlationPropertyRef")
            ]
            binding_nodes = subscription.findall(BPMN + "correlationPropertyBinding")
            bindings_by_id = {
                reference(node, node.get("correlationPropertyRef")): node
                for node in binding_nodes
            }
            if (
                not property_ids
                or len(set(property_ids)) != len(property_ids)
                or len(bindings_by_id) != len(binding_nodes)
                or set(property_ids) != set(bindings_by_id)
            ):
                raise ValidationError(
                    f"Correlation key {key_id!r} requires one binding per property"
                )

            bindings: dict[str, str] = {}
            for property_id in property_ids:
                prop = properties[property_id]
                retrieval_nodes = prop.findall(
                    BPMN + "correlationPropertyRetrievalExpression"
                )
                retrievals = [
                    node
                    for node in retrieval_nodes
                    if reference(node, node.get("messageRef")) == message_id
                ]
                if len(retrievals) != 1:
                    raise ValidationError(
                        f"Property {property_id!r} requires one retrieval expression "
                        f"for message {message_id!r}"
                    )
                message_field = _field(retrievals[0], "messagePath", default_language)
                if message_field in bindings:
                    raise ValidationError(
                        f"Duplicate message field {message_field!r} on key {key_id!r}"
                    )
                bindings[message_field] = _field(
                    bindings_by_id[property_id], "dataPath", default_language
                )
            catches[catch_id] = MessageCatch(message_id, message_name, bindings)
        if catches:
            compiled[process_id] = catches
    return compiled


def _field(node: Element, tag: str, default_language: str) -> str:
    expressions = node.findall(BPMN + tag)
    if len(expressions) == 1:
        expression = expressions[0]
        field = (expression.text or "").strip()
        if (
            not len(expression)
            and expression.get("language", default_language) == _XPATH
            and _FIELD_NAME.fullmatch(field)
        ):
            return field
    raise ValidationError(f"BPMN {tag} must contain a single field name")


def correlation_values(fields: list[str], data: dict[str, Any]) -> str | None:
    values = [data.get(field) for field in fields]
    if any(
        type(value) not in (str, bool, int, float)
        or (isinstance(value, float) and not math.isfinite(value))
        for value in values
    ):
        return None
    return canonical_json(values)


def canonical_json(value: Any) -> str:
    return json.dumps(value, allow_nan=False, separators=(",", ":"), sort_keys=True)


def text_hash(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()
