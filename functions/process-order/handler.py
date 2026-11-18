"""process-order : consomme un lot de messages SQS, de façon idempotente.

Variables d'environnement : TABLE_ORDERS, TABLE_IDEMPOTENCY, STREAM_NAME.
L'endpoint LocalStack est lu automatiquement dans AWS_ENDPOINT_URL quand il est défini.
"""
import json
import os
import time

import boto3
from botocore.exceptions import ClientError

# Clients créés hors du handler : réutilisés entre invocations.
dynamodb = boto3.client("dynamodb")
kinesis = boto3.client("kinesis")

TABLE_ORDERS = os.environ.get("TABLE_ORDERS", "orders")
TABLE_IDEMPOTENCY = os.environ.get("TABLE_IDEMPOTENCY", "idempotency")
STREAM_NAME = os.environ.get("STREAM_NAME", "flashticket-sales")
TTL_SECONDS = 24 * 3600


def log(level, msg, **fields):
    print(json.dumps({"level": level, "msg": msg, **fields}))


def process_message(order):
    """Traite UNE commande. Lève une exception pour la signaler en échec."""
    order_id = order["orderId"]
    correlation_id = order["correlationId"]
    idempotency_key = order["idempotencyKey"]

    try:
        dynamodb.put_item(
            TableName=TABLE_IDEMPOTENCY,
            Item={
                "pk": {"S": idempotency_key},
                "status": {"S": "IN_PROGRESS"},
                "stage": {"S": "RESERVED"},
                "orderId": {"S": order_id},
                "ttl": {"N": str(int(time.time()) + TTL_SECONDS)},
            },
            ConditionExpression="attribute_not_exists(pk)",
        )
        stage = "RESERVED"
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise

        existing = dynamodb.get_item(
            TableName=TABLE_IDEMPOTENCY,
            Key={"pk": {"S": idempotency_key}},
            ConsistentRead=True,
        ).get("Item")
        if not existing:
            raise RuntimeError("Réservation d'idempotence introuvable")
        if existing["status"]["S"] == "DONE":
            log("info", "doublon ignoré", correlationId=correlation_id, orderId=order_id)
            return
        stage = existing.get("stage", {}).get("S", "RESERVED")

    if stage == "RESERVED":
        existing_order = dynamodb.get_item(
            TableName=TABLE_ORDERS,
            Key={"orderId": {"S": order_id}},
            ConsistentRead=True,
        ).get("Item")
        if not existing_order:
            dynamodb.put_item(
                TableName=TABLE_ORDERS,
                Item={
                    "orderId": {"S": order_id},
                    "correlationId": {"S": correlation_id},
                    "idempotencyKey": {"S": idempotency_key},
                    "eventId": {"S": str(order["eventId"])},
                    "seats": {"N": str(order["seats"])},
                    "email": {"S": str(order["email"])},
                    "createdAt": {"S": str(order["createdAt"])},
                },
            )

        dynamodb.update_item(
            TableName=TABLE_IDEMPOTENCY,
            Key={"pk": {"S": idempotency_key}},
            UpdateExpression="SET #stage = :stage",
            ExpressionAttributeNames={"#stage": "stage"},
            ExpressionAttributeValues={":stage": {"S": "ORDER_WRITTEN"}},
        )

    event = {
        "type": "OrderConfirmed",
        "orderId": order_id,
        "correlationId": correlation_id,
        "idempotencyKey": idempotency_key,
        "eventId": order["eventId"],
        "seats": order["seats"],
        "email": order["email"],
        "confirmedAt": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    kinesis.put_record(
        StreamName=STREAM_NAME,
        PartitionKey=correlation_id,
        Data=json.dumps(event).encode("utf-8"),
    )

    dynamodb.update_item(
        TableName=TABLE_IDEMPOTENCY,
        Key={"pk": {"S": idempotency_key}},
        UpdateExpression="SET #status = :done",
        ExpressionAttributeNames={"#status": "status"},
        ExpressionAttributeValues={":done": {"S": "DONE"}},
    )
    log("info", "commande traitée", correlationId=correlation_id, orderId=order_id)


def handler(event, context):
    failures = []
    for record in event.get("Records", []):
        try:
            order = json.loads(record["body"])
            process_message(order)
        except Exception as exc:  # noqa: BLE001 : on renvoie l'élément en échec, pas tout le lot
            log("error", "message en échec", messageId=record["messageId"], error=str(exc))
            failures.append({"itemIdentifier": record["messageId"]})
    # ReportBatchItemFailures : seuls ces messages seront rejoués.
    return {"batchItemFailures": failures}
