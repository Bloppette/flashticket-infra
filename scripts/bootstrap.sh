set -euo pipefail

export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:4566}"
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-eu-west-3}"
AWS="${AWS_CMD:-aws --endpoint-url $AWS_ENDPOINT_URL}"

echo "== Création de la DLQ"
$AWS sqs create-queue --queue-name flashticket-orders-dlq >/dev/null

DLQ_URL=$($AWS sqs create-queue --queue-name flashticket-orders-dlq --query QueueUrl --output text) 
DLQ_ARN=$($AWS sqs get-queue-attributes --queue-url "$DLQ_URL" --attribute-names QueueArn --query Attributes.QueueArn --output text)
REDRIVE="{"deadLetterTargetArn":"$DLQ_ARN","maxReceiveCount":"3"}"

echo "== Création de la file principale"
ATTRS="{\"VisibilityTimeout\":\"60\",\"RedrivePolicy\":\"$REDRIVE\"}"

$AWS sqs create-queue \
  --queue-name flashticket-orders \
  --attributes $ATTRS >/dev/null

create_table() {
  local table_name=$1
  local key_name=$([ "$table_name" = orders ] && echo orderId || echo pk)
  if ! $AWS dynamodb describe-table --table-name "$table_name" >/dev/null 2>&1; then
    $AWS dynamodb create-table \
      --table-name "$table_name" \
      --attribute-definitions "AttributeName=$key_name,AttributeType=S" \
      --key-schema "AttributeName=$key_name,KeyType=HASH" \
      --billing-mode PAY_PER_REQUEST >/dev/null
  fi
}

echo "== Création des tables DynamoDB"
create_table orders
create_table idempotency
$AWS dynamodb update-time-to-live \
  --table-name idempotency \
  --time-to-live-specification Enabled=true,AttributeName=ttl >/dev/null

echo "== Création du flux Kinesis"
if ! $AWS kinesis describe-stream --stream-name flashticket-sales >/dev/null 2>&1; then
  $AWS kinesis create-stream --stream-name flashticket-sales --shard-count 1 >/dev/null
fi

echo "Ressources TD2 prêtes."
