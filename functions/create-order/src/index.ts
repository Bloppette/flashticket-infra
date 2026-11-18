// create-order : reçoit la commande (Function URL), valide, publie dans SQS, répond 202.
import { SQSClient, SendMessageCommand } from "@aws-sdk/client-sqs";
import { randomUUID } from "node:crypto";
import type { APIGatewayProxyEventV2, APIGatewayProxyResultV2 } from "aws-lambda";

// Astuce : le client est créé HORS du handler, il est réutilisé entre les invocations.
// L'endpoint LocalStack est lu automatiquement dans AWS_ENDPOINT_URL quand il est défini.
const sqs = new SQSClient({});
const QUEUE_URL = process.env.QUEUE_URL!;

const EMAIL_RE = /^[^@\s]+@[^@\s]+\.[^@\s]+$/;

function reply(statusCode: number, body: unknown, correlationId?: string): APIGatewayProxyResultV2 {
  return {
    statusCode,
    headers: { "content-type": "application/json", ...(correlationId ? { "x-correlation-id": correlationId } : {}) },
    body: JSON.stringify(body),
  };
}

export const handler = async (event: APIGatewayProxyEventV2): Promise<APIGatewayProxyResultV2> => {
  // L'identifiant de corrélation suit la commande de bout en bout (journaux).
  const correlationId =
    event.headers?.["x-correlation-id"] ??
    event.headers?.["X-Correlation-ID"] ??
    randomUUID();

  const idempotencyKey =
    event.headers?.["idempotency-key"] ??
    event.headers?.["Idempotency-Key"];
  if (!idempotencyKey?.trim()) {
    return reply(400, { errors: ["Idempotency-Key requis"] }, correlationId);
  }

  let rawBody = event.body ?? "";
  if (event.isBase64Encoded) {
    rawBody = Buffer.from(rawBody, "base64").toString("utf8");
  }

  let payload: unknown;
  try {
    payload = rawBody ? JSON.parse(rawBody) : {};
  } catch {
    return reply(400, { errors: ["Corps JSON invalide"] }, correlationId);
  }

  if (!payload || typeof payload !== "object" || Array.isArray(payload)) {
    return reply(400, { errors: ["Le corps doit être un objet JSON"] }, correlationId);
  }

  const body = payload as Record<string, unknown>;
  const errors: string[] = [];
  if (typeof body.eventId !== "string" || !body.eventId.trim()) {
    errors.push("eventId doit être une chaîne non vide");
  }
  if (typeof body.seats !== "number" || !Number.isInteger(body.seats) || body.seats < 1 || body.seats > 6) {
    errors.push("seats doit être un entier entre 1 et 6");
  }
  if (typeof body.email !== "string" || !EMAIL_RE.test(body.email)) {
    errors.push("email doit être une adresse valide");
  }
  if (errors.length > 0) {
    return reply(400, { errors }, correlationId);
  }

  const orderId = randomUUID();
  const message = {
    orderId,
    correlationId,
    idempotencyKey: idempotencyKey.trim(),
    eventId: (body.eventId as string).trim(),
    seats: body.seats,
    email: body.email,
    createdAt: new Date().toISOString(),
  };

  try {
    await sqs.send(new SendMessageCommand({
      QueueUrl: QUEUE_URL,
      MessageBody: JSON.stringify(message),
    }));
  } catch (error) {
    console.log(JSON.stringify({
      level: "error",
      msg: "publication SQS échouée",
      correlationId,
      orderId,
      error: error instanceof Error ? error.message : String(error),
    }));
    return reply(500, { error: "Impossible d'accepter la commande" }, correlationId);
  }

  console.log(JSON.stringify({ level: "info", msg: "commande acceptée", correlationId, orderId }));
  return reply(202, { orderId, correlationId }, correlationId);
};
