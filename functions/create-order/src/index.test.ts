import assert from "node:assert/strict";
import test from "node:test";
import { handler } from "./index.ts";

function event(overrides: Record<string, unknown> = {}) {
  return {
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ eventId: "concert", seats: 2, email: "client@example.com" }),
    isBase64Encoded: false,
    ...overrides,
  } as any;
}

test("rejette une requête sans Idempotency-Key", async () => {
  const response = await handler(event());
  assert.equal(response.statusCode, 400);
  assert.match(response.body as string, /Idempotency-Key/);
});

test("rejette un nombre de places invalide", async () => {
  const response = await handler(event({
    headers: { "idempotency-key": "test-key" },
    body: JSON.stringify({ eventId: "concert", seats: 7, email: "client@example.com" }),
  }));
  assert.equal(response.statusCode, 400);
  assert.match(response.body as string, /seats/);
});
