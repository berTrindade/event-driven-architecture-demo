#!/usr/bin/env bash
# Drives the demo end to end. Assumes `make up` is running.
set -euo pipefail

ORDER_API=${ORDER_API:-http://localhost:8000}
READ_API=${READ_API:-http://localhost:8001}

say() { printf '\n\033[1m%s\033[0m\n' "$*"; }

say "1) Place an order (order-service publishes orders.placed with a correlation id)"
RESP=$(curl -s -X POST "$ORDER_API/orders" \
  -H 'content-type: application/json' \
  -d '{"customer":"ada","item":"keyboard","amount_cents":4999}')
echo "   $RESP"
ORDER_ID=$(printf '%s' "$RESP" | sed -E 's/.*"order_id" *: *"([^"]+)".*/\1/')

say "2) payment + inventory react independently; the projection folds it. Polling until CONFIRMED..."
for _ in $(seq 1 15); do
  sleep 1
  STATUS=$(curl -s "$READ_API/orders/$ORDER_ID" || true)
  echo "   $STATUS"
  printf '%s' "$STATUS" | grep -q CONFIRMED && break
done

say "3) Now a POISON order: payment fails, the message goes to the dead-letter queue"
curl -s -X POST "$ORDER_API/orders" \
  -H 'content-type: application/json' \
  -d '{"customer":"grace","item":"POISON","amount_cents":1000}' | sed 's/^/   /'
echo
say "   It never confirms. See the dead-letter queue:"
echo "   docker compose exec rabbitmq rabbitmqctl list_queues name messages | grep dead-letter"
echo "   (or the management UI at http://localhost:15672)"

say "Done. Full read model:  curl $READ_API/orders"
