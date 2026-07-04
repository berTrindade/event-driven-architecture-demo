#!/usr/bin/env bash
# Drives the demo end to end. Assumes `make up` is running.
set -euo pipefail

ORDER_API=${ORDER_API:-http://localhost:8000}
READ_API=${READ_API:-http://localhost:8001}

say() { printf '\n\033[1m%s\033[0m\n' "$*"; }

say "1) Place an order. order-service just publishes orders.placed - it calls no one."
RESP=$(curl -s -X POST "$ORDER_API/orders" \
  -H 'content-type: application/json' \
  -d '{"customer":"ada","item":"keyboard","amount_cents":4999}')
echo "   $RESP"
ORDER_ID=$(printf '%s' "$RESP" | sed -E 's/.*"order_id" *: *"([^"]+)".*/\1/')

say "2) payment-service and inventory-service react independently."
say "   The projection folds their events into a read model. Polling until CONFIRMED..."
for _ in $(seq 1 15); do
  sleep 1
  STATUS=$(curl -s "$READ_API/orders/$ORDER_ID" || true)
  echo "   $STATUS"
  printf '%s' "$STATUS" | grep -q CONFIRMED && break
done

say "Done. Full read model:  curl $READ_API/orders"
