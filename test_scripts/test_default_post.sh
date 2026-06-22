API=http://localhost:8000
KEY=super-secret-api-key
# Подставь свой адрес с webhook.site:
WEBHOOK_URL=https://webhook.site/1ac97c37-e19d-4927-9f3c-1ce3a108125e
# Уникальный ключ на каждый запуск — чтобы 1-й запрос был новым (202), а повтор — 200:
IDEM_KEY="order-$(uuidgen)"

HDRS=$(mktemp)

BODY=$(curl -s -D "$HDRS" -X POST "$API/api/v1/payments" \
  -H "X-API-Key: $KEY" \
  -H "Idempotency-Key: $IDEM_KEY" \
  -H "Content-Type: application/json" \
  -d "{
        \"amount\": \"199.99\",
        \"currency\": \"RUB\",
        \"description\": \"Test payment\",
        \"metadata\": {\"order_id\": 12345, \"user_id\": 777},
        \"webhook_url\": \"$WEBHOOK_URL\"
      }")

echo "=== POST /payments — заголовки ответа ==="
cat "$HDRS"
echo "=== POST /payments — тело ==="
echo "$BODY" | python3 -m json.tool

PAYMENT_ID=$(echo "$BODY" | python3 -c "import sys, json; print(json.load(sys.stdin)['payment_id'])")
echo "payment_id = $PAYMENT_ID"

echo "=== Повтор того же запроса (ожидаем 200 + Idempotent-Replayed: true) ==="
curl -s -o /dev/null -D - -X POST "$API/api/v1/payments" \
  -H "X-API-Key: $KEY" \
  -H "Idempotency-Key: $IDEM_KEY" \
  -H "Content-Type: application/json" \
  -d "{\"amount\":\"199.99\",\"currency\":\"RUB\",\"webhook_url\":\"$WEBHOOK_URL\"}" \
  | grep -iE "HTTP/|Idempotency-Key|Idempotent-Replayed"

sleep 6
echo "=== GET /payments/$PAYMENT_ID ==="
curl -s "$API/api/v1/payments/$PAYMENT_ID" -H "X-API-Key: $KEY" | python3 -m json.tool

rm -f "$HDRS"

