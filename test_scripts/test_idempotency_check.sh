API=http://localhost:8000
KEY=super-secret-api-key
# Подставь свой адрес с webhook.site:
WEBHOOK_URL=https://webhook.site/1ac97c37-e19d-4927-9f3c-1ce3a108125e

PAYMENT_ID=$(curl -s -X POST "$API/api/v1/payments" \
  -H "X-API-Key: $KEY" \
  -H "Idempotency-Key: order-25493584359" \
  -H "Content-Type: application/json" \
  -d "{
        \"amount\": \"199.99\",
        \"currency\": \"RUB\",
        \"description\": \"Test payment\",
        \"metadata\": {\"order_id\": 12345, \"user_id\": 777},
        \"webhook_url\": \"$WEBHOOK_URL\"
      }" | python3 -c "import sys, json; print(json.load(sys.stdin)['payment_id'])")
echo "payment_id = $PAYMENT_ID"

sleep 6
curl -s "$API/api/v1/payments/$PAYMENT_ID" -H "X-API-Key: $KEY" | python3 -m json.tool
