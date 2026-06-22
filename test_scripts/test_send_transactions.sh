#!/usr/bin/env bash
# Шлёт набор РАЗНЫХ платежей в API и показывает их обработку end-to-end.
#
# Для каждого платежа: POST /api/v1/payments со своим Idempotency-Key, печать
# заголовков и тела ответа, сбор payment_id. Затем — демонстрация идемпотентного
# повтора (тот же ключ → 200 + Idempotent-Replayed: true) и, после паузы на
# обработку consumer'ом, финальный статус каждого платежа.
#
# Настройки через переменные окружения (со значениями по умолчанию):
#   API=http://localhost:8000                      базовый URL сервиса
#   KEY=super-secret-api-key                       значение заголовка X-API-Key
#   WEBHOOK_URL=https://webhook.site/<uuid>        куда слать webhook (по умолчанию webhook.site)
#   WAIT=8                                          пауза (сек) перед опросом статусов
#
# Пример:  ./test_send_transactions.sh
#          API=http://localhost:8000 KEY=мой-ключ ./test_send_transactions.sh

# API=http://localhost:8000 KEY=super-secret-api-key WAIT=10 ./test_send_transactions.sh

set -uo pipefail

API="${API:-http://localhost:8000}"
KEY="${KEY:-super-secret-api-key}"
WEBHOOK_URL="${WEBHOOK_URL:-https://webhook.site/1ac97c37-e19d-4927-9f3c-1ce3a108125e}"
WAIT="${WAIT:-8}"

# --- проверка зависимостей ---
command -v curl >/dev/null    || { echo "нужен curl"; exit 1; }
command -v python3 >/dev/null || { echo "нужен python3"; exit 1; }

# uuid: uuidgen, если есть, иначе через python3 (для переносимости)
gen_uuid() {
  if command -v uuidgen >/dev/null; then uuidgen
  else python3 -c "import uuid; print(uuid.uuid4())"; fi
}

# Сборка тела запроса. Аргументы: amount currency description metadata_json
# Пустые description / metadata просто не попадают в тело.
make_body() {
  python3 - "$1" "$2" "$3" "$4" "$WEBHOOK_URL" <<'PY'
import json, sys
amount, currency, desc, meta, webhook = sys.argv[1:6]
body = {"amount": amount, "currency": currency, "webhook_url": webhook}
if desc:
    body["description"] = desc
if meta:
    body["metadata"] = json.loads(meta)
print(json.dumps(body, ensure_ascii=False))
PY
}

# --- набор разных транзакций ---
BODIES=(
  "$(make_body 199.99  RUB 'Подписка Pro'      '{"order_id":12345,"user_id":777}')"
  "$(make_body 49.50   USD 'Разовый донат'     '')"
  "$(make_body 1500.00 EUR 'Оплата заказа #42' '{"order_id":42,"items":3}')"
  "$(make_body 9.99    RUB ''                   '{"trial":true}')"
)

PAYMENT_IDS=()
LAST_IDEM=""
LAST_BODY=""

post_payment() {
  local body="$1" idem hdrs resp pid
  idem="order-$(gen_uuid)"
  hdrs="$(mktemp)"

  echo "──────────────────────────────────────────────────────────"
  echo ">>> POST /payments   Idempotency-Key: $idem"
  echo "    body: $body"

  resp="$(curl -s -D "$hdrs" -X POST "$API/api/v1/payments" \
            -H "X-API-Key: $KEY" \
            -H "Idempotency-Key: $idem" \
            -H "Content-Type: application/json" \
            -d "$body")"

  echo "--- заголовки ответа ---"
  grep -iE "HTTP/|Idempotency-Key|Idempotent-Replayed|Content-Type" "$hdrs"
  echo "--- тело ---"
  echo "$resp" | python3 -m json.tool --no-ensure-ascii 2>/dev/null || echo "$resp"

  pid="$(echo "$resp" | python3 -c \
    "import sys,json; print(json.load(sys.stdin).get('payment_id',''))" 2>/dev/null)"
  if [ -n "$pid" ]; then
    PAYMENT_IDS+=("$pid")
    LAST_IDEM="$idem"
    LAST_BODY="$body"
    echo "payment_id = $pid"
  else
    echo "!!! payment_id не получен — запрос не прошёл (см. ответ выше)"
  fi

  rm -f "$hdrs"
}

echo "API=$API   KEY=$KEY   WEBHOOK_URL=$WEBHOOK_URL"
echo "Отправляю ${#BODIES[@]} разных платежей..."

for b in "${BODIES[@]}"; do
  post_payment "$b"
done

if [ -n "$LAST_IDEM" ]; then
  echo
  echo "=== Идемпотентный повтор последнего запроса (ожидаем 200 + Idempotent-Replayed: true) ==="
  curl -s -o /dev/null -D - -X POST "$API/api/v1/payments" \
    -H "X-API-Key: $KEY" \
    -H "Idempotency-Key: $LAST_IDEM" \
    -H "Content-Type: application/json" \
    -d "$LAST_BODY" \
    | grep -iE "HTTP/|Idempotency-Key|Idempotent-Replayed"
fi

if [ "${#PAYMENT_IDS[@]}" -eq 0 ]; then
  echo
  echo "Ни один платёж не создан — нечего опрашивать. Запущен ли API на $API?"
  exit 1
fi

echo
echo "=== Ждём $WAIT с, пока consumer обработает платежи (эмуляция 2–5 с)... ==="
sleep "$WAIT"

echo
echo "=== Финальный статус каждого платежа ==="
for pid in "${PAYMENT_IDS[@]}"; do
  echo "── GET /payments/$pid ──"
  curl -s "$API/api/v1/payments/$pid" -H "X-API-Key: $KEY" \
    | python3 -m json.tool --no-ensure-ascii 2>/dev/null || echo "(не удалось получить платёж)"
done

echo
echo "=== Сводка ==="
for pid in "${PAYMENT_IDS[@]}"; do
  line="$(curl -s "$API/api/v1/payments/$pid" -H "X-API-Key: $KEY" | python3 -c \
    "import sys,json; d=json.load(sys.stdin); print(f\"{d['status']:<10} {d['amount']:>10} {d['currency']}\")" \
    2>/dev/null)"
  printf "  %s  %s\n" "$pid" "${line:-<нет данных>}"
done
echo
echo "Статусы pending означают, что consumer ещё не дообработал — увеличь WAIT и повтори GET."
