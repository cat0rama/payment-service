#!/usr/bin/env bash
# Демонстрация Dead Letter Queue: создаёт платежи с заведомо ПАДАЮЩИМ webhook_url,
# из-за чего consumer повторяет обработку (MAX_PROCESSING_ATTEMPTS раз с backoff),
# а затем перекладывает сообщение в payments.dlq.
#
# Скрипт в реальном времени показывает счётчики очередей payments.new / .retry / .dlq
# (через RabbitMQ Management API), так что видно, как сообщение бегает по retry и
# в итоге оседает в DLQ. В конце — peek содержимого DLQ (без удаления) и статусы.
#
# Нужен поднятый стек: `make up` (api + consumer + rabbitmq + postgres).
#
# Настройки через переменные окружения (со значениями по умолчанию):
#   API=http://localhost:8000            базовый URL API
#   KEY=super-secret-api-key             X-API-Key
#   RABBIT_MGMT=http://localhost:15672   RabbitMQ Management API
#   RABBIT_USER=guest / RABBIT_PASS=guest
#   FAIL_WEBHOOK_URL=https://example.com/always-fails-405   (POST -> 405, всегда фейл)
#   COUNT=3                              сколько платежей создать
#   WATCH=150                            сколько секунд максимум следить за очередями
#
# Пример:  ./test_dlq_flow.sh
#          COUNT=5 FAIL_WEBHOOK_URL=https://httpstat.us/500 ./test_dlq_flow.sh

set -uo pipefail

API="${API:-http://localhost:8000}"
KEY="${KEY:-super-secret-api-key}"
RABBIT_MGMT="${RABBIT_MGMT:-http://localhost:15672}"
RABBIT_USER="${RABBIT_USER:-guest}"
RABBIT_PASS="${RABBIT_PASS:-guest}"
FAIL_WEBHOOK_URL="${FAIL_WEBHOOK_URL:-https://example.com/always-fails-405}"
COUNT="${COUNT:-3}"
WATCH="${WATCH:-150}"
VHOST="%2F"  # дефолтный vhost "/" в url-кодировке

command -v curl >/dev/null    || { echo "нужен curl"; exit 1; }
command -v python3 >/dev/null || { echo "нужен python3"; exit 1; }

gen_uuid() {
  if command -v uuidgen >/dev/null; then uuidgen
  else python3 -c "import uuid; print(uuid.uuid4())"; fi
}

# Число сообщений в очереди через Management API ('-' если очереди ещё нет).
q_count() {
  curl -s -u "$RABBIT_USER:$RABBIT_PASS" "$RABBIT_MGMT/api/queues/$VHOST/$1" \
    | python3 -c "import sys,json
try: print(json.load(sys.stdin).get('messages','-'))
except Exception: print('-')"
}

# Печатает "<messages_now> <publish_total>" для очереди $1:
# messages_now  — сколько сообщений лежит ПРЯМО СЕЙЧАС (мгновенный гейдж);
# publish_total — сколько сообщений ВСЕГО зашло в очередь за всё время (кумулятив).
# Для payments.retry publish_total = суммарное число попыток ретрая.
q_two() {
  curl -s -u "$RABBIT_USER:$RABBIT_PASS" "$RABBIT_MGMT/api/queues/$VHOST/$1" \
    | python3 -c "import sys,json
try:
    d=json.load(sys.stdin)
    print(d.get('messages','-'), (d.get('message_stats') or {}).get('publish','-'))
except Exception:
    print('- -')"
}

# --- preflight ---
if ! curl -s -o /dev/null --max-time 3 "$API/health"; then
  echo "API недоступен на $API. Подними стек: make up"; exit 1
fi
if ! curl -s -o /dev/null --max-time 3 -u "$RABBIT_USER:$RABBIT_PASS" "$RABBIT_MGMT/api/overview"; then
  echo "RabbitMQ Management недоступен на $RABBIT_MGMT (нужен образ *-management)."; exit 1
fi

echo "API=$API   FAIL_WEBHOOK_URL=$FAIL_WEBHOOK_URL   COUNT=$COUNT"
dlq_base="$(q_count payments.dlq)"
[ "$dlq_base" = "-" ] && dlq_base=0
echo "Стартовое число сообщений в payments.dlq: $dlq_base"
echo

# --- создаём платежи с падающим webhook ---
PAYMENT_IDS=()
for i in $(seq 1 "$COUNT"); do
  idem="dlq-$(gen_uuid)"
  body="$(python3 -c "import json,sys; print(json.dumps({
    'amount':'10.00','currency':'RUB',
    'description':'DLQ demo #'+sys.argv[1],
    'webhook_url':sys.argv[2]}))" "$i" "$FAIL_WEBHOOK_URL")"
  resp="$(curl -s -X POST "$API/api/v1/payments" \
            -H "X-API-Key: $KEY" -H "Idempotency-Key: $idem" \
            -H "Content-Type: application/json" -d "$body")"
  pid="$(echo "$resp" | python3 -c "import sys,json; print(json.load(sys.stdin).get('payment_id',''))" 2>/dev/null)"
  if [ -n "$pid" ]; then
    PAYMENT_IDS+=("$pid")
    echo "создан платёж $i: $pid"
  else
    echo "ошибка создания платежа $i: $resp"
  fi
done

if [ "${#PAYMENT_IDS[@]}" -eq 0 ]; then
  echo "Ни один платёж не создан — прекращаю."; exit 1
fi

target=$(( dlq_base + ${#PAYMENT_IDS[@]} ))
echo
echo "=== Слежу за очередями (ждём, пока в DLQ станет >= $target) ==="
echo "Параллельно можно смотреть попытки в логах: docker compose logs -f consumer"
echo "retry(сейчас) — мгновенно (сообщение лежит там лишь ~2–4с по TTL);"
echo "retry(всего)  — сколько раз сообщения вообще заходили в retry (кумулятив)."
printf "  %-6s | %-8s | %-8s %-8s | %-8s\n" "t,с" "new" "retry,сейч" "retry,всего" "dlq"

start=$(date +%s); deadline=$(( start + WATCH ))
while :; do
  now=$(date +%s); t=$(( now - start ))
  nn="$(q_two payments.new)";    n_now="${nn%% *}"
  rr="$(q_two payments.retry)";  r_now="${rr%% *}"; r_tot="${rr##* }"
  dd="$(q_two payments.dlq)";    d_now="${dd%% *}"
  printf "  %-6s | %-8s | %-8s %-8s | %-8s\n" "$t" "$n_now" "$r_now" "$r_tot" "$d_now"
  if [ "$d_now" != "-" ] && [ "$d_now" -ge "$target" ] 2>/dev/null; then
    echo ">>> Все ${#PAYMENT_IDS[@]} сообщений добрались до DLQ (retry,всего показывает суммарные попытки)."; break
  fi
  if [ "$now" -ge "$deadline" ]; then
    echo ">>> Истёк лимит ожидания ($WATCH c). Возможно, нужно увеличить WATCH."; break
  fi
  sleep 1
done

echo
echo "=== Содержимое DLQ (peek без удаления: ackmode=ack_requeue_true) ==="
curl -s -u "$RABBIT_USER:$RABBIT_PASS" -H "content-type: application/json" \
  -X POST "$RABBIT_MGMT/api/queues/$VHOST/payments.dlq/get" \
  -d "{\"count\":$target,\"ackmode\":\"ack_requeue_true\",\"encoding\":\"auto\"}" \
  | python3 -c "import sys,json
for m in json.load(sys.stdin):
    h=(m.get('properties') or {}).get('headers') or {}
    print('payload      :', m.get('payload'))
    print('  x-retry-count :', h.get('x-retry-count'))
    print('  x-death-reason:', h.get('x-death-reason'))" 2>/dev/null || echo "(не удалось прочитать DLQ)"

echo
echo "=== Статусы платежей ==="
echo "(status обычно succeeded — шлюз отработал; в DLQ сообщение ушло из-за провала webhook)"
for pid in "${PAYMENT_IDS[@]}"; do
  curl -s "$API/api/v1/payments/$pid" -H "X-API-Key: $KEY" | python3 -c "import sys,json
d=json.load(sys.stdin)
print(f\"  {d['payment_id']}  status={d['status']}  failure_reason={d.get('failure_reason')}\")" 2>/dev/null
done
