#!/usr/bin/env bash
#
# fake_broker_data.sh — publishes realistic $SYS/# values to both mosquitto
# brokers so the agent's scraping, InfluxDB writes, and alert rule all have
# something to show during the sprint demo.
#
# Usage:
#   ./fake_broker_data.sh                 # run continuously (Ctrl+C to stop)
#   ./fake_broker_data.sh --spike-cpu     # also trigger the cpu_percent alert once
#
# Requires: mosquitto-clients (`apt install mosquitto-clients` /
#           `brew install mosquitto`) — this gives you `mosquitto_pub`.

set -euo pipefail

BROKER1_HOST="localhost"
BROKER1_PORT=1883
BROKER2_HOST="localhost"
BROKER2_PORT=1884

publish_broker_metrics() {
  local host=$1
  local port=$2
  local clients=$3
  local msgs_sent=$4
  local msgs_recv=$5
  local bytes_sent=$6
  local bytes_recv=$7

  mosquitto_pub -h "$host" -p "$port" -t '$SYS/broker/clients/connected'  -m "$clients"
  mosquitto_pub -h "$host" -p "$port" -t '$SYS/broker/messages/sent'      -m "$msgs_sent"
  mosquitto_pub -h "$host" -p "$port" -t '$SYS/broker/messages/received'  -m "$msgs_recv"
  mosquitto_pub -h "$host" -p "$port" -t '$SYS/broker/bytes/sent'         -m "$bytes_sent"
  mosquitto_pub -h "$host" -p "$port" -t '$SYS/broker/bytes/received'     -m "$bytes_recv"
}

echo "Publishing fake \$SYS metrics to broker-1 (:$BROKER1_PORT) and broker-2 (:$BROKER2_PORT)."
echo "Press Ctrl+C to stop. Watch agent logs / GET /metrics in another terminal."

sent1=0; recv1=0; bsent1=0; brecv1=0
sent2=0; recv2=0; bsent2=0; brecv2=0
tick=0

while true; do
  tick=$((tick + 1))

  # Broker 1: steady, believable growth
  clients1=$(( (RANDOM % 5) + 3 ))
  sent1=$((sent1 + RANDOM % 20 + 5))
  recv1=$((recv1 + RANDOM % 20 + 5))
  bsent1=$((bsent1 + RANDOM % 2000 + 500))
  brecv1=$((brecv1 + RANDOM % 2000 + 500))
  publish_broker_metrics "$BROKER1_HOST" "$BROKER1_PORT" "$clients1" "$sent1" "$recv1" "$bsent1" "$brecv1"

  # Broker 2: a bit noisier, occasional client churn
  clients2=$(( (RANDOM % 8) ))
  sent2=$((sent2 + RANDOM % 15 + 2))
  recv2=$((recv2 + RANDOM % 15 + 2))
  bsent2=$((bsent2 + RANDOM % 1500 + 300))
  brecv2=$((brecv2 + RANDOM % 1500 + 300))
  publish_broker_metrics "$BROKER2_HOST" "$BROKER2_PORT" "$clients2" "$sent2" "$recv2" "$bsent2" "$brecv2"

  echo "[tick $tick] broker-1 clients=$clients1 sent=$sent1 | broker-2 clients=$clients2 sent=$sent2"

  # Optional: one-shot CPU spike to trip the alert rule (cpu_percent > 0.01
  # in your config.toml — deliberately low so a real docker container's CPU
  # usage will cross it almost immediately without needing a stress tool).
  if [[ "${1:-}" == "--spike-cpu" && $tick -eq 3 ]]; then
    echo ">>> Triggering CPU load on broker-1's container to fire the alert rule..."
    docker exec mosquitto sh -c 'yes > /dev/null &' || true
    echo ">>> CPU spike started in background inside the mosquitto container."
  fi

  sleep 2
done
