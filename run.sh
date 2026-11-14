#!/usr/bin/env bash
set -euo pipefail
llmgw_source="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
source "$llmgw_source/scripts/env.sh"
cd "$llmgw_source"
case "${1:-status}" in
  build)
    go build -trimpath -o "$LLMGW_DATA_ROOT/bin/gateway" ./cmd/gateway
    go build -trimpath -o "$LLMGW_DATA_ROOT/bin/upstream-fixture" ./cmd/upstream-fixture
    go build -race -trimpath -o "$LLMGW_DATA_ROOT/bin/gateway-race" ./cmd/gateway
    ;;
  test) exec /home/zjr/anaconda3/bin/python -B scripts/check.py ;;
  demo) shift; exec /home/zjr/anaconda3/bin/python -B scripts/demo.py "$@" ;;
  showcase) shift; exec /home/zjr/anaconda3/bin/python -B scripts/showcase.py "$@" ;;
  report) exec /home/zjr/anaconda3/bin/python -B scripts/report.py ;;
  diagnose) shift; exec /home/zjr/anaconda3/bin/python -B scripts/analyze_latency.py "$@" ;;
  install-k6) exec /home/zjr/anaconda3/bin/python -B scripts/install_k6.py ;;
  setup|init|start|stop|status|verify|stability|deepen|perf|soak|benchmark|compare|tools|real-load) action="$1"; shift; exec /home/zjr/anaconda3/bin/python -B scripts/runtime.py "$action" "$@" ;;
  *) printf 'Usage: bash run.sh {setup|build|test|init|start|stop|status|verify|stability|deepen|perf|soak|benchmark|compare|tools|real-load|diagnose|demo|showcase|report|install-k6}\n'; exit 2 ;;
esac
