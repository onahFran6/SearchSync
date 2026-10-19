#!/usr/bin/env bash
# End-to-end happy-path check against a running stack (infra up, migrations
# applied, connector registered, API and indexer running). Exits non-zero on
# the first failed assertion; intended for manual or CI invocation, not as a
# substitute for the full practical-experiment suite in
# docs/01_CDC_SEARCH_IMPLEMENTATION.md.
set -euo pipefail

cd "$(dirname "$0")/.."
set -a
source .env
set +a

API="http://localhost${API_ADDR}"
ES="${ELASTICSEARCH_URL}"
SKU="SMOKE-$(date +%s)"

echo "== create =="
CREATE_RESP=$(curl -sf -X POST "$API/products" \
  -H "Content-Type: application/json" \
  -d "{\"sku\":\"$SKU\",\"name\":\"Smoke Test Product\",\"description\":\"d\",\"category\":\"smoke\",\"price_cents\":1234}")
ID=$(echo "$CREATE_RESP" | python3 -c "import json,sys; print(json.load(sys.stdin)['id'])")
echo "created id=$ID"

echo "== get =="
curl -sf "$API/products/$ID" | python3 -c "
import json, sys
p = json.load(sys.stdin)
assert p['sku'] == '$SKU', f\"expected sku $SKU, got {p['sku']}\"
assert p['price_cents'] == 1234
print('ok')
"

echo "== wait for CDC propagation =="
for i in $(seq 1 20); do
  if curl -sf "$ES/products/_doc/$ID" | python3 -c "import json,sys; sys.exit(0 if json.load(sys.stdin).get('found') else 1)" 2>/dev/null; then
    echo "indexed after ${i}x0.5s"
    break
  fi
  sleep 0.5
  if [ "$i" -eq 20 ]; then
    echo "FAIL: product $ID never appeared in Elasticsearch" >&2
    exit 1
  fi
done

echo "== search =="
curl -sf "$API/search?q=Smoke" | python3 -c "
import json, sys
results = json.load(sys.stdin)['results']
assert any(r['id'] == '$ID' for r in results), f\"product $ID not found in search results: {results}\"
print('ok')
"

echo "== delete =="
curl -sf -o /dev/null -X DELETE "$API/products/$ID"
curl -s -o /dev/null -w "%{http_code}" "$API/products/$ID" | grep -q 404 && echo "ok: 404 after delete"

echo "== wait for delete propagation =="
for i in $(seq 1 20); do
  if ! curl -sf "$ES/products/_doc/$ID" | python3 -c "import json,sys; sys.exit(0 if json.load(sys.stdin).get('found') else 1)" 2>/dev/null; then
    echo "removed from ES after ${i}x0.5s"
    break
  fi
  sleep 0.5
  if [ "$i" -eq 20 ]; then
    echo "FAIL: product $ID still present in Elasticsearch after delete" >&2
    exit 1
  fi
done

echo
echo "SMOKE TEST PASSED"
