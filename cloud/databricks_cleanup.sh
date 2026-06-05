#!/usr/bin/env bash
# cloud/databricks_cleanup.sh
# ============================
# Lists and terminates all idle/broken Databricks clusters.
# Cancels any stuck or failed runs.
# Also shows supported node types so you can pick a working VM.
#
# Usage:
#   source .env
#   bash cloud/databricks_cleanup.sh

WORKSPACE_URL="adb-7405606595163846.6.azuredatabricks.net"
HOST="https://$WORKSPACE_URL"

echo "=== Getting Azure AD token... ==="
AAD_TOKEN=$(az account get-access-token \
  --resource "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d" \
  --query accessToken --output tsv)
echo "   OK"

dbx() {
  curl -s -X "${1}" "$HOST/api/${3}" \
    -H "Authorization: Bearer $AAD_TOKEN" \
    -H "Content-Type: application/json" \
    ${4:+-d "${4}"}
}

# ── 1. List & terminate all clusters ──────────────────────────────────────────
echo ""
echo "=== Clusters ==="
CLUSTERS_RAW=$(dbx GET "" "2.0/clusters/list")

echo "$CLUSTERS_RAW" | python3 -c "
import sys, json
d = json.loads(sys.stdin.read())
clusters = d.get('clusters', [])
if not clusters:
    print('  No clusters found.')
for c in clusters:
    print(f\"  {c['cluster_id']}  {c['state']:12s}  {c.get('cluster_name','?')}\")
"

# Terminate any cluster not already TERMINATED/TERMINATING
CLUSTER_IDS=$(echo "$CLUSTERS_RAW" | python3 -c "
import sys, json
d = json.loads(sys.stdin.read())
dead = {'TERMINATED', 'TERMINATING'}
for c in d.get('clusters', []):
    if c['state'] not in dead:
        print(c['cluster_id'])
")

if [ -z "$CLUSTER_IDS" ]; then
  echo "  Nothing to terminate."
else
  for CID in $CLUSTER_IDS; do
    echo "  Terminating cluster: $CID"
    dbx POST "" "2.0/clusters/delete" "{\"cluster_id\": \"$CID\"}" > /dev/null
    echo "  ✅ Terminate signal sent → $CID"
  done
fi

# ── 2. List & cancel active runs ──────────────────────────────────────────────
echo ""
echo "=== Runs (last 20) ==="
RUNS_RAW=$(dbx GET "" "2.1/jobs/runs/list?limit=20&active_only=false")

echo "$RUNS_RAW" | python3 -c "
import sys, json
d = json.loads(sys.stdin.read())
runs = d.get('runs', [])
if not runs:
    print('  No runs found.')
for r in runs:
    s = r.get('state', {})
    lc = s.get('life_cycle_state', '?')
    rs = s.get('result_state', '')
    print(f\"  {r['run_id']}  {lc:16s}  {rs:10s}  {r.get('run_name','?')}\")
"

# Cancel any runs that are still active (PENDING or RUNNING)
ACTIVE_RUNS=$(echo "$RUNS_RAW" | python3 -c "
import sys, json
d = json.loads(sys.stdin.read())
active = {'PENDING', 'RUNNING', 'TERMINATING'}
for r in d.get('runs', []):
    if r.get('state', {}).get('life_cycle_state') in active:
        print(r['run_id'])
")

if [ -z "$ACTIVE_RUNS" ]; then
  echo "  No active runs to cancel."
else
  for RID in $ACTIVE_RUNS; do
    echo "  Cancelling run: $RID"
    dbx POST "" "2.1/jobs/runs/cancel" "{\"run_id\": $RID}" > /dev/null
    echo "  ✅ Cancel signal sent → $RID"
  done
fi

# ── 3. Show available node types (smallest first) ─────────────────────────────
echo ""
echo "=== Supported Node Types (smallest RAM first) ==="
dbx GET "" "2.0/clusters/list-node-types" | python3 -c "
import sys, json
d = json.loads(sys.stdin.read())
types = d.get('node_types', [])
# Sort by memory
types.sort(key=lambda x: (x.get('memory_mb', 0), x.get('num_cores', 0)))
print(f\"  {'Node Type':<30} {'vCPUs':>6}  {'RAM GB':>7}\")
print('  ' + '-'*50)
for t in types[:20]:  # show smallest 20
    mem_gb = t.get('memory_mb', 0) / 1024
    cores  = t.get('num_cores', 0)
    name   = t.get('node_type_id', '?')
    print(f\"  {name:<30} {cores:>6}  {mem_gb:>7.1f}\")
"

echo ""
echo "Done. Re-run databricks_setup.sh with a supported node type from the list above."
