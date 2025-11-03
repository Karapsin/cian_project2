#!/bin/bash
set -euo pipefail

# ==== Config (edit paths/names if needed) ====
CONTAINER="mongo"  # name for the new MongoDB container on THIS laptop
DATA_DIR="/home/kardinal/projects/cian/db"  # fresh data dir on THIS laptop
BACKUP_ROOT="/home/kardinal/projects/cian/loaded_backup"  # where dump_* folders live

# ==== Pick latest backup ====
LATEST_DUMP=$(ls -d "$BACKUP_ROOT"/dump_* 2>/dev/null | sort -V | tail -n1 || true)
if [[ -z "${LATEST_DUMP}" ]]; then
  echo "❌ No dump_* folder found in $BACKUP_ROOT"; exit 1
fi
echo "🗂 Using backup: $LATEST_DUMP"

# ==== Start fresh MongoDB ====
mkdir -p "$DATA_DIR"
echo "🧹 Ensuring a clean DB directory..."
rm -rf "${DATA_DIR:?}/"* 2>/dev/null || true

# Remove any old container with same name
if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER"; then
  docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
fi

echo "🚀 Starting MongoDB container: $CONTAINER"
docker run -d --name "$CONTAINER" -p 27017:27017 -v "$DATA_DIR:/data/db" mongo:latest

# Wait a moment for mongod to be ready (simple retry)
echo "⏳ Waiting for mongod..."
for i in {1..20}; do
  if docker exec "$CONTAINER" mongo --quiet --eval 'db.runCommand({ ping: 1 })' >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

# ==== Restore ====
echo "📥 Restoring from dump into fresh Mongo..."
docker run --rm \
  --network "container:${CONTAINER}" \
  -v "${LATEST_DUMP}:/dump:ro" \
  mongo:latest \
  mongorestore --host localhost --drop /dump

echo "✅ Restore complete. Mongo is running at mongodb://localhost:27017"
