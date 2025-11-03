#!/bin/bash
set -euo pipefail

# ==== Config ====
CONTAINER="mongo"   # name of your running MongoDB container
BACKUP_ROOT="/home/kardinal/projects/cian/database_backup"

# ==== Checks ====
command -v docker >/dev/null 2>&1 || { echo "❌ Docker not found"; exit 1; }

docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER" \
  || { echo "❌ Container '$CONTAINER' does not exist"; exit 1; }

docker ps --format '{{.Names}}' | grep -qx "$CONTAINER" \
  || { echo "❌ Container '$CONTAINER' is not running"; exit 1; }

# ==== Prep ====
TS=$(date +%Y%m%d_%H%M%S)
HOST_DEST="$BACKUP_ROOT/dump_$TS"
mkdir -p "$HOST_DEST"

echo "📦 Starting MongoDB backup from container: $CONTAINER"
echo "📂 Host destination: $HOST_DEST"

# Clean any previous temp dump inside container (ignore errors)
docker exec "$CONTAINER" sh -c 'rm -rf /tmp/mongo_dump && mkdir -p /tmp/mongo_dump'

# Create dump inside container (to /tmp which is writable)
docker exec "$CONTAINER" sh -c 'mongodump --out /tmp/mongo_dump'
echo "📂 Dumped to the tmp"

# Copy dump out to host
docker cp "$CONTAINER:/tmp/mongo_dump/." "$HOST_DEST/"
echo "📂 copied to the destination folder"

# Clean temp dump inside container
docker exec "$CONTAINER" sh -c 'rm -rf /tmp/mongo_dump'

echo "✅ Backup completed. Contents:"
find "$HOST_DEST" -maxdepth 2 -type f | sed 's/^/   /'
echo "🎉 Done: $HOST_DEST"

echo "setting permissions"
/bin/chown -R kardinal:kardinal /home/kardinal/projects/cian/database_backup
/bin/chmod -R u+rwX /home/kardinal/projects/cian/database_backup
echo "finished"
