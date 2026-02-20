#!/bin/bash

BACKUP_DIR="./backups"
DATE=$(date +%Y%m%d_%H%M%S)

mkdir -p $BACKUP_DIR

echo "Begin create data layers (MRR, STG, DWH)..."

# Dump each schema from Docker container.
docker exec -t innovis_test-pg_dwh-1 pg_dump -U admin -d data_warehouse -n mrr > $BACKUP_DIR/mrr_backup_$DATE.sql
docker exec -t innovis_test-pg_dwh-1 pg_dump -U admin -d data_warehouse -n stg > $BACKUP_DIR/stg_backup_$DATE.sql
docker exec -t innovis_test-pg_dwh-1 pg_dump -U admin -d data_warehouse -n dwh > $BACKUP_DIR/dwh_backup_$DATE.sql

echo "✅ Backups have been successfully created in the folder $BACKUP_DIR"