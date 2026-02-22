#!/bin/bash

BACKUP_DIR="/opt/airflow/backups"
DATE=$(date +%Y%m%d_%H%M%S)

mkdir -p $BACKUP_DIR

echo "Begin create data layers (MRR, STG, DWH)..."

export PGPASSWORD="password"

pg_dump -h pg_dwh -U admin -d data_warehouse -n mrr > $BACKUP_DIR/mrr_backup_$DATE.sql
pg_dump -h pg_dwh -U admin -d data_warehouse -n stg > $BACKUP_DIR/stg_backup_$DATE.sql
pg_dump -h pg_dwh -U admin -d data_warehouse -n dwh > $BACKUP_DIR/dwh_backup_$DATE.sql

if [ $? -eq 0 ]; then
    echo "✅ Backups have been successfully created in the folder $BACKUP_DIR"
    exit 0
else
    echo "❌ ERROR: Failed to create backups!"
    exit 1
fi