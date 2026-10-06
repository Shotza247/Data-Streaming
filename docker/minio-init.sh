#!/bin/sh
# docker/minio-init.sh
#
# One-shot initialisation script that runs inside the minio/mc container.
# Waits for MinIO to become available, configures the mc alias, creates
# all required buckets, and sets them to download policy.
#
# NOTE: modern mc uses `mc alias set` not `mc config host add`.

# --- Configuration ---------------------------------------------------------
MINIO_HOST="${MINIO_ENDPOINT:-minio:9000}"
MINIO_ALIAS="local"
MINIO_ACCESS_KEY="${MINIO_ACCESS_KEY:-minioadmin}"
MINIO_SECRET_KEY="${MINIO_SECRET_KEY:-minioadmin}"

BUCKET_RAW="${MINIO_BUCKET_RAW:-raw-tax}"
BUCKET_CLEANED="${MINIO_BUCKET_CLEANED:-cleaned-tax}"
BUCKET_MLFLOW="${MINIO_BUCKET_MLFLOW:-mlflow-artifacts}"

# --- Wait for MinIO to become available ------------------------------------
echo "Waiting for MinIO at ${MINIO_HOST} ..."
for i in $(seq 1 30); do
    if mc alias set "${MINIO_ALIAS}" "http://${MINIO_HOST}" \
        "${MINIO_ACCESS_KEY}" "${MINIO_SECRET_KEY}" 2>/dev/null; then
        echo "MinIO is ready."
        break
    fi
    echo "  attempt ${i}/30 — not ready yet, retrying in 2s..."
    sleep 2
done

# --- Create buckets --------------------------------------------------------
echo "Creating buckets..."
mc mb --ignore-existing "${MINIO_ALIAS}/${BUCKET_RAW}"
mc mb --ignore-existing "${MINIO_ALIAS}/${BUCKET_CLEANED}"
mc mb --ignore-existing "${MINIO_ALIAS}/${BUCKET_MLFLOW}"

# --- Set download policy on all buckets ------------------------------------
echo "Setting bucket policies to download (public-read)..."
mc anonymous set download "${MINIO_ALIAS}/${BUCKET_RAW}"
mc anonymous set download "${MINIO_ALIAS}/${BUCKET_CLEANED}"
mc anonymous set download "${MINIO_ALIAS}/${BUCKET_MLFLOW}"

echo "Listing buckets:"
mc ls "${MINIO_ALIAS}/"

echo "MinIO bucket initialisation complete."
