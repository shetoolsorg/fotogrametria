#!/usr/bin/env bash
set -euo pipefail

# --- Config ---
MINIO_CONTAINER="qfieldcloud-minio-1"
MINIO_ENDPOINT="http://127.0.0.1:9000"
MINIO_USER="minioadmin"
MINIO_PASS="minioadmin"
BUCKET="qfieldcloud-local"

EXPORT_DIR="$(pwd)/metabase_data/gpkg"
STATE_FILE="${EXPORT_DIR}/.versions"

mkdir -p "$EXPORT_DIR"
touch "$STATE_FILE"

run_mc() {
  local cmd="$1"
  sudo docker run --rm \
    --network "container:${MINIO_CONTAINER}" \
    -v "${EXPORT_DIR}:/export" \
    --entrypoint /bin/sh \
    minio/mc -lc "
      mc alias set qfc ${MINIO_ENDPOINT} ${MINIO_USER} ${MINIO_PASS} >/dev/null 2>&1
      ${cmd}
    "
}

get_saved_version() {
  local key="$1"
  grep "^${key}=" "$STATE_FILE" | cut -d= -f2 || true
}

save_version() {
  local key="$1"
  local version="$2"
  grep -v "^${key}=" "$STATE_FILE" > "${STATE_FILE}.tmp" || true
  echo "${key}=${version}" >> "${STATE_FILE}.tmp"
  mv "${STATE_FILE}.tmp" "$STATE_FILE"
}

echo "Scanning projects..."

project_hashes=$(
  run_mc "mc ls qfc/${BUCKET}/projects" \
    | sed 's#^.* ##' \
    | sed 's#/$##' \
    | sed '/^$/d'
)

for project_hash in $project_hashes; do

  file_entries=$(
    run_mc "mc ls qfc/${BUCKET}/projects/${project_hash}/files" \
      | sed 's#^.* ##' \
      | sed '/^$/d' \
      || true
  )

  for entry in $file_entries; do
    case "$entry" in
      *.gpkg/)
        gpkg_name="${entry%/}"

        latest_version=$(
          run_mc "mc ls qfc/${BUCKET}/projects/${project_hash}/files/${gpkg_name}" \
            | sed 's#^.* ##' \
            | sort \
            | tail -n 1
        )

        key="${project_hash}/${gpkg_name}"
        saved_version=$(get_saved_version "$key")

        if [[ "$latest_version" == "$saved_version" ]]; then
          echo "SKIP ${gpkg_name} (unchanged)"
          continue
        fi

        echo "UPDATE ${gpkg_name} -> ${latest_version}"

        target_name="${project_hash}__${gpkg_name}"

        run_mc "mc cp 'qfc/${BUCKET}/projects/${project_hash}/files/${gpkg_name}/${latest_version}' '/export/${target_name}'"

        save_version "$key" "$latest_version"
        ;;
    esac
  done
done

echo "Sync complete."

