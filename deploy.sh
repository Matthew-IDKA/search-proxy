#!/bin/bash
# deploy.sh -- Deploy search-proxy source to Unraid and rebuild container
#
# Usage:
#   bash D:/infrastructure/projects/search-proxy/deploy.sh              # deploy + rebuild
#   bash D:/infrastructure/projects/search-proxy/deploy.sh --dry-run    # show what would be deployed
#   bash D:/infrastructure/projects/search-proxy/deploy.sh --no-build   # deploy files only, skip rebuild

set -euo pipefail

REMOTE="root@nas.lab.idka.info"
BUILD_DIR="/mnt/user/appdata/search-proxy/build"
COMPOSE_DIR="/boot/config/plugins/compose.manager/projects/inhale"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DRY_RUN=false
NO_BUILD=false

while [[ $# -gt 0 ]]; do
    case "$1" in
        --dry-run)  DRY_RUN=true; shift ;;
        --no-build) NO_BUILD=true; shift ;;
        *)          echo "Unknown arg: $1"; exit 1 ;;
    esac
done

echo "=== search-proxy deploy ==="

FILES=(Dockerfile requirements.txt search_proxy.py)
if $DRY_RUN; then
    echo "Would deploy to $REMOTE:$BUILD_DIR/"
    for f in "${FILES[@]}"; do echo "  $f"; done
    echo "=== Deploy complete (dry run) ==="
    exit 0
fi

echo "Deploying source to $BUILD_DIR/"
# shellcheck disable=SC2029
ssh "$REMOTE" "mkdir -p $BUILD_DIR"
for f in "${FILES[@]}"; do
    scp -q "$SCRIPT_DIR/$f" "$REMOTE:$BUILD_DIR/" && echo "  OK: $f"
done

if $NO_BUILD; then
    echo "=== Deploy complete (no build) ==="
    exit 0
fi

echo "Building image..."
# shellcheck disable=SC2029
ssh "$REMOTE" "cd $BUILD_DIR && docker build -t search-proxy:local . 2>&1 | tail -5"

echo "Restarting container..."
# shellcheck disable=SC2029
ssh "$REMOTE" "cd $COMPOSE_DIR && docker compose up -d search-proxy 2>&1"

echo "=== Deploy complete ==="
