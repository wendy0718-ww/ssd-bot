#!/usr/bin/env bash
# Pulls the latest ssd-bot code from GitHub and restarts the service.
# Run this ON THE SERVER as dwan, from /opt/ssd-bot:
#   ./deploy.sh
#
# Safe to re-run any time: .env, venv/, memory.json, and other gitignored
# files are never touched by `git reset --hard` since they aren't tracked.
set -euo pipefail

cd "$(dirname "$0")"

echo "==> Fetching latest changes..."
git fetch origin

BEFORE=$(git rev-parse HEAD)
git reset --hard origin/main
AFTER=$(git rev-parse HEAD)

if [ "$BEFORE" == "$AFTER" ]; then
  echo "==> Already up to date (${AFTER:0:7}). Restarting service anyway..."
else
  echo "==> Updated ${BEFORE:0:7} -> ${AFTER:0:7}"

  # Only reinstall deps if requirements.txt changed in this pull.
  if git diff --name-only "$BEFORE" "$AFTER" | grep -q "^requirements.txt$"; then
    echo "==> requirements.txt changed, reinstalling dependencies..."
    venv/bin/pip install -r requirements.txt --quiet
  fi
fi

echo "==> Restarting service..."
sudo systemctl restart ssd-bot

echo "==> Status:"
sudo systemctl status ssd-bot --no-pager
