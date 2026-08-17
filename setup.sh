#!/bin/bash
# SSD Bot — Local Setup Script
# Run once on your machine: bash setup.sh

set -e
echo ""
echo "=== SSD Bot Setup ==="
echo ""

# 1. Check Python
if ! command -v python3 &>/dev/null; then
  echo "❌ Python 3 not found. Install from https://python.org and re-run."
  exit 1
fi
echo "✅ Python $(python3 --version)"

# 2. Create virtual environment (always recreate to avoid stale paths)
echo "→ Creating virtual environment..."
rm -rf venv
python3 -m venv venv
echo "✅ Virtual environment ready"

# 3. Install dependencies
echo "→ Installing dependencies..."
./venv/bin/pip install -q --upgrade pip
./venv/bin/pip install -q slack-bolt anthropic python-dotenv requests beautifulsoup4
echo "✅ Dependencies installed"

# 4. Create .env if missing
if [ ! -f ".env" ]; then
  cp .env.example .env
  echo ""
  echo "📝 .env file created. Please fill in your credentials:"
  echo ""
  echo "   SLACK_BOT_TOKEN      → https://api.slack.com/apps"
  echo "   SLACK_APP_TOKEN      → https://api.slack.com/apps (App-Level Token)"
  echo "   ANTHROPIC_API_KEY    → https://console.anthropic.com"
  echo "   CONFLUENCE_EMAIL     → your Atlassian email"
  echo "   CONFLUENCE_API_TOKEN → https://id.atlassian.com/manage-profile/security/api-tokens"
  echo ""
  echo "Once filled in, run:  ./venv/bin/python app.py"
else
  echo "✅ .env already exists"
  echo ""
  echo "To start the bot, run:"
  echo "  ./venv/bin/python app.py"
fi

echo ""
echo "=== Setup complete ==="
