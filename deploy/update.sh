#!/bin/bash
# Legion Bot — Deploy update after pushing new code to GitHub
# Run on the server: bash /home/legion/aiagent/deploy/update.sh

set -e

APP_DIR="/home/legion/aiagent"
APP_USER="legion"

echo ""
echo "======================================================"
echo "  Legion Bot — Deploying Update"
echo "======================================================"

cd "$APP_DIR"

# ── Pull latest code ─────────────────────────────────────
echo "[1/4] Pulling latest code..."
sudo -u "$APP_USER" git pull origin main
echo "  Done"

# ── Update Python dependencies if requirements changed ───
echo "[2/4] Updating Python dependencies..."
sudo -u "$APP_USER" venv/bin/pip install -r requirements.txt -q
echo "  Done"

# ── Rebuild frontend if source changed ───────────────────
echo "[3/4] Rebuilding frontend..."
cd "$APP_DIR/frontend"
# Ensure dist is writable by the app user before building
chown -R "$APP_USER":"$APP_USER" "$APP_DIR/frontend/dist" 2>/dev/null || true
sudo -u "$APP_USER" npm install --silent
sudo -u "$APP_USER" npm run build
echo "  Done"

# ── Install / refresh Hyblock collector timer ────────────
echo "[4/5] Installing Hyblock collector timer..."
cp "$APP_DIR/deploy/hyblock-collector.service" /etc/systemd/system/hyblock-collector.service
cp "$APP_DIR/deploy/hyblock-collector.timer"   /etc/systemd/system/hyblock-collector.timer
systemctl daemon-reload
systemctl enable --now hyblock-collector.timer
echo "  Done"

# ── Restart bot ──────────────────────────────────────────
echo "[5/5] Restarting bot..."
systemctl restart legion-bot
sleep 3

# Verify it started
if systemctl is-active --quiet legion-bot; then
    echo "  Bot is running"
else
    echo "  ERROR: Bot failed to start — check logs:"
    echo "  journalctl -u legion-bot -n 50"
    exit 1
fi

echo ""
echo "  Update complete!"
echo "  Live logs: journalctl -u legion-bot -f"
echo "  Collector: journalctl -u hyblock-collector -n 20"
echo "======================================================"
echo ""
