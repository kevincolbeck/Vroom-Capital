#!/bin/bash
# Legion Bot — Fresh VPS Setup Script
# Run once on a clean Ubuntu 24.04 server as root:
#   curl -sL https://raw.githubusercontent.com/YOUR_USER/YOUR_REPO/main/deploy/setup.sh | bash
# Or: scp deploy/setup.sh root@YOUR_SERVER_IP:~ && ssh root@YOUR_SERVER_IP bash setup.sh

set -e

REPO_URL=""          # e.g. https://github.com/youruser/aiagent.git  -- fill this in
APP_DIR="/home/legion/aiagent"
APP_USER="legion"
DOMAIN=""            # optional: your domain name e.g. bot.yourdomain.com
                     # leave empty to use IP address only

echo ""
echo "======================================================"
echo "  Legion Bot — VPS Setup"
echo "======================================================"
echo ""

# ── 1. System packages ───────────────────────────────────
echo "[1/8] Installing system packages..."
apt-get update -qq
apt-get install -y -qq \
    python3.11 python3.11-venv python3-pip \
    nginx certbot python3-certbot-nginx \
    git curl wget ufw fail2ban \
    build-essential libssl-dev

# Node.js 20 LTS (for frontend build)
if ! command -v node &>/dev/null; then
    curl -fsSL https://deb.nodesource.com/setup_20.x | bash -
    apt-get install -y nodejs
fi

echo "  Python: $(python3.11 --version)"
echo "  Node:   $(node --version)"
echo "  npm:    $(npm --version)"

# ── 2. Create app user ───────────────────────────────────
echo "[2/8] Creating user '$APP_USER'..."
if ! id "$APP_USER" &>/dev/null; then
    useradd -m -s /bin/bash "$APP_USER"
    echo "  Created user $APP_USER"
else
    echo "  User $APP_USER already exists"
fi

# ── 3. Clone repo ────────────────────────────────────────
echo "[3/8] Cloning repository..."
if [ -z "$REPO_URL" ]; then
    echo ""
    echo "  ERROR: Set REPO_URL at the top of this script before running."
    echo "  e.g. REPO_URL=\"https://github.com/youruser/aiagent.git\""
    exit 1
fi

if [ -d "$APP_DIR/.git" ]; then
    echo "  Repo already exists — pulling latest..."
    cd "$APP_DIR" && git pull
else
    git clone "$REPO_URL" "$APP_DIR"
fi
chown -R "$APP_USER:$APP_USER" "$APP_DIR"

# ── 4. Python virtual environment ────────────────────────
echo "[4/8] Setting up Python environment..."
cd "$APP_DIR"
sudo -u "$APP_USER" python3.11 -m venv venv
sudo -u "$APP_USER" venv/bin/pip install --upgrade pip -q
sudo -u "$APP_USER" venv/bin/pip install -r requirements.txt -q
echo "  Python deps installed"

# ── 5. Build frontend ────────────────────────────────────
echo "[5/8] Building React frontend..."
cd "$APP_DIR/frontend"
sudo -u "$APP_USER" npm install --silent
sudo -u "$APP_USER" npm run build
echo "  Frontend built → frontend/dist/"

# ── 6. Create data directories & .env ───────────────────
echo "[6/8] Setting up data directories..."
sudo -u "$APP_USER" mkdir -p "$APP_DIR/data" "$APP_DIR/data_cache" "$APP_DIR/logs"

if [ ! -f "$APP_DIR/.env" ]; then
    sudo -u "$APP_USER" cp "$APP_DIR/.env.example" "$APP_DIR/.env"
    echo ""
    echo "  !! .env file created from .env.example"
    echo "  !! Edit $APP_DIR/.env and add your API keys before starting the bot"
    echo ""
fi

# ── 7. systemd service ───────────────────────────────────
echo "[7/8] Installing systemd service..."
cp "$APP_DIR/deploy/legion-bot.service" /etc/systemd/system/legion-bot.service
# Substitute actual app directory into service file
sed -i "s|/home/legion/aiagent|$APP_DIR|g" /etc/systemd/system/legion-bot.service
sed -i "s|User=legion|User=$APP_USER|g" /etc/systemd/system/legion-bot.service

systemctl daemon-reload
systemctl enable legion-bot
echo "  systemd service installed and enabled"

# ── 8. nginx ────────────────────────────────────────────
echo "[8/8] Configuring nginx..."
cp "$APP_DIR/deploy/nginx.conf" /etc/nginx/sites-available/legion-bot
sed -i "s|/home/legion/aiagent|$APP_DIR|g" /etc/nginx/sites-available/legion-bot

if [ -n "$DOMAIN" ]; then
    sed -i "s|server_name _;|server_name $DOMAIN;|g" /etc/nginx/sites-available/legion-bot
fi

ln -sf /etc/nginx/sites-available/legion-bot /etc/nginx/sites-enabled/legion-bot
rm -f /etc/nginx/sites-enabled/default
nginx -t && systemctl restart nginx

# Optional: SSL with Let's Encrypt
if [ -n "$DOMAIN" ]; then
    echo "  Setting up SSL for $DOMAIN..."
    certbot --nginx -d "$DOMAIN" --non-interactive --agree-tos -m admin@"$DOMAIN" || true
fi

# ── Firewall ─────────────────────────────────────────────
ufw allow OpenSSH
ufw allow 'Nginx Full'
ufw --force enable

echo ""
echo "======================================================"
echo "  Setup complete!"
echo ""
echo "  Next steps:"
echo "  1. Edit your API keys:  nano $APP_DIR/.env"
echo "  2. Start the bot:       systemctl start legion-bot"
echo "  3. View logs:           journalctl -u legion-bot -f"
if [ -n "$DOMAIN" ]; then
    echo "  4. Admin UI:            https://$DOMAIN"
else
    SERVER_IP=$(curl -s ifconfig.me)
    echo "  4. Admin UI:            http://$SERVER_IP"
fi
echo "======================================================"
echo ""
