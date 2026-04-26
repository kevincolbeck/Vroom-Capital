# Legion Bot — Cloud Deployment Guide

## What you need
- A Hetzner account (hetzner.com) — takes 5 minutes to sign up
- Your Bitunix API key + secret
- Your code pushed to a **private** GitHub repo

---

## Step 1 — Create the VPS

1. Log into cloud.hetzner.com
2. Click **New Server**
3. Select:
   - **Location:** Any (US East or EU — pick closest to you)
   - **Image:** Ubuntu 24.04
   - **Type:** CX22 (2 vCPU, 4GB RAM) — €4/month
   - **SSH Key:** Add your public key (recommended) or use a password
4. Click **Create & Buy Now**
5. Copy the server IP address

---

## Step 2 — Push your code to GitHub

On your laptop:
```bash
cd C:\Users\kevin\Downloads\aiagent
git init
git add .
git commit -m "Initial deploy"
git remote add origin https://github.com/YOUR_USERNAME/YOUR_REPO.git
git push -u origin main
```

Make sure the repo is **Private** — it contains your bot logic.

---

## Step 3 — Run setup on the server

SSH into your server:
```bash
ssh root@YOUR_SERVER_IP
```

Edit the setup script to add your repo URL, then run it:
```bash
# Download the setup script
curl -O https://raw.githubusercontent.com/YOUR_USERNAME/YOUR_REPO/main/deploy/setup.sh

# Edit REPO_URL at the top of the file
nano setup.sh

# Run it
bash setup.sh
```

This takes about 3-5 minutes. It installs everything automatically.

---

## Step 4 — Add your API keys

```bash
nano /home/legion/aiagent/.env
```

Fill in:
- `BITUNIX_API_KEY` and `BITUNIX_API_SECRET` — from Bitunix → Account → API Management
- `ADMIN_PASSWORD` — what you'll use to log into the dashboard
- `SECRET_KEY` — any random 32+ character string (make one at random.org)

Save and exit (Ctrl+X, Y, Enter).

---

## Step 5 — Start the bot

```bash
systemctl start legion-bot
```

Check it's running:
```bash
systemctl status legion-bot
```

View live logs:
```bash
journalctl -u legion-bot -f
```

---

## Step 6 — Open the admin dashboard

Open your browser and go to:
```
http://YOUR_SERVER_IP
```

Log in with your `ADMIN_PASSWORD`. The bot is running 24/7.

---

## Day-to-day commands

| Action | Command |
|--------|---------|
| View live logs | `journalctl -u legion-bot -f` |
| Restart bot | `systemctl restart legion-bot` |
| Stop bot | `systemctl stop legion-bot` |
| Start bot | `systemctl start legion-bot` |
| Check status | `systemctl status legion-bot` |
| Deploy update | `bash /home/legion/aiagent/deploy/update.sh` |

---

## Deploying code updates

Whenever you change the code on your laptop:

```bash
# On your laptop — push changes
git add .
git commit -m "Update strategy"
git push

# On the server — pull and restart
ssh root@YOUR_SERVER_IP
bash /home/legion/aiagent/deploy/update.sh
```

The update script pulls new code, updates dependencies, rebuilds the frontend, and restarts the bot — all in one command.

---

## Optional: Add a domain name

If you want `https://bot.yourdomain.com` instead of a raw IP:

1. Buy a domain (Namecheap, Cloudflare, etc.)
2. Add an **A record** pointing to your server IP
3. Re-run setup with `DOMAIN="bot.yourdomain.com"` set at the top of setup.sh
4. SSL certificate is installed automatically via Let's Encrypt

---

## Security notes

- The admin dashboard is protected by JWT login — only you can access it
- The bot runs as a non-root `legion` user
- Firewall only allows SSH (22), HTTP (80), and HTTPS (443)
- Your `.env` file is never committed to git
- fail2ban is installed to block brute-force SSH attempts
