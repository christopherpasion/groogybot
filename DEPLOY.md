# Deployment Guide for Hostinger VPS

## Quick Start

### 1. SSH into your VPS
```bash
ssh root@72.62.78.11
```

### 2. Install Docker (if not already installed)
```bash
curl -fsSL https://get.docker.com | sh
```

### 3. Clone your repository
```bash
git clone git@github.com:christopherpasion/groogybot.git
cd groogybot
```

### 4. Create the .env file with your secrets
```bash
nano .env
```

Add the following content (replace with your actual values):
```
DISCORD_TOKEN=your_actual_discord_token
DISCORD_SERVER_ID=your_actual_server_id
SHRINKME_API_KEY=your_shrinkme_key
SHRINKEARN_API_KEY=your_shrinkearn_key
```

Save and exit (Ctrl+X, then Y, then Enter)

### 5. Build and run with Docker Compose
```bash
docker compose up -d --build
```

### 6. Check logs
```bash
docker compose logs -f
```

### 7. Useful commands
```bash
# Stop the bot
docker compose down

# Restart the bot
docker compose restart

# View logs
docker compose logs -f groogybot

# Rebuild after code changes
docker compose up -d --build
```

## Troubleshooting

- **Bot not starting**: Check logs with `docker compose logs`
- **Token invalid**: Make sure your .env file has the correct DISCORD_TOKEN
- **Permission denied**: Run with `sudo` if needed
