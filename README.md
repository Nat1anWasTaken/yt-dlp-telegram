# yt-dlp-telegram

## Configuration

Create a `.env` file in the project root and set the Telegram bot token:

```
TELOXIDE_TOKEN=YOUR_TELEGRAM_BOT_TOKEN
```

`TELOXIDE_TOKEN` must be a valid token from BotFather. The container will exit and restart if the token is invalid.

## Run

```
docker compose up -d
```
