# yt-dlp-telegram

A small Telegram bot that takes a media URL, lets you choose a format, and sends the downloaded/converted file back using yt-dlp and ffmpeg.

## Requirements

- Docker + Docker Compose
- A Telegram bot token (from BotFather)

## Configuration

Create a `.env` file in the project root:

```
TELOXIDE_TOKEN=YOUR_TELEGRAM_BOT_TOKEN
```

Optional settings:

```
# Comma-separated list of Telegram user IDs allowed to use the bot
ALLOWED_USER_IDS=123456789,987654321

# yt-dlp tuning
YTDLP_PLAYER_CLIENT=android
YTDLP_FORCE_IPV4=true
```

## Run

```
docker compose up -d
```

## Usage

- Start a chat with your bot.
- Send any media URL.
- Pick a format and output type from the inline buttons.

## Logs

```
docker compose logs -f bot
```
