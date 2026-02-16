# Upwork Bot

Automated job scraper bot for monitoring Upwork job listings and posting new jobs to Discord.

## Features
- Monitors multiple Upwork job search queries simultaneously
- Filters jobs by keywords
- Saves discovered jobs to SQLite database
- Posts new jobs to Discord channels with detailed information
- Token refresh management for API access

## Setup

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Create a `.env` file with your Discord token:
```
DISCORD_TOKEN=your_token_here
```

3. Configure tracked URLs in `config.json`

4. Run the bot:
```bash
python main.py
```

## Configuration

Edit `config.json` to customize:
- `tracked_urls`: Job search queries with keywords and Discord channel IDs
- `fetch_interval`: How often to check for new jobs (seconds)
- `token_refresh_hours`: When to refresh authentication tokens

## Database

Jobs are stored in `jobs.db` (SQLite) with:
- `jobs` table: Job details (title, description, budget, type, etc.)
- `seen_jobs` table: Tracks which jobs have been processed for each source

## Logging

Bot activity is logged to `bot.log`
