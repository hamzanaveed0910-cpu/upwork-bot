import asyncio
import time
import json
import signal
import base64
import json as _json
import os
from dotenv import load_dotenv
from datetime import datetime, timezone
# Import the bot parts: scraper gets jobs, auth_manager gets login info, logger prints messages
from core.scraper import UpworkScraper
from core.auth_manager import AuthManager
from core.logger import log
from core.discord_poster import DiscordPoster
from core.config_manager import ConfigManager
# Import database functions
from core import db

async def main():

    # Make the scraper and auth manager objects
    scraper = UpworkScraper()
    auth = AuthManager()
    # Load config via ConfigManager (allows runtime updates)
    config_mgr = ConfigManager('config.json')
    config = config_mgr.get_config()

    # Validate config
    tracked_raw = config.get('tracked_urls')
    if not isinstance(tracked_raw, list) or not tracked_raw:
        log.error("Invalid or missing 'tracked_urls' in config.json. Provide a non-empty list.")
        return
    for entry in tracked_raw:
        if not isinstance(entry, dict) or 'url' not in entry or 'label' not in entry:
            log.error("Each tracked_urls entry must be an object with 'url' and 'label'.")
            return

    # Load environment variables (secrets should be in .env)
    load_dotenv()
    # Initialize Discord poster if token provided (prefer env var)
    discord_token = os.getenv('DISCORD_TOKEN') or (config.get('discord_token') if config else None)
    discord_poster = None
    if discord_token and discord_token.strip():
        try:
            discord_poster = DiscordPoster(discord_token.strip(), config_manager=config_mgr)
            # attach scraper so Discord commands can run on-demand searches
            try:
                discord_poster.scraper = scraper
            except Exception:
                pass
            log.info("Discord poster initialized.")
        except Exception as e:
            log.warning(f"Discord poster failed to initialize: {e}")
            discord_poster = None
    else:
        log.info("No Discord token provided; Discord features disabled.")

    # Make sure the jobs table exists in the database and seen_jobs
    await db.async_create_jobs_table()

    # Print a nice header so you know the bot started
    print("\n" + "╔" + "═"*78 + "╗")
    print("║" + " "*78 + "║")
    print("║" + "🤖 UPWORK JOB BOT - AUTOMATED JOB MONITOR".center(78) + "║")
    print("║" + " "*78 + "║")
    print("╚" + "═"*78 + "╝\n")
    
    log.info("🚀 Starting Automated Upwork Bot...")


    # Step 1: Get the special info (headers) needed to look like a real user
    # `intercept_headers` is blocking; run it in a thread
    headers = await asyncio.to_thread(auth.intercept_headers)
    if headers:
        # If we got headers, put them in the scraper
        scraper.update_headers(headers)
    else:
        # If we didn't get headers, stop the bot
        log.critical("❌ Setup failed. No tokens captured.")
        return

    # Let the user know the bot is ready
    log.info("⚡ Bot is online! Starting the monitoring loop...\n")

    # Token refresh policy
    TOKEN_REFRESH_HOURS = config.get('token_refresh_hours', 11)
    TOKEN_REFRESH_MARGIN = 60 * 60  # refresh 1 hour before expiry when possible
    last_token_refresh = time.time()

    def _get_token_expiry_from_headers(headers_dict):
        # Try to decode Bearer JWT token expiry without external deps
        auth_hdr = headers_dict.get('authorization') or headers_dict.get('Authorization')
        if not auth_hdr or not auth_hdr.lower().startswith('bearer '):
            return None
        token = auth_hdr.split(' ', 1)[1]
        parts = token.split('.')
        if len(parts) < 2:
            return None
        try:
            payload_b64 = parts[1]
            # pad base64
            padding = '=' * (-len(payload_b64) % 4)
            payload_bytes = base64.urlsafe_b64decode(payload_b64 + padding)
            payload = _json.loads(payload_bytes.decode('utf-8'))
            exp = payload.get('exp')
            if exp:
                return float(exp)
        except Exception:
            return None
        return None

    token_expiry = _get_token_expiry_from_headers(headers) or (time.time() + TOKEN_REFRESH_HOURS * 3600)

    # Helper function to validate job relevance to the searched keyword
    def _is_job_relevant(job: dict, keyword: str) -> bool:
        """Check if job title or description contains the searched keyword.
        
        Uses case-insensitive substring matching. Returns True if at least one
        word from the keyword is found in the job title or description.
        """
        if not keyword or not isinstance(job, dict):
            return True
        
        title = (job.get('title') or '').lower()
        description = (job.get('description') or '').lower()
        combined = f"{title} {description}"
        
        # Split keyword into words and check if any significant word appears in the job
        keywords = keyword.lower().split()
        # Filter out very short words (1 char) which cause false positives
        keywords = [k for k in keywords if len(k) > 1]
        
        if not keywords:
            return True
        
        # Require at least one keyword word to match (not all words)
        for kw in keywords:
            if kw in combined:
                return True
        
        return False

    # Step 2: Keep checking for new jobs forever, processing each tracked URL (concurrently)
    # `tracked` will be obtained fresh each loop from `config_mgr` to pick up runtime changes
    fetch_interval = (config.get('fetch_interval', 30) if config else 30)

    # Monitoring state
    start_time = time.time()
    last_refresh_time = last_token_refresh

    stop = False

    def _signal_handler(sig, frame):
        nonlocal stop
        log.info("Shutdown signal received")
        stop = True

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    async def scrape_single_url(url_config):
        url = url_config.get('url')
        label = url_config.get('label') or url
        keyword = url_config.get('keyword', "")
        channel_id = url_config.get('channel_id')
        # Ask scraper for jobs from this single URL using per-URL keyword
        jobs, total, success = await scraper.fetch_jobs(keyword=keyword, urls=url)
        if not success:
            log.warning(f"Fetch failed for {label} ({url})")
            # If the scraper recorded a 401, trigger a reactive refresh
            if getattr(scraper, 'last_status_code', None) == 401:
                log.warning("401 detected during fetch — refreshing tokens reactively...")
                new_headers = await asyncio.to_thread(auth.intercept_headers)
                if new_headers:
                    scraper.update_headers(new_headers)
                    nonlocal token_expiry, last_token_refresh
                    token_expiry = _get_token_expiry_from_headers(new_headers) or (time.time() + TOKEN_REFRESH_HOURS * 3600)
                    last_token_refresh = time.time()
                    log.info("✅ Token refreshed after 401.")
            return
        log.info(f"✅ [{label}] Found {len(jobs)} jobs (total reported {total})")
        for job in jobs:
            try:
                if await db.async_is_new_job_for_source(job['id'], url):
                    # Validate job is actually relevant to the keyword
                    if not _is_job_relevant(job, keyword):
                        log.debug(f"Skipping job '{job.get('title', 'N/A')}' — keyword '{keyword}' not found in title/description")
                        await db.async_mark_job_seen(job['id'], url)
                        continue
                    
                    # Attach source to job record and insert
                    job_record = dict(job)
                    # store human-friendly label as the source/category
                    job_record['categories'] = label
                    await db.async_insert_job(job_record)
                    await db.async_mark_job_seen(job['id'], url)
                    # Console output
                    print(f"\nNew job from [{label}]: {job['title']} ({job['id']})")
                    # Post to Discord if configured
                    if discord_poster and channel_id:
                        log.info(f"Enqueueing job '{job['title'][:50]}' to Discord channel {channel_id}")
                        # Build full details for thread
                        job_type = job.get('jobType', 'N/A')
                        budget = job.get('budget', {})
                        budget_str = 'N/A'
                        if isinstance(budget, dict):
                            if 'amount' in budget:
                                budget_str = f"{budget['amount']} {budget.get('isoCurrencyCode', 'USD')}"
                            elif 'min' in budget and 'max' in budget:
                                budget_str = f"${budget['min']}-${budget['max']}/hr"
                        # Format published time if available
                        def _format_published(published_raw):
                            if not published_raw:
                                return 'N/A'
                            try:
                                # Determine local timezone
                                local_tz = datetime.now().astimezone().tzinfo

                                # If numeric (seconds or milliseconds)
                                if isinstance(published_raw, (int, float)):
                                    ts = float(published_raw)
                                    if ts > 1e12:
                                        ts = ts / 1000.0
                                    dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(local_tz)
                                    return dt.strftime('%Y-%m-%d %H:%M %Z')

                                s = str(published_raw)
                                # If ISO-like timestamp
                                if 'T' in s:
                                    s2 = s.rstrip('Z')
                                    try:
                                        dt = datetime.fromisoformat(s2)
                                        if dt.tzinfo is None:
                                            dt = dt.replace(tzinfo=timezone.utc)
                                        dt = dt.astimezone(local_tz)
                                        return dt.strftime('%Y-%m-%d %H:%M %Z')
                                    except Exception:
                                        return s

                                # Fallback numeric parse
                                try:
                                    ts = float(s)
                                    if ts > 1e12:
                                        ts = ts / 1000.0
                                    dt = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(local_tz)
                                    return dt.strftime('%Y-%m-%d %H:%M %Z')
                                except Exception:
                                    return s
                            except Exception:
                                return str(published_raw)

                        published_raw = job.get('published')
                        published_str = _format_published(published_raw)

                        # Truncate description to avoid bloated files (keep max 5000 chars with truncation note)
                        desc = job.get('description', 'N/A')
                        if isinstance(desc, str) and len(desc) > 5000:
                            desc = desc[:5000] + "\n\n[Description truncated due to length limit]"
                        
                        full_details = f"""**Full Job Details**
**Title:** {job['title']}
**Type:** {job_type}
**Budget:** {budget_str}
**Published:** {published_str}
**Description:** {desc}
**Link:** {job['link']}"""
                        # Short description shown in the main message includes published time
                        short_desc = f"Posted: {published_str}\n{job.get('description','')[:300]}"
                        await discord_poster.enqueue_job({
                            'title': job['title'],
                            'description': short_desc,
                            'link': job['link'],
                            'full_details': full_details
                        }, channel_id)
                    elif not channel_id:
                        log.debug(f"No channel_id configured for {label}; skipping Discord post")
            except Exception as e:
                log.error(f"Error processing job {job.get('id')}: {e}")

    # Background cleanup task to remove old seen_jobs entries
    async def periodic_cleanup():
        while not stop:
            try:
                await db.async_cleanup_old_jobs(days=30)
                log.info("Performed periodic cleanup of seen_jobs.")
            except Exception as e:
                log.error(f"Cleanup error: {e}")
            await asyncio.sleep(60 * 60 * 24)  # run daily

    # Start cleanup task
    asyncio.create_task(periodic_cleanup())

    # Start Discord poster if available
    if discord_poster:
        try:
            await discord_poster.start()
            log.info("Discord poster started.")
        except Exception as e:
            log.error(f"Failed to start Discord poster: {e}")
            discord_poster = None

    while not stop:
        # Refresh token if near expiry
        now = time.time()
        if token_expiry and now > (token_expiry - TOKEN_REFRESH_MARGIN):
            log.info("🔄 Token near expiry — refreshing via browser...")
            new_headers = await asyncio.to_thread(auth.intercept_headers)
            if new_headers:
                scraper.update_headers(new_headers)
                token_expiry = _get_token_expiry_from_headers(new_headers) or (time.time() + TOKEN_REFRESH_HOURS * 3600)
                last_token_refresh = time.time()
                log.info("✅ Token refreshed proactively.")
            else:
                log.warning("⚠️ Failed to refresh token proactively — will retry later.")

        # Get tracked URLs fresh from config manager (runtime changes allowed)
        tracked = config_mgr.get_tracked()
        tasks = [asyncio.create_task(scrape_single_url(u)) for u in tracked]
        if tasks:
            await asyncio.gather(*tasks)

        # Wait configured interval
        await asyncio.sleep(fetch_interval)

    # Cleanup on stop
    if discord_poster:
        try:
            await discord_poster.stop()
        except Exception as e:
            log.error(f"Error stopping Discord poster: {e}")

if __name__ == "__main__":
    import sys
    # If you run this file with 'show-jobs', it will print all jobs saved in the database
    if len(sys.argv) > 1 and sys.argv[1] == 'show-jobs':
        jobs = asyncio.run(db.async_get_all_jobs())
        print(f"\n{'='*80}")
        print(f"Total jobs in database: {len(jobs)}")
        print(f"{'='*80}")
        for idx, (job_id, title, description, jobType, published, budget, link, categories) in enumerate(jobs, 1):
            print(f"\n{'-'*80}")
            print(f"Job #{idx}")
            print(f"ID: {job_id}")
            print(f"Title: {title}")
            print(f"Type: {jobType}")
            print(f"Published: {published}")
            print(f"Link: {link}")
            print(f"Source: {categories}")
            print(f"Budget: {budget}")
            print(f"Description: {description[:200]}{'...' if len(description) > 200 else ''}")
        print(f"\n{'='*80}")
    else:
        try:
            # Run the main bot function
            asyncio.run(main())
        except KeyboardInterrupt:
            log.info("👋 Bot stopped by user.")