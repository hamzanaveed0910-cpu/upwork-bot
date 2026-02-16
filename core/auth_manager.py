import asyncio
# This file helps us get the special info (headers/tokens) needed to look like a real user on Upwork
from DrissionPage import ChromiumPage, ChromiumOptions
from core.logger import log
import time
import os

# This class handles getting the login info for the bot
class AuthManager:
    def __init__(self):
        # We use a permanent profile to build trust with Upwork
        # Allow overriding profile path via environment for flexibility
        self.profile_path = os.environ.get('UPWORK_PROFILE_PATH') or os.path.join(os.getcwd(), "upwork_bot_profile")

    def intercept_headers(self):
        """Open a browser and capture the headers/tokens needed to act like a real user.

        Note: this is a blocking function (uses DrissionPage which is not async-safe).
        Call it from an executor (e.g. `await asyncio.to_thread(auth.intercept_headers)`).
        """
        log.info("🛡️ Launching Automated Browser to grab tokens...")

        co = ChromiumOptions()
        # Tell it where Chrome is installed (configurable via CHROME_PATH env var)
        chrome_path = os.environ.get('CHROME_PATH') or r"C:\Program Files\Google\Chrome\Application\chrome.exe"
        co.set_browser_path(chrome_path)
        # Use our bot profile so Upwork trusts us (path can be overridden via UPWORK_PROFILE_PATH)
        co.set_paths(user_data_path=self.profile_path)
        co.set_argument('--start-maximized')

        page = None
        try:
            page = ChromiumPage(co)
            # Start listening to ALL traffic
            page.listen.start()

            log.info("🧭 Navigating to Upwork home to trigger session...")
            page.get("https://www.upwork.com/nx/search/jobs/")

            # Wait for the page to load
            time.sleep(4)
            log.info("📍 Waiting for job search request...")

            # Try scrolling to make sure Upwork sends us the right info
            for attempt in range(3):
                log.info(f"🔄 Attempt {attempt + 1}/3 - Scrolling...")
                page.scroll.down(5)
                time.sleep(2)

            timeout = 60
            start_time = time.time()
            backup_headers = None

            # Wait for the special request that has the token we need
            while time.time() - start_time < timeout:
                res = page.listen.wait(timeout=1)
                if res:
                    try:
                        url_lower = res.request.url.lower()
                        headers_raw = res.request.headers
                    except Exception:
                        continue

                    if "graphql" in url_lower and "authorization" in headers_raw:
                        request_type = res.request.url.split('alias=')[-1]
                        headers = dict(headers_raw)
                        # Only return if it's the correct job search request
                        if "visitor" in request_type.lower() and "job" in request_type.lower():
                            log.info(f"✅ JOB SEARCH TOKEN CAPTURED: {request_type}")
                            page.listen.stop()
                            page.quit()
                            return headers
                        # If it's not the main job search, keep as backup
                        backup_headers = headers

            # If we didn't get the main token, use the backup
            if backup_headers:
                log.warning("⚠️ - continuing with available headers")
                page.listen.stop()
                page.quit()
                return backup_headers

            log.error("❌ Timeout: Failed to capture any valid GraphQL token.")
            if page:
                page.quit()
            return None

        except Exception as e:
            log.error(f"❌ Automation Error: {e}")
            try:
                if page:
                    page.quit()
            except Exception:
                pass
            return None