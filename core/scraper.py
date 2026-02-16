from curl_cffi.requests import AsyncSession
from core.logger import log
import random

class UpworkScraper:
    def __init__(self):
        self.url = "https://www.upwork.com/api/graphql/v1?alias=visitorJobSearch"
        self.headers = None
        self.last_status_code = None
        self._max_retries = 3
        # The website address where we ask for jobs
        # We need special info (headers) to look like a real user

    def update_headers(self, captured_headers: dict):
        """Inject stolen headers into the scraper"""
        self.headers = captured_headers
        
        # CLEANUP: Remove headers that cause 400/403 errors with curl_cffi
        forbidden = ['content-length', 'Content-Length', 'connection', 'host', 'accept-encoding']
        for h in forbidden:
            if h in self.headers:
                del self.headers[h]
        
        # Ensure we look like the browser that captured the token
        self.headers["content-type"] = "application/json"
        log.info("📥 Scraper: Dynamic tokens injected into Job Search engine.")
        # This function puts the special info (headers) we got from the browser into our bot
        # Remove some headers that can cause errors
        # Make sure we look like the browser

    async def fetch_jobs(self, offset=0, count=10, keyword="", urls=None):
        if not self.headers:
            return [], 0, False
            # If we don't have headers, we can't get jobs

        # Enhanced GraphQL query with available job details
        gql_query = """
        query VisitorJobSearch($request: VisitorJobSearchV1Request!) {
          search {
            universalSearchNuxt {
              visitorJobSearchV1(request: $request) {
                paging { total offset count }
                results {
                  id
                  title
                  description
                  jobTile {
                    job {
                      jobType
                      publishTime
                      hourlyBudgetMin
                      hourlyBudgetMax
                      fixedPriceAmount { amount isoCurrencyCode }
                    }
                  }
                }
              }
            }
          }
        }
        """
        # This is the info we send to Upwork. It includes the keyword for searching.

        payload = {
            "query": gql_query,
            "variables": {
                "request": {
                    "userQuery": keyword,
                    "sort": "recency+desc",
                    "paging": {"offset": offset, "count": count}
                }
            }
        }

        # Allow searching multiple endpoints (backwards-compatible)
        target_urls = []
        if urls:
            if isinstance(urls, (list, tuple)):
                target_urls = list(urls)
            else:
                target_urls = [urls]
        else:
            target_urls = [self.url]

        async def _fetch_from(url):
            backoff = 1
            attempt = 0
            while attempt < self._max_retries:
                try:
                    async with AsyncSession(impersonate="chrome120") as session:
                        r = await session.post(url, json=payload, headers=self.headers, timeout=30)
                        self.last_status_code = r.status_code
                        if r.status_code == 200:
                            data = r.json()
                            if "errors" in data:
                                log.error(f"❌ GraphQL Errors ({url}): {data['errors']}")
                                return [], 0, False

                            search_data = data.get("data", {}).get("search", {}).get("universalSearchNuxt", {}).get("visitorJobSearchV1", {})
                            results = search_data.get("results", [])
                            total = search_data.get("paging", {}).get("total", 0)

                            jobs = []
                            for res in results:
                                job_info = res.get("jobTile", {}).get("job", {})
                                budget = {}
                                fixed_price = job_info.get("fixedPriceAmount", {})
                                if fixed_price:
                                    budget = {
                                        "amount": fixed_price.get("amount"),
                                        "isoCurrencyCode": fixed_price.get("isoCurrencyCode")
                                    }
                                else:
                                    min_rate = job_info.get("hourlyBudgetMin")
                                    max_rate = job_info.get("hourlyBudgetMax")
                                    if min_rate and max_rate:
                                        budget = {
                                            "min": min_rate,
                                            "max": max_rate,
                                            "isoCurrencyCode": "USD"
                                        }

                                job_id = res.get("id")
                                job = {
                                    "id": job_id,
                                    "title": res.get("title"),
                                    "description": res.get("description", ""),
                                    "jobType": job_info.get("jobType"),
                                    "published": job_info.get("publishTime"),
                                    "budget": budget,
                                    "link": f"https://www.upwork.com/jobs/{job_id}/"
                                }
                                jobs.append(job)
                            return jobs, total, True
                        else:
                            # 401 should be handled by caller (reactive refresh)
                            log.warning(f"Scraper HTTP {r.status_code} from {url}")
                            # Retry for transient 5xx codes
                            if 500 <= r.status_code < 600:
                                attempt += 1
                                await asyncio.sleep(backoff)
                                backoff *= 2
                                continue
                            return [], 0, False

                except Exception as e:
                    log.error(f"❌ Scraper Exception ({url}): {e}")
                    attempt += 1
                    jitter = random.uniform(0, backoff)
                    await asyncio.sleep(backoff + jitter)
                    backoff *= 2
                    continue
            return [], 0, False

        # Run requests concurrently across target URLs
        import asyncio
        tasks = [_fetch_from(u) for u in target_urls]
        results = await asyncio.gather(*tasks)

        combined_jobs = []
        combined_total = 0
        overall_success = True
        for jobs, total, success in results:
            if jobs:
                combined_jobs.extend(jobs)
            combined_total += total or 0
            if not success:
                overall_success = False

        return combined_jobs, combined_total, overall_success
            # If something goes wrong, show the error