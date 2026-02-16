"""
Database helpers for the Upwork bot.

This module uses `categories` as the human-friendly source label.
It contains migration logic to copy legacy `url_source` values into
`categories` and to remove the old columns safely by rebuilding tables.
"""
import sqlite3
import asyncio
from contextlib import closing

# The name of our database file
DB_PATH = 'jobs.db'


def create_jobs_table():
    """Ensure tables exist and run migrations to normalize schema.

    This function will:
    - Create `jobs` table with a `categories` column.
    - Create `seen_jobs` table keyed by `(job_id, categories)`.
    - If legacy `url_source` columns exist, copy their values to
      `categories` and then recreate tables without `url_source`.
    """
    with closing(sqlite3.connect(DB_PATH)) as conn:
        with conn:
            # Create jobs table with categories column
            conn.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    description TEXT,
                    jobType TEXT,
                    published INTEGER,
                    budget TEXT,
                    link TEXT,
                    categories TEXT
                )
            ''')

            # Create seen_jobs table using categories as the second key
            conn.execute('''
                CREATE TABLE IF NOT EXISTS seen_jobs (
                    job_id TEXT,
                    categories TEXT,
                    first_seen TIMESTAMP,
                    PRIMARY KEY (job_id, categories)
                )
            ''')

            # Migration helper: if legacy `url_source` columns exist,
            # recreate tables without them and copy data across.
            def _drop_legacy_url_source():
                cur = conn.cursor()

                # jobs table migration
                cur.execute("PRAGMA table_info(jobs)")
                job_cols = [r[1] for r in cur.fetchall()]
                if 'url_source' in job_cols:
                    # create new jobs table without url_source
                    cur.execute('''
                        CREATE TABLE IF NOT EXISTS jobs_new (
                            id TEXT PRIMARY KEY,
                            title TEXT,
                            description TEXT,
                            jobType TEXT,
                            published INTEGER,
                            budget TEXT,
                            link TEXT,
                            categories TEXT
                        )
                    ''')
                    # If categories already exists, prefer it; otherwise copy url_source -> categories
                    if 'categories' in job_cols:
                        cur.execute(
                            "INSERT OR IGNORE INTO jobs_new (id,title,description,jobType,published,budget,link,categories) SELECT id,title,description,jobType,published,budget,link,categories FROM jobs"
                        )
                    else:
                        cur.execute(
                            "INSERT OR IGNORE INTO jobs_new (id,title,description,jobType,published,budget,link,categories) SELECT id,title,description,jobType,published,budget,link,url_source FROM jobs"
                        )
                    cur.execute('DROP TABLE jobs')
                    cur.execute('ALTER TABLE jobs_new RENAME TO jobs')

                # seen_jobs table migration
                cur.execute("PRAGMA table_info(seen_jobs)")
                seen_cols = [r[1] for r in cur.fetchall()]
                if 'url_source' in seen_cols:
                    cur.execute('''
                        CREATE TABLE IF NOT EXISTS seen_jobs_new (
                            job_id TEXT,
                            categories TEXT,
                            first_seen TIMESTAMP,
                            PRIMARY KEY (job_id, categories)
                        )
                    ''')
                    if 'categories' in seen_cols:
                        cur.execute(
                            "INSERT OR IGNORE INTO seen_jobs_new (job_id,categories,first_seen) SELECT job_id,categories,first_seen FROM seen_jobs"
                        )
                    else:
                        cur.execute(
                            "INSERT OR IGNORE INTO seen_jobs_new (job_id,categories,first_seen) SELECT job_id,url_source,first_seen FROM seen_jobs"
                        )
                    cur.execute('DROP TABLE seen_jobs')
                    cur.execute('ALTER TABLE seen_jobs_new RENAME TO seen_jobs')

            try:
                _drop_legacy_url_source()
            except Exception:
                # Migration failed; don't crash the app here
                pass


def insert_job(job):
    """Insert a job into the `jobs` table.

    Accepts a dict and prefers `categories`, falling back to legacy `url_source`.
    """
    categories = None
    if isinstance(job, dict):
        categories = job.get('categories') or job.get('url_source')
    with closing(sqlite3.connect(DB_PATH)) as conn:
        with conn:
            conn.execute('''
                INSERT OR IGNORE INTO jobs (id, title, description, jobType, published, budget, link, categories)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                job['id'],
                job.get('title'),
                job.get('description'),
                job.get('jobType'),
                job.get('published'),
                str(job.get('budget')),
                job.get('link'),
                categories,
            ))


def job_exists(job_id):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.cursor()
        cur.execute('SELECT 1 FROM jobs WHERE id = ?', (job_id,))
        return cur.fetchone() is not None


def is_new_job_for_source(job_id, categories):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.cursor()
        cur.execute('SELECT 1 FROM seen_jobs WHERE job_id = ? AND categories = ?', (job_id, categories))
        return cur.fetchone() is None


def mark_job_seen(job_id, categories):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        with conn:
            conn.execute('''
                INSERT OR IGNORE INTO seen_jobs (job_id, categories, first_seen)
                VALUES (?, ?, datetime('now'))
            ''', (job_id, categories))


def cleanup_old_jobs(days=30):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        with conn:
            conn.execute("""
                DELETE FROM seen_jobs
                WHERE first_seen < datetime('now', ?)
            """, (f'-{days} days',))


def get_all_jobs():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.cursor()
        cur.execute('SELECT id, title, description, jobType, published, budget, link, categories FROM jobs')
        return cur.fetchall()


def get_last_post_times():
    """Return a list of (categories_label, last_seen_timestamp) tuples."""
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.cursor()
        cur.execute('''
            SELECT j.categories, MAX(s.first_seen) as last_seen
            FROM seen_jobs s
            JOIN jobs j ON s.job_id = j.id
            GROUP BY j.categories
        ''')
        return cur.fetchall()


# Async wrappers: run blocking DB ops in a thread to avoid blocking the event loop
async def async_create_jobs_table():
    return await asyncio.to_thread(create_jobs_table)


async def async_insert_job(job):
    return await asyncio.to_thread(insert_job, job)


async def async_job_exists(job_id):
    return await asyncio.to_thread(job_exists, job_id)


async def async_is_new_job_for_source(job_id, categories):
    return await asyncio.to_thread(is_new_job_for_source, job_id, categories)


async def async_mark_job_seen(job_id, categories):
    return await asyncio.to_thread(mark_job_seen, job_id, categories)


async def async_cleanup_old_jobs(days=30):
    return await asyncio.to_thread(cleanup_old_jobs, days)


async def async_get_all_jobs():
    return await asyncio.to_thread(get_all_jobs)


async def async_get_last_post_times():
    return await asyncio.to_thread(get_last_post_times)
