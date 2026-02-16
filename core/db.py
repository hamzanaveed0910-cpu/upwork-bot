# This file handles saving and loading jobs from the database
import sqlite3
import asyncio
from contextlib import closing

# The name of our database file
DB_PATH = 'jobs.db'

def create_jobs_table():
    # Make sure the jobs table exists in the database
    with closing(sqlite3.connect(DB_PATH)) as conn:
        with conn:
            conn.execute('''
                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    title TEXT,
                    description TEXT,
                    jobType TEXT,
                    published INTEGER,
                    budget TEXT,
                    link TEXT,
                    url_source TEXT
                )
            ''')
            # Ensure older databases get the new column
            cur = conn.execute("PRAGMA table_info(jobs)")
            cols = [r[1] for r in cur.fetchall()]
            if 'url_source' not in cols:
                try:
                    conn.execute('ALTER TABLE jobs ADD COLUMN url_source TEXT')
                except Exception:
                    # If alter fails, ignore and continue (schema may vary)
                    pass
            # Table to track which job was seen for which source URL
            conn.execute('''
                CREATE TABLE IF NOT EXISTS seen_jobs (
                    job_id TEXT,
                    url_source TEXT,
                    first_seen TIMESTAMP,
                    PRIMARY KEY (job_id, url_source)
                )
            ''')

def insert_job(job):
    # Add a new job to the database (if it's not already there)
    # Backwards-compatible: accept either a dict or allow url_source via job.get('url_source')
    url_source = job.get('url_source') if isinstance(job, dict) else None
    with closing(sqlite3.connect(DB_PATH)) as conn:
        with conn:
            conn.execute('''
                INSERT OR IGNORE INTO jobs (id, title, description, jobType, published, budget, link, url_source)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                job['id'],
                job.get('title'),
                job.get('description'),
                job.get('jobType'),
                job.get('published'),
                str(job.get('budget')),
                job.get('link'),
                url_source
            ))

def job_exists(job_id):
    # Check if a job is already in the database
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.cursor()
        cur.execute('SELECT 1 FROM jobs WHERE id = ?', (job_id,))
        return cur.fetchone() is not None

def is_new_job_for_source(job_id, url_source):
    # Check if a job_id has been seen for a specific URL source
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.cursor()
        cur.execute('SELECT 1 FROM seen_jobs WHERE job_id = ? AND url_source = ?', (job_id, url_source))
        return cur.fetchone() is None

def mark_job_seen(job_id, url_source):
    with closing(sqlite3.connect(DB_PATH)) as conn:
        with conn:
            conn.execute('''
                INSERT OR IGNORE INTO seen_jobs (job_id, url_source, first_seen)
                VALUES (?, ?, datetime('now'))
            ''', (job_id, url_source))

def cleanup_old_jobs(days=30):
    # Remove seen_jobs older than `days` days
    with closing(sqlite3.connect(DB_PATH)) as conn:
        with conn:
            conn.execute("""
                DELETE FROM seen_jobs
                WHERE first_seen < datetime('now', ?)
            """, (f'-{days} days',))

def get_all_jobs():
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.cursor()
        cur.execute('SELECT id, title, description, jobType, published, budget, link, url_source FROM jobs')
        return cur.fetchall()


def get_last_post_times():
    """Return a list of (url_source_label, last_seen_timestamp) tuples.

    This joins `seen_jobs` (which records first_seen per job+source)
    with `jobs` to map job_id -> jobs.url_source (the human label stored
    when the job was inserted). We return the most recent first_seen per
    label.
    """
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.cursor()
        cur.execute('''
            SELECT j.url_source, MAX(s.first_seen) as last_seen
            FROM seen_jobs s
            JOIN jobs j ON s.job_id = j.id
            GROUP BY j.url_source
        ''')
        return cur.fetchall()



# Async wrappers: run blocking DB ops in a thread to avoid blocking the event loop
async def async_create_jobs_table():
    return await asyncio.to_thread(create_jobs_table)

async def async_insert_job(job):
    return await asyncio.to_thread(insert_job, job)

async def async_job_exists(job_id):
    return await asyncio.to_thread(job_exists, job_id)

async def async_is_new_job_for_source(job_id, url_source):
    return await asyncio.to_thread(is_new_job_for_source, job_id, url_source)

async def async_mark_job_seen(job_id, url_source):
    return await asyncio.to_thread(mark_job_seen, job_id, url_source)

async def async_cleanup_old_jobs(days=30):
    return await asyncio.to_thread(cleanup_old_jobs, days)

async def async_get_all_jobs():
    return await asyncio.to_thread(get_all_jobs)


async def async_get_last_post_times():
    return await asyncio.to_thread(get_last_post_times)
