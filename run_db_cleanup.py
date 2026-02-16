import sqlite3

conn = sqlite3.connect('jobs.db')
cur = conn.cursor()

# Check jobs table
cur.execute("PRAGMA table_info(jobs)")
job_cols = [r[1] for r in cur.fetchall()]
print('jobs columns:', job_cols)
if 'url_source' in job_cols:
    print('Jobs table still has url_source; rebuilding...')
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
    if 'categories' in job_cols:
        cur.execute("INSERT OR IGNORE INTO jobs_new (id,title,description,jobType,published,budget,link,categories) SELECT id,title,description,jobType,published,budget,link,categories FROM jobs")
    else:
        cur.execute("INSERT OR IGNORE INTO jobs_new (id,title,description,jobType,published,budget,link,categories) SELECT id,title,description,jobType,published,budget,link,url_source FROM jobs")
    cur.execute('DROP TABLE jobs')
    cur.execute('ALTER TABLE jobs_new RENAME TO jobs')
    conn.commit()
    print('Jobs table rebuilt.')
else:
    print('Jobs table has no url_source column.')

# Check seen_jobs table
cur.execute("PRAGMA table_info(seen_jobs)")
seen_cols = [r[1] for r in cur.fetchall()]
print('seen_jobs columns:', seen_cols)
if 'url_source' in seen_cols:
    print('seen_jobs still has url_source; rebuilding...')
    cur.execute('''
        CREATE TABLE IF NOT EXISTS seen_jobs_new (
            job_id TEXT,
            categories TEXT,
            first_seen TIMESTAMP,
            PRIMARY KEY (job_id, categories)
        )
    ''')
    if 'categories' in seen_cols:
        cur.execute("INSERT OR IGNORE INTO seen_jobs_new (job_id,categories,first_seen) SELECT job_id,categories,first_seen FROM seen_jobs")
    else:
        cur.execute("INSERT OR IGNORE INTO seen_jobs_new (job_id,categories,first_seen) SELECT job_id,url_source,first_seen FROM seen_jobs")
    cur.execute('DROP TABLE seen_jobs')
    cur.execute('ALTER TABLE seen_jobs_new RENAME TO seen_jobs')
    conn.commit()
    print('seen_jobs table rebuilt.')
else:
    print('seen_jobs table has no url_source column.')

conn.close()
