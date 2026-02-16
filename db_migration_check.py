import sqlite3

conn = sqlite3.connect('jobs.db')
cur = conn.cursor()

for t in ('jobs','seen_jobs'):
    try:
        cur.execute(f"PRAGMA table_info({t})")
        cols=[r[1] for r in cur.fetchall()]
        print(t, 'columns->', cols)
    except Exception as e:
        print('Error reading', t, e)

# show a sample job row
cur.execute("SELECT id, title, categories FROM jobs ORDER BY rowid DESC LIMIT 5")
for r in cur.fetchall():
    print('JOB:', r)

# show sample seen_jobs
cur.execute("SELECT job_id, categories, first_seen FROM seen_jobs ORDER BY first_seen DESC LIMIT 5")
for r in cur.fetchall():
    print('SEEN:', r)

conn.close()
