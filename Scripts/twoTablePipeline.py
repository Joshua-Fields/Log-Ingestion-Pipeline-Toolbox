import psycopg2
from psycopg2 import sql
from pathlib import Path
import re

# === CONFIGURATION ===
REMOTE_DB_HOST = '10.3.58.57'
REMOTE_DB_PORT = '5432'
REMOTE_DB_USER = 'postgres'
REMOTE_DB_PASSWORD = '1234'
REMOTE_DB_NAME = 'pdw_remote_database'
LOG_DIR = Path('D:/Repos/delimiter/Scripts/log')  # Update this to your log directory

# === DATABASE SETUP ===
conn = psycopg2.connect(
    dbname='postgres',
    user=REMOTE_DB_USER,
    password=REMOTE_DB_PASSWORD,
    host=REMOTE_DB_HOST,
    port=REMOTE_DB_PORT
)
conn.autocommit = True
cur = conn.cursor()

# Create the database if it doesn't exist
cur.execute(f"SELECT 1 FROM pg_database WHERE datname = %s", (REMOTE_DB_NAME,))
if not cur.fetchone():
    cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(REMOTE_DB_NAME)))
cur.close()
conn.close()

# === Connect to the Target Database ===
conn = psycopg2.connect(
    dbname=REMOTE_DB_NAME,
    user=REMOTE_DB_USER,
    password=REMOTE_DB_PASSWORD,
    host=REMOTE_DB_HOST,
    port=REMOTE_DB_PORT
)
cur = conn.cursor()

# === Create the Tables ===
cur.execute('''
    CREATE TABLE IF NOT EXISTS "FileRegistry" (
        file_id SERIAL PRIMARY KEY,
        file_name TEXT UNIQUE,
        ingestion_time TIMESTAMP DEFAULT NOW()
    );
''')

cur.execute('''
    CREATE TABLE IF NOT EXISTS "PDWData" (
        file_id INT REFERENCES "FileRegistry"(file_id),
        MC_Bin SMALLINT,
        Toa DOUBLE PRECISION,
        TimeEnd DOUBLE PRECISION,
        Mag DOUBLE PRECISION,
        PW DOUBLE PRECISION,
        Freq DOUBLE PRECISION,
        Fmin DOUBLE PRECISION,
        Fmax DOUBLE PRECISION,
        Fmop SMALLINT,
        Phase01 DOUBLE PRECISION,
        CW SMALLINT,
        PhaseSegs SMALLINT,
        Thres DOUBLE PRECISION,
        FileSeek BIGINT
    );
''')
conn.commit()

# === Process Logs and Insert ===
for log_file in LOG_DIR.glob('*.log'):
    file_name = log_file.name
    print(f"Processing {file_name}")

    # Register the file (skip if already ingested)
    cur.execute('SELECT file_id FROM "FileRegistry" WHERE file_name = %s', (file_name,))
    file_entry = cur.fetchone()

    if file_entry:
        print(f"⚠️ {file_name} already ingested. Skipping.")
        continue

    # Insert file and fetch the generated file_id
    cur.execute('INSERT INTO "FileRegistry" (file_name) VALUES (%s) RETURNING file_id', (file_name,))
    file_id = cur.fetchone()[0]

    with open(log_file, 'r') as infile:
        for line in infile:
            line = line.strip()
            if not line or line.startswith('MC_Bin'):
                continue
            row = re.split(r'\s+', line)
            if len(row) == 14:
                cur.execute('''
                    INSERT INTO "PDWData" (
                        file_id, MC_Bin, Toa, TimeEnd, Mag, PW, Freq, Fmin, Fmax,
                        Fmop, Phase01, CW, PhaseSegs, Thres, FileSeek
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (file_id, *row))

    conn.commit()
    print(f"✅ {file_name} ingested successfully with file_id {file_id}.")

cur.close()
conn.close()
print("🎉 All logs processed and database updated.")
