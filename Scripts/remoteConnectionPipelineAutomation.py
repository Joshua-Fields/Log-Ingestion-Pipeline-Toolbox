import re
import csv
import psycopg2
from psycopg2 import sql
from pathlib import Path

# === CONFIGURATION ===
BASE_DIR = Path(__file__).resolve().parent
LOG_DIR = BASE_DIR / 'log'
CSV_FILE = BASE_DIR / 'csv' / 'PulseDescriptors.csv'
CSV_FILE.parent.mkdir(parents=True, exist_ok=True)

REMOTE_DB_HOST = 'Your IPv4 Address'
REMOTE_DB_PORT = '5432'
REMOTE_DB_USER = 'postgres'
REMOTE_DB_PASSWORD = '1234'
REMOTE_DB_NAME = 'pdw_remote_database'

# === STEP 1: Convert ALL logs to CSV ===
header_written = False
with open(CSV_FILE, 'w', newline='') as outfile:
    writer = csv.writer(outfile)
    writer.writerow(["MC_Bin", "Toa", "TimeEnd", "Mag", "PW", "Freq", "Fmin", "Fmax",
                     "Fmop", "Phase01", "CW", "PhaseSegs", "Thres", "FileSeek"])

    #Goes through each log file in the log folder
    for log_file in LOG_DIR.glob('*.log'):
        print(f"Processing {log_file}")
        with open(log_file, 'r') as infile:
            for line in infile:
                line = line.strip()
                if not line or line.startswith('MC_Bin'):  # Skip header
                    continue
                row = re.split(r'\s+', line)
                if len(row) == 14:
                    writer.writerow(row)



print(f"✅ All logs processed into {CSV_FILE}")

# === STEP 2: Connect to PostgreSQL maintenance DB ===
conn = psycopg2.connect(
    dbname='postgres',
    user=REMOTE_DB_USER,
    password=REMOTE_DB_PASSWORD,
    host=REMOTE_DB_HOST,
    port=REMOTE_DB_PORT
)
conn.autocommit = True
cur = conn.cursor()

# === STEP 3: Check/Create Database ===
cur.execute(f"SELECT 1 FROM pg_database WHERE datname = '{REMOTE_DB_NAME}'")
if not cur.fetchone():
    cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(REMOTE_DB_NAME)))
    print(f"✅ Database '{REMOTE_DB_NAME}' created")
else:
    print(f"⚠️ Database '{REMOTE_DB_NAME}' exists, skipping creation")
cur.close()
conn.close()

# === STEP 4: Connect to the target DB and create the PDW table if needed ===
conn = psycopg2.connect(
    dbname=REMOTE_DB_NAME,
    user=REMOTE_DB_USER,
    password=REMOTE_DB_PASSWORD,
    host=REMOTE_DB_HOST,
    port=REMOTE_DB_PORT
)
cur = conn.cursor()

create_table_query = '''
CREATE TABLE IF NOT EXISTS "PDW" (
    MC_Bin     SMALLINT,
    Toa        DOUBLE PRECISION,
    TimeEnd    DOUBLE PRECISION,
    Mag        DOUBLE PRECISION,
    PW         DOUBLE PRECISION,
    Freq       DOUBLE PRECISION,
    Fmin       DOUBLE PRECISION,
    Fmax       DOUBLE PRECISION,
    Fmop       SMALLINT,
    Phase01    DOUBLE PRECISION,
    CW         SMALLINT,
    PhaseSegs  SMALLINT,
    Thres      DOUBLE PRECISION,
    FileSeek   BIGINT PRIMARY KEY
);
'''
cur.execute(create_table_query)
print("✅ Table 'PDW' checked/created")

# === STEP 5: Row-by-row INSERT with conflict skip ===
insert_query = '''
    INSERT INTO "PDW" (MC_Bin, Toa, TimeEnd, Mag, PW, Freq, Fmin, Fmax, Fmop,
                       Phase01, CW, PhaseSegs, Thres, FileSeek)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (FileSeek) DO NOTHING;
'''

rows_inserted = 0
duplicate_rows = 0
with open(CSV_FILE, 'r') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        try:
            cur.execute(insert_query, (
                int(row['MC_Bin']),
                float(row['Toa']),
                float(row['TimeEnd']),
                float(row['Mag']),
                float(row['PW']),
                float(row['Freq']),
                float(row['Fmin']),
                float(row['Fmax']),
                int(row['Fmop']),
                float(row['Phase01']),
                int(row['CW']),
                int(row['PhaseSegs']),
                float(row['Thres']),
                int(row['FileSeek'])
            ))
            if cur.rowcount > 0:
                rows_inserted += 1
            else:
                duplicate_rows += 1
        except Exception as e:
            print(f"⚠️ Skipping problematic row due to error: {e}")


conn.commit()
print(f"✅ {rows_inserted} new rows inserted into 'PDW'")
print(f"⚠️ {duplicate_rows} duplicate rows skipped based on FileSeek")


cur.close()
conn.close()
print("🎉 Pipeline complete and database updated!")
