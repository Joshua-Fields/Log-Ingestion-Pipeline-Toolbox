import re
import csv
import psycopg2
from psycopg2 import sql

# === CONFIGURATION ===
input_file = 'log/PulseDescriptors.log'
output_csv = 'csv/PulseDescriptors.csv'
db_name = 'pdw_database'
db_user = 'postgres'   # Change to your Postgres username
db_password = '1234'
db_host = 'localhost'
db_port = '5432'

# === STEP 1: Convert log to CSV ===
with open(input_file, 'r') as infile, open(output_csv, 'w', newline='') as outfile:
    writer = csv.writer(outfile)
    for line in infile:
        line = line.strip()
        if not line:
            continue
        row = re.split(r'\s+', line)
        writer.writerow(row)
print(f"✅ Log converted to {output_csv}")

# === STEP 2: Connect to Postgres (default 'postgres' database) to create new DB ===
conn = psycopg2.connect(dbname='postgres', user=db_user, password=db_password, host=db_host, port=db_port)
conn.autocommit = True
cur = conn.cursor()

# Drop database if exists for clean run (Optional)
cur.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(db_name)))
cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(db_name)))
print(f"✅ Database '{db_name}' created")

cur.close()
conn.close()

# === STEP 3: Connect to the new database and create the PDW table ===
conn = psycopg2.connect(dbname=db_name, user=db_user, password=db_password, host=db_host, port=db_port)
cur = conn.cursor()

create_table_query = '''
CREATE TABLE "PDW" (
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
print("✅ Table 'PDW' created")

# === STEP 4: Import CSV into the PDW table ===
with open(output_csv, 'r') as f:
    next(f)  # Skip header if needed, otherwise comment this line
    cur.copy_expert('''
        COPY "PDW" (MC_Bin, Toa, TimeEnd, Mag, PW, Freq, Fmin, Fmax, Fmop, Phase01, CW, PhaseSegs, Thres, FileSeek)
        FROM STDIN WITH CSV
    ''', f)

print("✅ Data imported into 'PDW' table")

# === Finalize ===
conn.commit()
cur.close()
conn.close()
print("🎉 All done!")
