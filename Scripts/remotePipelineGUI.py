import tkinter as tk
from tkinter import filedialog, messagebox
import threading
from pathlib import Path
import psycopg2
from psycopg2 import sql
import csv
import re

def run_pipeline():
    try:
        remote_host = host_entry.get()
        remote_port = port_entry.get()
        remote_user = user_entry.get()
        remote_password = password_entry.get()
        remote_db_name = db_entry.get()
        log_dir = Path(log_dir_entry.get())
        csv_dir = Path(csv_dir_entry.get())
        csv_file = csv_dir / 'generatedPDW.csv'

        if not log_dir.exists():
            messagebox.showerror("Error", "Log directory does not exist.")
            return

        csv_file.parent.mkdir(parents=True, exist_ok=True)

        # Process log files into CSV
        with open(csv_file, 'w', newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(["MC_Bin", "Toa", "TimeEnd", "Mag", "PW", "Freq", "Fmin", "Fmax",
                             "Fmop", "Phase01", "CW", "PhaseSegs", "Thres", "FileSeek"])

            for log_file in log_dir.glob('*.log'):
                with open(log_file, 'r') as infile:
                    for line in infile:
                        line = line.strip()
                        if not line or line.startswith('MC_Bin'):
                            continue
                        row = re.split(r'\s+', line)
                        if len(row) == 14:
                            writer.writerow(row)

        conn = psycopg2.connect(
            dbname='postgres',
            user=remote_user,
            password=remote_password,
            host=remote_host,
            port=remote_port
        )
        conn.autocommit = True
        cur = conn.cursor()

        cur.execute(f"SELECT 1 FROM pg_database WHERE datname = %s", (remote_db_name,))
        if not cur.fetchone():
            cur.execute(sql.SQL("CREATE DATABASE {}".format(remote_db_name)))
        cur.close()
        conn.close()

        conn = psycopg2.connect(
            dbname=remote_db_name,
            user=remote_user,
            password=remote_password,
            host=remote_host,
            port=remote_port
        )
        cur = conn.cursor()

        create_table_query = '''
        CREATE TABLE IF NOT EXISTS "PDW" (
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
            FileSeek BIGINT PRIMARY KEY
        );
        '''
        cur.execute(create_table_query)

        insert_query = '''
            INSERT INTO "PDW" (MC_Bin, Toa, TimeEnd, Mag, PW, Freq, Fmin, Fmax, Fmop,
                               Phase01, CW, PhaseSegs, Thres, FileSeek)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (FileSeek) DO NOTHING;
        '''

        rows_inserted = 0
        duplicates = 0
        with open(csv_file, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    cur.execute(insert_query, (
                        int(row['MC_Bin']), float(row['Toa']), float(row['TimeEnd']), float(row['Mag']),
                        float(row['PW']), float(row['Freq']), float(row['Fmin']), float(row['Fmax']),
                        int(row['Fmop']), float(row['Phase01']), int(row['CW']), int(row['PhaseSegs']),
                        float(row['Thres']), int(row['FileSeek'])
                    ))
                    if cur.rowcount > 0:
                        rows_inserted += 1
                    else:
                        duplicates += 1
                except Exception as e:
                    print(f"⚠️ Row skipped due to error: {e}")

        conn.commit()
        cur.close()
        conn.close()

        messagebox.showinfo("✅ Complete", f"{rows_inserted} rows inserted.\n{duplicates} duplicates skipped.\nCSV saved to:\n{csv_file}")

    except Exception as e:
        messagebox.showerror("Pipeline Failed", str(e))

root = tk.Tk()
root.title("PDW Remote Database Pipeline")
root.configure(bg="#1e1e1e")
root.resizable(True, True)

# Dynamic resizing
for i in range(8):
    root.grid_rowconfigure(i, weight=1)
root.grid_columnconfigure(1, weight=1)

def style_label(master, text, row):
    tk.Label(master, text=text, bg="#1e1e1e", fg="#00ff99", font=("Helvetica", 10, "bold")).grid(row=row, column=0, sticky="w", padx=10, pady=5)

def on_enter(e):
    run_button['background'] = '#00ffcc'

def on_leave(e):
    run_button['background'] = '#00ff99'

style_label(root, "Remote Host (IPv4):", 0)
host_entry = tk.Entry(root)
host_entry.insert(0, 'IPv4 Address (cmd ipconfig)')
host_entry.grid(row=0, column=1, sticky='ew', padx=10)

style_label(root, "Port:", 1)
port_entry = tk.Entry(root)
port_entry.insert(0, '5432')
port_entry.grid(row=1, column=1, sticky='ew', padx=10)

style_label(root, "User:", 2)
user_entry = tk.Entry(root)
user_entry.insert(0, 'postgres')
user_entry.grid(row=2, column=1, sticky='ew', padx=10)

style_label(root, "Password:", 3)
password_entry = tk.Entry(root, show="*")
password_entry.insert(0, 'your-password-here')
password_entry.grid(row=3, column=1, sticky='ew', padx=10)

style_label(root, "Database Name:", 4)
db_entry = tk.Entry(root)
db_entry.insert(0, 'pdw_remote_database')
db_entry.grid(row=4, column=1, sticky='ew', padx=10)

style_label(root, "Log Directory:", 5)
log_dir_entry = tk.Entry(root)
log_dir_entry.grid(row=5, column=1, sticky='ew', padx=10)
tk.Button(root, text="Browse", command=lambda: log_dir_entry.insert(0, filedialog.askdirectory())).grid(row=5, column=2, padx=5)

style_label(root, "CSV Output Directory:", 6)
csv_dir_entry = tk.Entry(root)
csv_dir_entry.grid(row=6, column=1, sticky='ew', padx=10)
tk.Button(root, text="Browse", command=lambda: csv_dir_entry.insert(0, filedialog.askdirectory())).grid(row=6, column=2, padx=5)

# Run Button Centered
run_frame = tk.Frame(root, bg="#1e1e1e")
run_frame.grid(row=7, column=0, columnspan=3, sticky='ew')

run_button = tk.Button(run_frame, text="Run Pipeline", command=lambda: threading.Thread(target=run_pipeline).start(),
                       bg="#00ff99", fg="black", font=("Helvetica", 12, "bold"), width=20)
run_button.pack(anchor='center')

run_button.bind("<Enter>", on_enter)
run_button.bind("<Leave>", on_leave)

root.mainloop()
