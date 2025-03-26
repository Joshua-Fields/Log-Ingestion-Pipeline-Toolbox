# Delimiter Log Ingestion Toolbox

A modular Python-based toolbox for ingesting, transforming, and loading log file data into PostgreSQL databases. Whether you're working locally, remotely, with CSVs, or fully relational tables, this set of scripts has you covered.

---

## 🚀 Features

- Convert raw log files into CSV for analysis or migration.
- Ingest CSVs directly into PostgreSQL tables.
- Create and manage relational schemas between files and data.
- Support for remote PostgreSQL servers.
- Simple GUI interface for non-technical users.

---

## 📁 Script Overview

### `log_to_csv.py`
Converts raw log files into clean, delimited CSV files.
- Automatically splits values based on whitespace.
- Useful for previewing or preprocessing data.

### `pipelineAutomation.py`
Local ETL pipeline that:
- Converts log files to CSV.
- Creates a local PostgreSQL database and table.
- Loads CSV data into the table.

### `remoteConnectionPipelineAutomation.py`
Full pipeline for remote PostgreSQL instances.
- Connects to a remote DB.
- Creates the database and table if they don't exist.
- Uploads data via CSV with duplicate-handling logic.

### `remotePipelineGUI.py`
User-friendly GUI to:
- Input remote DB credentials.
- Select log and output paths.
- Run the ingestion pipeline visually.
- Fully responsive with hover and layout effects.

### `twoTablePipeline.py`
Relational model ingestion script that:
- Creates two tables: `FileRegistry` and `PDWData`.
- Ingests log files directly without CSV.
- Tracks which file each data row came from using foreign keys.

---

## 🛠 Requirements

- Python 3.7+
- PostgreSQL

### Python Libraries:

```bash
pip install psycopg2
```

If you're using the GUI:

```bash
pip install tk
```

---

## 🧠 Use Cases

- Engineering teams ingesting PDW or sensor logs.
- Data analysts processing raw batch logs.
- ETL pipeline demos for PostgreSQL integrations.

---

## 📌 Project Goals

- Provide a flexible toolkit for different ingestion methods.
- Support both CLI automation and GUI-based workflows.
- Allow easy relational tracking of logs-to-data via foreign keys.

---

## 📂 Folder Structure

```
delimiter-ingestion-toolbox/
├── log_to_csv.py
├── pipelineAutomation.py
├── remoteConnectionPipelineAutomation.py
├── remotePipelineGUI.py
├── twoTablePipeline.py
├── log/          # Input log files
├── csv/          # Generated CSVs (if using CSV-based methods)
```

---

## 📜 License

MIT License

---

## 👋 Author

Built by **Joshua** ⚙️  
Inspired by real-world ingestion needs and collaborative dev workflows.

---

## 🙋‍♂️ Want to Contribute?

Pull requests welcome! If you'd like to expand support for other formats, automate more parts of the pipeline, or extend to other databases — feel free to fork and improve.

---

## 🧪 Coming Soon?

- Progress bar for GUI
- Log schema auto-detection
- CLI flags and YAML config support
- Dockerized version of the pipeline

---

## 📫 Contact

Questions? Suggestions? Open an issue or reach out via GitHub!
