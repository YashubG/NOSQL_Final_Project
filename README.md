# NASA HTTP Log Analysis — DAS 839 NoSQL Systems End-Term Project

A unified **Java CLI tool** for comparative log analytics across **4 NoSQL/big-data execution pipelines**: MongoDB, MapReduce, Pig, and Hive. All pipelines process NASA HTTP access logs (July & August 1995, ~3.46M records) and produce mathematically identical results.

---

## Table of Contents

1. [Project Overview](#1-project-overview)
2. [Quick Start (Teammates Start Here)](#2-quick-start-teammates-start-here)
3. [System Architecture](#3-system-architecture)
4. [Repository Structure](#4-repository-structure)
5. [Prerequisites](#5-prerequisites)
6. [Setup & Installation](#6-setup--installation)
7. [Running the Project](#7-running-the-project)
8. [Pipelines](#8-pipelines)
9. [Mandatory Queries](#9-mandatory-queries)
10. [Parsing Strategy](#10-parsing-strategy)
11. [Batching Strategy](#11-batching-strategy)
12. [Database Schema](#12-database-schema)
13. [Correctness Check](#13-correctness-check)
14. [Troubleshooting](#14-troubleshooting)
15. [Phase History](#15-phase-history)

---

## 1. Project Overview

This project implements a **comparative systems prototype** (as specified in the DAS 839 End-Term guidelines) where the user selects one of four execution pipelines via a single CLI interface. Each pipeline performs the complete **ETL flow** — parse, clean, batch, aggregate, load, and report — using its native execution technology while sharing the same parsing rules, cleaning logic, and output schema.

**Key Features:**
- 🔄 **4 Pipelines**: MongoDB, MapReduce, Pig, Hive — switchable at runtime
- 📊 **3 Mandatory Queries**: Daily Traffic, Top Resources, Hourly Errors
- 📦 **3 Batching Strategies**: Fixed-size, Monthly, Weekly
- ✅ **Built-in Correctness Checker**: Pairwise comparison across all pipelines
- 🗄️ **MySQL Reporting**: Structured result tables with full run/batch metadata
- 🔢 **Malformed Record Tracking**: Per-batch and per-run counters (never silently dropped)

---

## 2. Quick Start (Teammates Start Here)

If MongoDB, MySQL, Hadoop, Pig, and Hive are already installed, this gets you from zero → results in about 5 minutes.

```bash
# 1. Clone & enter the repo
git clone https://github.com/InvincibleAgam/NOSQL_PROJECT.git
cd NOSQL_PROJECT

# 2. Make sure services are up
brew services start mysql           # port 3307
brew services start mongodb-community   # port 27017

# 3. Download NASA logs (~37 MB compressed, ~370 MB on disk)
bash scripts/download_data.sh

# 4. One-time only — initialize the Hive Derby metastore
schematool -dbType derby -initSchema

# 5. Build the uber JAR
mvn clean package

# 6. Run the CLI
java -cp target/nosql-project-1.0-SNAPSHOT-jar-with-dependencies.jar com.invincibleagam.Main
```

When the menu appears, pick **`5`** (run all 4 pipelines + correctness check), accept the defaults (press Enter for strategy and batch size), then type **`a`** to run all queries.

> **First time on macOS arm64? Read [§14 Troubleshooting](#14-troubleshooting) before you panic.** Hive 4.2 on Apple Silicon has known sharp edges; the fixes are already in the code but require the `schematool` step above.

---

## 3. System Architecture

```
┌─────────────────────────────────────────────────────┐
│              Java CLI Controller                     │
│         (Main.java — pipeline selector)              │
│  Select: 1.MongoDB  2.MR  3.Pig  4.Hive  5.All+Check│
└──┬─────────┬─────────┬─────────┬────────────────────┘
   │         │         │         │
┌──▼───┐  ┌──▼───┐  ┌──▼───┐  ┌──▼───┐
│Mongo │  │  MR  │  │ Pig  │  │ Hive │
│Pipe  │  │ Pipe │  │ Pipe │  │ Pipe │
│.java │  │.java │  │.java │  │.java │
└──┬───┘  └──┬───┘  └──┬───┘  └──┬───┘
   │         │         │         │
   │    ┌────┴─────────┴─────────┘
   │    │  Staged TSV (shared format)
   │    │  host\ttimestamp\tlog_date\t...
   │    │
   │    ├──→ NASALogDriver.java (Hadoop Mapper/Reducer)
   │    ├──→ pig_q1.pig / pig_q2.pig / pig_q3.pig
   │    └──→ hive_setup.hql + hive_q1/q2/q3.hql
   │
   └────────────┬────────────────────
                │
   ┌────────────▼────────────────────┐
   │   Shared Core Infrastructure     │
   │                                   │
   │  LogParser.java      — regex      │
   │  LogicalBatchProcessor.java       │
   │    (fixed / monthly / weekly)     │
   │  ParsedLog.java      — POJO      │
   │  Config.java          — settings  │
   │  DatabaseManager.java — MySQL     │
   │    (load + report + correctness)  │
   └───────────────────────────────────┘
```

---

## 4. Repository Structure

```
NOSQL_PROJECT/
├── pom.xml                          # Maven build (Java 17, Hadoop 3.3.6, MongoDB, MySQL)
├── README.md
├── RUN_COMMANDS.txt                 # Minimal cheat sheet (also in §2 above)
├── hive-site.xml                    # Hive config: local warehouse dir
├── config.py                        # Legacy Python config (Phase 1 reference)
├── sql/
│   └── schema.sql                   # Full MySQL schema (5 tables)
├── scripts/
│   ├── download_data.sh             # Downloads NASA HTTP logs (~37MB compressed)
│   ├── run_all.sh                   # Automated end-to-end run (Mongo + MR only)
│   ├── pig/
│   │   ├── pig_q1.pig               # Pig Latin — Daily Traffic Summary
│   │   ├── pig_q2.pig               # Pig Latin — Top 20 Resources
│   │   └── pig_q3.pig               # Pig Latin — Hourly Error Analysis
│   └── hive/
│       ├── hive_setup.hql           # Create external table over staged TSV
│       ├── hive_q1.hql              # HiveQL — Daily Traffic Summary
│       ├── hive_q2.hql              # HiveQL — Top 20 Resources
│       └── hive_q3.hql              # HiveQL — Hourly Error Analysis
├── src/main/java/com/invincibleagam/
│   ├── Main.java                    # CLI controller (pipeline selector)
│   ├── core/
│   │   ├── Config.java              # DB hosts, ports, file paths
│   │   ├── LogParser.java           # Shared regex parser
│   │   ├── BatchProcessor.java      # Original fixed-size batch iterator
│   │   ├── BatchStrategy.java       # Enum: FIXED / MONTHLY / WEEKLY
│   │   ├── LogicalBatchProcessor.java # Month/week/fixed batch iterator
│   │   └── DatabaseManager.java     # MySQL DDL, loading, reporting, correctness check
│   ├── models/
│   │   └── ParsedLog.java           # Parsed log line POJO
│   └── pipelines/
│       ├── MongoPipeline.java       # MongoDB aggregation pipeline
│       ├── MapReducePipeline.java   # Hadoop MapReduce orchestrator
│       ├── NASALogDriver.java       # Hadoop Mapper/Reducer classes (Q1, Q2, Q3)
│       ├── PigPipeline.java         # Apache Pig orchestrator
│       └── HivePipeline.java        # Apache Hive orchestrator (uses beeline + embedded HS2)
└── data/
    └── raw/                         # NASA log files (downloaded by script)
```

---

## 5. Prerequisites

| Software      | Version  | Purpose            | Install (macOS)                   |
|---------------|----------|--------------------|-----------------------------------|
| Java JDK      | 17+      | Build & run        | `brew install openjdk@17`         |
| Maven         | 3.9+     | Build              | `brew install maven`              |
| MySQL         | 8.x      | Result storage     | `brew install mysql`              |
| MongoDB       | 7.x      | MongoDB pipeline   | `brew install mongodb-community`  |
| Hadoop        | 3.3+     | MapReduce pipeline | `brew install hadoop`             |
| Apache Pig    | 0.17+    | Pig pipeline       | `brew install pig`                |
| Apache Hive   | **4.2+** | Hive pipeline      | `brew install hive`               |

> **Defaults** (see [`Config.java`](src/main/java/com/invincibleagam/core/Config.java)): MySQL on **3307** as `root` with no password, MongoDB on **27017**. Override by editing `Config.java`.

> **Apple Silicon note:** Hive 4.x on macOS arm64 has two installation-level quirks (legacy `hive` CLI removed → use beeline; bundled JLine native lib is x86_64-only). Both are handled by `HivePipeline.java`; you just need the one-time `schematool -dbType derby -initSchema`. See [§14](#14-troubleshooting) for details.

---

## 6. Setup & Installation

```bash
# 1. Clone the repository
git clone https://github.com/InvincibleAgam/NOSQL_PROJECT.git
cd NOSQL_PROJECT

# 2. Download NASA HTTP access logs (~37MB compressed → ~370MB decompressed)
bash scripts/download_data.sh

# 3. Start MySQL (port 3307, root, no password)
#    The tool auto-creates the database and tables on first run.
brew services start mysql

# 4. Start MongoDB (port 27017) — only needed for the MongoDB pipeline
brew services start mongodb-community

# 5. ONE-TIME ONLY: initialize the Hive Derby metastore
#    Required for Hive 4.x; without this you'll get "Version information not found in metastore"
schematool -dbType derby -initSchema

# 6. Build the Java project (creates uber JAR with all dependencies)
mvn clean package

# 7. Verify the build
ls -lh target/nosql-project-1.0-SNAPSHOT-jar-with-dependencies.jar
```

---

## 7. Running the Project

### Interactive CLI (recommended)

```bash
java -cp target/nosql-project-1.0-SNAPSHOT-jar-with-dependencies.jar com.invincibleagam.Main
```

This launches the interactive menu:

```
================================================================================
  NASA HTTP Log Analysis Tool — NoSQL Systems Project (Java)
================================================================================

  ✓  Found: data/raw/NASA_access_log_Jul95
  ✓  Found: data/raw/NASA_access_log_Aug95

Select a pipeline:
  1. MongoDB Aggregation Pipeline
  2. Hadoop MapReduce Pipeline
  3. Apache Pig Pipeline
  4. Apache Hive Pipeline
  5. Run ALL 4 Pipelines + Correctness Check
  6. View Report for a Run

Choice [1-6]:
```

After selecting a pipeline, you'll be prompted for:
1. **Batch Strategy** — Fixed (default) / Monthly / Weekly
2. **Batch Size** — Number of records per batch (default: 10,000; only for Fixed strategy)
3. **Query Selection** — All, individual (Q1/Q2/Q3), or custom combination

### Run a single pipeline (example: MongoDB)

```bash
java -cp target/nosql-project-1.0-SNAPSHOT-jar-with-dependencies.jar com.invincibleagam.Main <<EOF
1
1
10000
a
EOF
```

### Run all 4 pipelines + correctness check

```bash
java -cp target/nosql-project-1.0-SNAPSHOT-jar-with-dependencies.jar com.invincibleagam.Main <<EOF
5
1

a
EOF
```

### Automated end-to-end run (MongoDB + MapReduce only)

```bash
bash scripts/run_all.sh
```

> `run_all.sh` does **not** run Pig or Hive. Use the interactive menu for the full 4-way comparison.

---

## 8. Pipelines

### 8.1 MongoDB Pipeline (`MongoPipeline.java`)
- **Technology**: MongoDB Aggregation Framework (Java Driver Sync 5.0)
- **Flow**: Parse → Insert documents to MongoDB → Run `$group`, `$sort`, `$limit` aggregations → Load results to MySQL
- **Execution**: In-process via Java MongoDB driver

### 8.2 MapReduce Pipeline (`MapReducePipeline.java` + `NASALogDriver.java`)
- **Technology**: Native Hadoop MapReduce API
- **Flow**: Parse → Stage as TSV → Execute `hadoop jar` with Mapper/Reducer classes → Read output → Load to MySQL
- **Execution**: `hadoop jar target/nosql-project-1.0-SNAPSHOT-jar-with-dependencies.jar`

### 8.3 Pig Pipeline (`PigPipeline.java` + `scripts/pig/*.pig`)
- **Technology**: Apache Pig Latin (local mode)
- **Flow**: Parse → Stage as TSV → Execute `pig -x local -f` for each query script → Read output → Load to MySQL
- **Scripts**: `pig_q1.pig` (GROUP BY), `pig_q2.pig` (nested DISTINCT), `pig_q3.pig` (nested FILTER)

### 8.4 Hive Pipeline (`HivePipeline.java` + `scripts/hive/*.hql`)
- **Technology**: HiveQL via **`beeline`** against an **embedded HiveServer2** (`jdbc:hive2://`) backed by Derby metastore
- **Flow**: Parse → Stage as TSV → Create external table (`hive_setup.hql`) → Run each query (`hive_q*.hql`) writing `INSERT OVERWRITE LOCAL DIRECTORY` output → Read output → Load to MySQL
- **Scripts**: `hive_q1.hql` (GROUP BY), `hive_q2.hql` (subquery + LIMIT), `hive_q3.hql` (CASE WHEN)
- **macOS arm64 fix**: `HADOOP_CLIENT_OPTS` includes `-Dorg.jline.terminal.provider=dumb` to bypass beeline's JLine JNA crash. See [§14](#14-troubleshooting).

### Shared Infrastructure

All 4 pipelines share:
- **`LogParser.java`**: Rigorous regex pattern — `^(\S+)\s+\S+\s+\S+\s+\[([^\]]+)\]\s+"([^"]*)"\s+(\d{3})\s+(\S+)$`
- **`LogicalBatchProcessor.java`**: Streams log files with GZIP support; groups by fixed-size, calendar month, or ISO week
- **`DatabaseManager.java`**: MySQL DDL creation, result loading, reporting, and pairwise correctness checking

---

## 9. Mandatory Queries

| Query   | Description                | Output Schema                                                                                   |
|---------|----------------------------|-------------------------------------------------------------------------------------------------|
| **Q1**  | Daily Traffic Summary      | `log_date, status_code, request_count, total_bytes`                                             |
| **Q2**  | Top 20 Requested Resources | `resource_path, request_count, total_bytes, distinct_host_count`                                |
| **Q3**  | Hourly Error Analysis      | `log_date, log_hour, error_request_count, total_request_count, error_rate, distinct_error_hosts`|

Error status codes: **400–599** (inclusive).

---

## 10. Parsing Strategy

The shared `LogParser.java` uses a strict regex to extract:
`host`, `timestamp`, `log_date`, `log_hour`, `http_method`, `resource_path`, `protocol_version`, `status_code`, `bytes_transferred`

- **Bytes field**: `-` is treated as `0`
- **Malformed records**: If a line fails regex or timestamp parsing, `LogParser.parse()` returns `null`. Pipelines **never silently drop** these records — they increment a per-batch and per-run `malformed_count` that is stored in both `run_metadata` and `batch_metadata` tables.
- **Observed**: ~33 malformed records out of 3,461,613 total lines.

---

## 11. Batching Strategy

| Strategy    | Batch Key                       | Description                                          |
|-------------|---------------------------------|------------------------------------------------------|
| **Fixed**   | `batch_1`, `batch_2`, ...       | N records per batch (default 10,000)                 |
| **Monthly** | `1995-07`, `1995-08`            | All records from one calendar month                  |
| **Weekly**  | `1995-W27`, `1995-W28`, ...     | All records from one ISO week                        |

- **Average batch size** = `total_records / num_batches`
- **Batch IDs** start from 1 and increase sequentially
- Per-batch metadata (key, strategy, valid/malformed counts, timestamps) is stored in `batch_metadata`

---

## 12. Database Schema

MySQL database: `nasa_log_analysis` (auto-created on first run)

| Table             | Purpose                                                              |
|-------------------|----------------------------------------------------------------------|
| `query1_results`  | Daily Traffic Summary results per pipeline run                       |
| `query2_results`  | Top 20 Resources results per pipeline run                            |
| `query3_results`  | Hourly Error Analysis results per pipeline run                       |
| `run_metadata`    | Per-run summary (pipeline, batch strategy, runtime, malformed count) |
| `batch_metadata`  | Per-batch details (batch key, record counts, timestamps)             |

All result tables include: `pipeline_name`, `run_id`, `batch_id`, `execution_time`.

Full DDL: [`sql/schema.sql`](sql/schema.sql)

---

## 13. Correctness Check

**Option 5** in the CLI runs all 4 pipelines sequentially, then performs **6 pairwise comparisons** (MongoDB↔MR, MongoDB↔Pig, MongoDB↔Hive, MR↔Pig, MR↔Hive, Pig↔Hive).

For each pair, it compares:
- **Q1**: Aggregated `(log_date, status_code)` → `request_count`, `total_bytes`
- **Q2**: Top 20 `resource_path` → `request_count`, `total_bytes`, `distinct_host_count`
- **Q3**: Aggregated `(log_date, log_hour)` → `error_request_count`, `total_request_count`

Result: `✅ ALL QUERIES MATCH` or `❌ MISMATCH DETECTED` with diff details.

---

## 14. Troubleshooting

Most issues here are Hive-specific on macOS arm64 (Apple Silicon) with Hive 4.2. The pipeline already includes the workarounds; this section explains the *why* in case something drifts.

### 14.1 `Version information not found in metastore`

**Cause:** Hive 4 no longer auto-initializes the Derby metastore schema.

**Fix (run once):**
```bash
schematool -dbType derby -initSchema
```

### 14.2 `Another instance of Derby may have already booted the database`

**Cause:** A previous Hive run was killed before releasing the embedded-Derby exclusive lock at `metastore_db/db.lck`.

**Fix (if no Hive process is actually running):**
```bash
rm -rf metastore_db derby.log
schematool -dbType derby -initSchema
```
Verify nothing's holding it first: `lsof metastore_db/db.lck` should be empty.

### 14.3 `Unable to create a system terminal` / JNA `UnsatisfiedLinkError` on arm64

**Cause:** Hive 4.2's bundled JLine JNA native library ships only x86_64; it crashes during terminal init on Apple Silicon.

**Fix:** Already applied in [`HivePipeline.java`](src/main/java/com/invincibleagam/pipelines/HivePipeline.java) — `HADOOP_CLIENT_OPTS` includes `-Dorg.jline.terminal.provider=dumb`. If you ever call beeline by hand on arm64, set this yourself:
```bash
export HADOOP_CLIENT_OPTS="-Dorg.jline.terminal.provider=dumb"
beeline -u "jdbc:hive2://" -e "show databases;"
```

### 14.4 `No current connection` from beeline / `hive -e` returns nothing

**Cause:** Hive 4 removed the embedded `hive` CLI; `hive` now wraps `beeline`, which needs a JDBC URL.

**Fix:** Use embedded HiveServer2 mode (already done by the pipeline):
```bash
beeline -u "jdbc:hive2://" -f scripts/hive/hive_setup.hql
```

### 14.5 MapReduce hangs at "Running job" forever

**Cause:** Hadoop is configured for YARN but YARN isn't running, or vice versa.

**Fix:** The pipeline runs in **local mode**. If you see this, check `$HADOOP_HOME/etc/hadoop/mapred-site.xml` and confirm `mapreduce.framework.name=local`, or just remove that property.

### 14.6 MySQL `Access denied for user 'root'@'localhost'`

**Cause:** Your local MySQL has a root password set, but `Config.java` defaults to no password.

**Fix:** Either set your MySQL root password to empty for this project, or update [`Config.java`](src/main/java/com/invincibleagam/core/Config.java):
```java
public static final String MYSQL_PASSWORD = "your-password";
```
Then `mvn clean package` again.

---

## 15. Phase History

### Phase 1 (Completed)
- ✅ System architecture designed
- ✅ Shared parser (`LogParser.java`) with regex + malformed record tracking
- ✅ Fixed-size batching via `BatchProcessor.java`
- ✅ MongoDB pipeline with aggregation framework
- ✅ MapReduce pipeline with native Hadoop Mapper/Reducer
- ✅ MySQL reporting schema (4 tables)
- ✅ CLI controller with pipeline selection
- ✅ Report generation from MySQL

### Phase 2 (Completed)
- ✅ Apache Pig pipeline with 3 Pig Latin scripts
- ✅ Apache Hive pipeline with HiveQL scripts + external table setup
- ✅ Logical batching (Monthly / Weekly) via `LogicalBatchProcessor.java`
- ✅ `batch_metadata` table for per-batch tracking
- ✅ Per-batch malformed record counters
- ✅ Query-wise selective execution (run only Q1, Q2, Q3, or any combination)
- ✅ 4-way pairwise correctness checker
- ✅ Updated CLI with all 4 pipelines + "Run All + Check" option

### Stabilization (Completed)
- ✅ PigPipeline classpath fix for `commons-collections`
- ✅ Hive shutdown-hook timeout tuning
- ✅ `hive-site.xml` local warehouse directory
- ✅ Hive 4.2 / macOS arm64 compatibility — `beeline` + embedded HS2 + JLine dumb-terminal workaround
- ✅ Documented one-time `schematool -dbType derby -initSchema` step
