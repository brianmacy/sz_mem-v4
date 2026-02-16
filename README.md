# sz_mem-v4 — Senzing V4 Entity Resolution with In-Memory SQLite

A complete, working example of how to perform [Senzing](https://senzing.com) entity resolution (ER) using a **SQLite in-memory database** with the Senzing V4 Python SDK. This approach eliminates the need for a persistent database server during development, testing, or proof-of-concept work — making it the fastest way to get started with Senzing ER.

## What This Example Demonstrates

- **In-memory SQLite setup** — Create and initialize a Senzing datastore entirely in RAM
- **Automatic data source registration** — Scan input data and register all `DATA_SOURCE` values before loading
- **Multithreaded record ingestion** — Load records concurrently using Python's `ThreadPoolExecutor`
- **Redo record processing** — Process deferred entity re-evaluations after the initial load
- **Database backup** — Persist the in-memory results to a `backup.db` file on disk
- **Error classification** — Handle bad input, retryable, and unrecoverable errors differently
- **Progress monitoring** — Track throughput (records/second) and engine statistics during loading

## How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                        Execution Flow                           │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Parse SENZING_ENGINE_CONFIGURATION_JSON                     │
│  2. Create in-memory SQLite DB (shared cache)                   │
│  3. Initialize DB with Senzing schema                           │
│  4. Scan input file → register all DATA_SOURCEs                 │
│  5. Persist Senzing config with registered data sources         │
│  6. Multithreaded record ingestion (futures_add)                │
│  7. Multithreaded redo processing (futures_redo)                │
│  8. Backup in-memory DB → backup.db on disk                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### Why In-Memory SQLite?

| Benefit                   | Description                                                            |
| ------------------------- | ---------------------------------------------------------------------- |
| **Speed**                 | No disk I/O during entity resolution — everything runs in RAM          |
| **Zero infrastructure**   | No database server to install, configure, or maintain                  |
| **Portable**              | Results can be backed up to a standard SQLite file for later analysis  |
| **Ideal for PoC/testing** | Quickly evaluate Senzing ER against your data without production setup |

### What Are Redo Records?

During record ingestion, Senzing may identify entity relationships that need re-evaluation. For example, when adding record C reveals that records A and B (previously considered separate entities) should now be merged. Rather than blocking ingestion to resolve this immediately, Senzing creates a **redo record** — a deferred work item. Processing redo records after the initial load ensures all entity relationships are fully resolved.

## Prerequisites

### 1. Install Senzing V4

Follow the [Senzing V4 Linux Quickstart Guide](https://www.senzing.com/docs/quickstart/quickstart_api) to install the Senzing SDK on your system.

**Debian/Ubuntu:**

```bash
# Add the Senzing repository and install
sudo apt update
sudo apt install senzingsdk-poc
```

**RHEL/Amazon Linux:**

```bash
sudo yum install senzingsdk-poc
```

### 2. Create a Senzing Project

```bash
/opt/senzing/er/bin/sz_create_project ~/senzing_project
```

This creates an isolated Senzing instance with all necessary configuration, resources, and schema files.

### 3. Install Python Dependencies

Requires **Python 3.10+**.

```bash
pip install senzing senzing-core orjson
```

| Package        | Purpose                                                          |
| -------------- | ---------------------------------------------------------------- |
| `senzing`      | Senzing V4 Python SDK interface definitions                      |
| `senzing-core` | Native Senzing engine bindings (`SzAbstractFactoryCore`)         |
| `orjson`       | High-performance JSON parsing (significantly faster than `json`) |

## Configuration

### SENZING_ENGINE_CONFIGURATION_JSON

The Senzing engine is configured via the `SENZING_ENGINE_CONFIGURATION_JSON` environment variable. This JSON string tells Senzing where to find its resources and how to connect to the database.

For an in-memory SQLite setup, the `SQL.CONNECTION` URI points to a SQLite database path. The program intercepts this path and creates the database in memory instead of on disk.

```bash
export SENZING_ENGINE_CONFIGURATION_JSON='{
  "PIPELINE": {
    "CONFIGPATH": "/home/user/senzing_project/etc",
    "RESOURCEPATH": "/home/user/senzing_project/resources",
    "SUPPORTPATH": "/home/user/senzing_project/data"
  },
  "SQL": {
    "CONNECTION": "sqlite3://na:na@/home/user/senzing_project/var/sqlite/G2C.db"
  }
}'
```

| Field                   | Description                                                                                |
| ----------------------- | ------------------------------------------------------------------------------------------ |
| `PIPELINE.CONFIGPATH`   | Path to Senzing configuration files (`etc/` in your project)                               |
| `PIPELINE.RESOURCEPATH` | Path to Senzing resource files including the SQLite schema                                 |
| `PIPELINE.SUPPORTPATH`  | Path to Senzing support data files                                                         |
| `SQL.CONNECTION`        | SQLite connection URI — the path is extracted and used for the in-memory shared cache name |

**Tip:** When you create a Senzing project, the configuration file at `<project>/etc/sz_engine_config.ini` contains the equivalent values. You can use the helper utility to generate the JSON:

```bash
source ~/senzing_project/setupEnv
```

## Input Data Format

Input data must be a **JSONL** (JSON Lines) file — one JSON record per line. Each record **must** contain:

- `DATA_SOURCE` — Identifies which data source the record belongs to
- `RECORD_ID` — A unique identifier for the record within its data source

Additional fields should follow the [Senzing Entity Specification](https://senzing.com/docs/) for attribute mapping (names, addresses, identifiers, etc.).

### Example Input (`sample.jsonl`)

```json
{"DATA_SOURCE": "CUSTOMERS", "RECORD_ID": "1001", "NAME_FULL": "Robert Smith", "ADDR_FULL": "123 Main St, Las Vegas, NV 89132", "PHONE_NUMBER": "702-555-0100", "DATE_OF_BIRTH": "1978-12-11"}
{"DATA_SOURCE": "CUSTOMERS", "RECORD_ID": "1002", "NAME_FULL": "Bob R Smith Jr", "ADDR_FULL": "123 Main Street, Las Vegas, NV 89132", "EMAIL_ADDRESS": "bob@example.com"}
{"DATA_SOURCE": "WATCHLIST", "RECORD_ID": "W-1001", "NAME_FULL": "Robert Smith", "DATE_OF_BIRTH": "12/11/1978", "SSN_NUMBER": "111-22-3333"}
```

In this example, Senzing's AI-driven entity resolution would likely resolve the first two records as the same person (despite name/address variations) and evaluate the third record against them using additional identifiers.

## Usage

```bash
# Set the Senzing engine configuration
export SENZING_ENGINE_CONFIGURATION_JSON='{ ... }'

# Run entity resolution on your data
python mem_load.py <input_file.jsonl>
```

### Command-Line Options

| Option                    | Description                                           |
| ------------------------- | ----------------------------------------------------- |
| `fileToProcess`           | **(Required)** Path to the JSONL input file           |
| `-t`, `--debugTrace`      | Enable verbose Senzing debug trace logging            |
| `-x`, `--skipEnginePrime` | Skip engine priming for faster startup (experimental) |

### Example Run

```bash
export SENZING_ENGINE_CONFIGURATION_JSON='{
  "PIPELINE": {
    "CONFIGPATH": "/home/user/senzing_project/etc",
    "RESOURCEPATH": "/home/user/senzing_project/resources",
    "SUPPORTPATH": "/home/user/senzing_project/data"
  },
  "SQL": {
    "CONNECTION": "sqlite3://na:na@/home/user/senzing_project/var/sqlite/G2C.db"
  }
}'

python mem_load.py sample.jsonl
```

### Expected Output

```
Added DATA_SOURCE: CUSTOMERS
Added DATA_SOURCE: WATCHLIST
Processed 1,000 adds, 850 records per second, 0 errors
Processed 2,000 adds, 920 records per second, 0 errors
...
Successfully loaded 10,000 records, with 0 errors

{ ... engine statistics JSON ... }

Redo created: 42
Threads: 8
Processed 1,000 redo records, with 0 errors
...
Copied 5000 of 12000 pages...
Copied 10000 of 12000 pages...
Copied 12000 of 12000 pages...
```

After completion, the resolved entity data is saved to **`backup.db`** in the current directory — a standard SQLite database file you can query directly.

## Output: backup.db

The `backup.db` file contains the complete Senzing datastore after entity resolution. You can use any SQLite-compatible tool to explore the results:

```bash
sqlite3 backup.db
```

Or use Senzing tools and APIs to query resolved entities from the backup database.

## Architecture & Key Design Decisions

### In-Memory SQLite with Shared Cache

```python
conn = sqlite3.connect(
    "file:" + parsed.path[1:] + "?mode=memory&cache=shared",
    autocommit=True
)
```

The `mode=memory&cache=shared` URI parameters create a named in-memory database that multiple connections (threads) can access simultaneously. The database name is derived from the path in the Senzing configuration, ensuring Senzing's internal connections reference the same in-memory instance. Journal mode is set to `OFF` since durability is unnecessary for an in-memory database.

### Multithreaded Processing Pattern

Both record ingestion (`futures_add`) and redo processing (`futures_redo`) use a **bounded thread pool** pattern:

1. Pre-fill the thread pool with one task per worker thread
2. Wait for the first task to complete (`FIRST_COMPLETED`)
3. On success, submit the next record to maintain pool saturation
4. On error, classify and handle appropriately without submitting a replacement

This keeps the thread pool fully saturated without unbounded memory growth from queuing all records at once.

### Error Handling Strategy

| Error Type                            | Severity | Action                             |
| ------------------------------------- | -------- | ---------------------------------- |
| `SzBadInputError` / `JSONDecodeError` | ERROR    | Log and skip — bad record data     |
| `SzRetryableError`                    | WARN     | Log and skip — transient issue     |
| `SzUnrecoverableError` / `SzError`    | CRITICAL | Log and abort — fatal engine error |

## Senzing V4 SDK Components Used

| Component               | Purpose                                                               |
| ----------------------- | --------------------------------------------------------------------- |
| `SzAbstractFactoryCore` | Factory for creating all Senzing engine objects                       |
| `SzConfigManager`       | Manage Senzing configurations (create, persist defaults)              |
| `SzConfig`              | Register data sources and export configuration                        |
| `SzEngine`              | Core entity resolution engine — add records, process redos, get stats |
| `SzEngineFlags`         | Control flags for engine operations                                   |

### SDK Object Lifecycle

```
SzAbstractFactoryCore (initialization)
    ├── create_configmanager() → SzConfigManager
    │       └── create_config_from_template() → SzConfig
    │               ├── register_data_source()  (for each DATA_SOURCE)
    │               └── export() → config JSON
    │       └── set_default_config()  (persist to datastore)
    └── create_engine() → SzEngine
            ├── add_record()           (load records)
            ├── get_stats()            (performance metrics)
            ├── count_redo_records()   (check for pending redos)
            ├── get_redo_record()      (fetch a redo record)
            └── process_redo_record()  (resolve deferred work)
```

## Limitations

- **SQLite in-memory** is single-node only — not suitable for distributed or production deployments
- **Memory-bound** — the entire datastore must fit in available RAM
- The default Senzing license allows up to **500 source records**. Contact [Senzing](https://senzing.com) for larger evaluations
- The backup file (`backup.db`) is written to the current working directory

## Resources

- [Senzing V4 Linux Quickstart Guide](https://www.senzing.com/docs/quickstart/quickstart_api)
- [Senzing V4 Python SDK Documentation](https://garage.senzing.com/sz-sdk-python/senzing.html)
- [Senzing in 3 Python Calls](https://www.senzing.com/docs/python/4/3calls)
- [Senzing V4 Code Snippets](https://github.com/Senzing/code-snippets-v4)
- [What Is Entity Resolution?](https://senzing.com/what-is-entity-resolution/)
- [Senzing Engine Configuration](https://www.senzing.com/docs/tutorials/senzing_engine_config/)
- [Senzing Entity Specification](https://senzing.com/docs/)

## License

[Apache License 2.0](LICENSE)
