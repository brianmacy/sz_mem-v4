#! /usr/bin/env python3

"""Senzing V4 Entity Resolution with In-Memory SQLite.

Loads entity records from a JSONL file, performs entity resolution using the
Senzing V4 SDK backed by a SQLite in-memory database, processes any deferred
redo records, and backs up the resulting datastore to a persistent file.

This approach eliminates the need for a database server, making it ideal for
development, testing, and proof-of-concept evaluations.

Usage:
    export SENZING_ENGINE_CONFIGURATION_JSON='{ ... }'
    python mem_load.py <input_file.jsonl> [-t] [-x]

Environment:
    SENZING_ENGINE_CONFIGURATION_JSON: Required. JSON string with Senzing
        engine configuration including SQL.CONNECTION (SQLite URI) and
        PIPELINE paths (CONFIGPATH, RESOURCEPATH, SUPPORTPATH).

Requirements:
    - Senzing V4 SDK installed (senzingsdk-runtime or senzingsdk-poc)
    - PYTHONPATH includes /opt/senzing/er/sdk/python
    - LD_LIBRARY_PATH includes /opt/senzing/er/lib
    - Python package: orjson (pip install orjson)
"""

import os
import sys
import orjson as json
import time
import sqlite3
from urllib.parse import urlparse
import concurrent.futures

import argparse

import itertools

# Progress is logged every LOG_INTERVAL successfully processed records.
# Engine statistics are printed every STATS_INTERVAL records.
LOG_INTERVAL = 1000
STATS_INTERVAL = 10 * LOG_INTERVAL

from senzing import (
    SzConfig,
    SzConfigManager,
    SzEngine,
    SzEngineFlags,
    SzError,
    SzRetryableError,
    SzBadInputError,
    SzUnrecoverableError,
)

from senzing_core import SzAbstractFactoryCore


def progress(status, remaining, total):
    """SQLite backup progress callback.

    Called by sqlite3.Connection.backup() after each batch of pages is copied,
    providing visibility into the backup operation's progress.

    Args:
        status: The status code from SQLite (unused).
        remaining: Number of pages still to be copied.
        total: Total number of pages in the source database.
    """
    print(f"Copied {total-remaining} of {total} pages...")


def mock_logger(level, exception, error_rec=None):
    """Log an error message to stderr with optional offending record.

    Provides a simple logging mechanism that writes structured error output
    to stderr. In production, this would be replaced with a proper logging
    framework.

    Args:
        level: Severity level string (e.g., "ERROR", "WARN", "CRITICAL").
        exception: The exception object or error message to log.
        error_rec: Optional. The source record that caused the error,
            included in the output for debugging.
    """
    print(f"\n{level}: {exception}", file=sys.stderr)
    if error_rec:
        print(f"{error_rec}", file=sys.stderr)

def process_redo(engine, rec):
    """Process a single redo record through the Senzing engine.

    Redo records are deferred entity re-evaluations created during ingestion
    when the engine determines that prior entity decisions need revisiting
    based on newly observed data.

    Args:
        engine: An initialized SzEngine instance.
        rec: The redo record JSON string obtained from engine.get_redo_record().
    """
    engine.process_redo_record(rec)


def add_record(engine, rec_to_add):
    """Parse and add a single entity record to the Senzing engine.

    Extracts the DATA_SOURCE and RECORD_ID from the JSON record and submits
    it to the Senzing engine for entity resolution. The engine will match
    and merge this record with existing entities as appropriate.

    Args:
        engine: An initialized SzEngine instance.
        rec_to_add: A JSON-encoded string (bytes or str) containing at minimum
            DATA_SOURCE and RECORD_ID fields, plus any entity attributes
            (names, addresses, identifiers, etc.).

    Raises:
        SzBadInputError: If the record contains invalid data for Senzing.
        SzRetryableError: If a transient engine error occurs.
        SzUnrecoverableError: If a fatal engine error occurs.
        orjson.JSONDecodeError: If rec_to_add is not valid JSON.
    """
    record_dict = json.loads(rec_to_add)
    data_source = record_dict.get("DATA_SOURCE", None)
    record_id = record_dict.get("RECORD_ID", None)
    engine.add_record(data_source, record_id, rec_to_add)


def engine_stats(engine):
    """Retrieve and print current engine performance statistics.

    Queries the Senzing engine for internal performance metrics and prints
    them as a JSON string. Statistics include workload breakdown, timing,
    and throughput information useful for performance tuning.

    Args:
        engine: An initialized SzEngine instance.

    Raises:
        SzError: Re-raised if a non-retryable engine error occurs.
    """
    try:
        response = engine.get_stats()
        print(f"\n{response}\n")
    except SzRetryableError as err:
        mock_logger("WARN", err)
    except SzError as err:
        mock_logger("CRITICAL", err)
        raise


def record_stats(total_records, record_delta, error, prev_time):
    """Print a progress line showing throughput and error counts.

    Calculates and displays the current processing rate based on the number
    of records processed since the last progress report.

    Args:
        total_records: Total number of records successfully processed so far.
        record_delta: Number of records processed since the last report.
        error: Total number of errors encountered so far.
        prev_time: Timestamp (from time.time()) of the previous report.

    Returns:
        float: The current timestamp, to be passed as prev_time on the
        next call.
    """
    print(
        f"Processed {total_records:,} adds,"
        f" {int(record_delta / (time.time() - prev_time)):,} records per second,"
        f" {error} errors",
        flush=True,
    )
    return time.time()


def futures_add(engine, file):
    """Ingest records from a file using a bounded thread pool.

    Reads entity records from a JSONL file and submits them to the Senzing
    engine for entity resolution using concurrent threads. The thread pool
    is kept fully saturated by submitting a new record each time one
    completes, avoiding unbounded memory growth.

    Progress is logged every LOG_INTERVAL records. Engine statistics are
    printed every STATS_INTERVAL records.

    Error handling:
        - SzBadInputError / JSONDecodeError: logged as ERROR, record skipped
        - SzRetryableError: logged as WARN, record skipped
        - SzUnrecoverableError / SzError: logged as CRITICAL, processing aborted

    Args:
        engine: An initialized SzEngine instance.
        file: An open file object positioned at the start of the JSONL data.
            Each line must be a complete JSON record.
    """
    prev_time = time.time()
    prev_success = success_recs = error_recs = 0

    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Pre-fill the thread pool: submit one task per worker thread
        # to start processing immediately at full concurrency.
        futures = {}
        for i in range(0, executor._max_workers):
            record = file.readline()
            if record:
                futures[executor.submit(add_record, engine, record)] = (
                    record
                )

        # Process completed futures and refill the pool as records finish.
        # This bounded pattern prevents queuing all records in memory at once.
        while futures:
            done, _ = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for f in done:
                try:
                    f.result()
                except (SzBadInputError, json.JSONDecodeError) as err:
                    mock_logger("ERROR", err, futures[f])
                    error_recs += 1
                except SzRetryableError as err:
                    mock_logger("WARN", err, futures[f])
                    error_recs += 1
                except (SzUnrecoverableError, SzError) as err:
                    mock_logger("CRITICAL", err, futures[f])
                    raise
                else:
                    # Only submit the next record on success.
                    # On error the pool shrinks by one, providing natural
                    # backpressure if errors are frequent.
                    record = file.readline()
                    if record:
                        futures[executor.submit(add_record, engine, record)] = (
                            record
                        )

                    success_recs += 1
                    if success_recs % LOG_INTERVAL == 0:
                        # if time.time() - prev_time > 1:
                        prev_time = record_stats(
                            success_recs,
                            success_recs - prev_success,
                            error_recs,
                            prev_time,
                        )
                        prev_success = success_recs

                    if success_recs % STATS_INTERVAL == 0:
                        engine_stats(engine)
                    if success_recs % 100000 == 0:
                        try:
                            # cur.execute("PRAGMA optimize")
                            pass
                        except BaseException:
                            pass
                finally:
                    del futures[f]

        print(
            f"Successfully loaded {success_recs:,} records, with"
            f" {error_recs:,} errors"
        )
        engine_stats(engine)


def futures_redo(engine):
    """Process all pending redo records using a bounded thread pool.

    After initial record ingestion, the Senzing engine may have created redo
    records — deferred entity re-evaluations triggered when new data reveals
    that prior entity resolution decisions need to be revisited. This function
    drains the redo queue by processing records concurrently.

    Uses the same bounded thread pool pattern as futures_add: the pool is
    pre-filled and refilled as tasks complete, preventing unbounded memory
    growth.

    Progress is logged every LOG_INTERVAL records. Engine statistics and
    remaining redo counts are printed every STATS_INTERVAL records.

    Args:
        engine: An initialized SzEngine instance with pending redo records.

    Raises:
        SzUnrecoverableError: If a fatal engine error occurs during processing.
        SzError: If a non-retryable engine error occurs.
    """
    success_recs = error_recs = 0
    redo_paused = False

    with concurrent.futures.ThreadPoolExecutor() as executor:
        print(f"Threads: {executor._max_workers}")
        # Pre-fill the thread pool with redo records.
        futures = {}
        for i in range(0, executor._max_workers):
            record = engine.get_redo_record()
            if record:
                futures[executor.submit(process_redo, engine, record)] = record

        while True:
            done, _ = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_COMPLETED
            )
            for f in done:
                try:
                    _ = f.result()
                    success_recs += 1
                    if success_recs % LOG_INTERVAL == 0:
                        print(
                            f"Processed {success_recs:,} redo records, with"
                            f" {error_recs:,} errors"
                        )

                    if success_recs % STATS_INTERVAL == 0:
                        engine_stats(engine)
                        print(f"Redo remaining: {engine.count_redo_records()}")
                except SzBadInputError as err:
                    mock_logger("ERROR", err, futures[f])
                    error_recs += 1
                except SzRetryableError as err:
                    mock_logger("WARN", err, futures[f])
                    error_recs += 1
                except (SzUnrecoverableError, SzError) as err:
                    mock_logger("CRITICAL", err, futures[f])
                    raise

                finally:
                    del futures[f]

            # Refill the pool. If no more redo records are available and all
            # futures have completed, exit the loop.
            while len(futures) < executor._max_workers:
                record = engine.get_redo_record()
                if not record:
                    break
                futures[executor.submit(process_redo, engine, record)] = record

            if not futures:
                break


# =============================================================================
# Main execution
# =============================================================================
# The main flow is wrapped in a try/except to catch and display any fatal
# Senzing errors at the top level.

try:
    # -------------------------------------------------------------------------
    # Step 1: Parse command-line arguments
    # -------------------------------------------------------------------------
    parser = argparse.ArgumentParser()
    parser.add_argument("fileToProcess", default=None)
    parser.add_argument(
        "-t",
        "--debugTrace",
        dest="debugTrace",
        action="store_true",
        default=False,
        help="output debug trace information",
    )
    parser.add_argument(
        "-x",
        "--skipEnginePrime",
        dest="skipEnginePrime",
        action="store_true",
        default=False,
        help="skip the engine prime_engine to speed up execution",
    )
    args = parser.parse_args()

    # -------------------------------------------------------------------------
    # Step 2: Load Senzing engine configuration from environment
    # -------------------------------------------------------------------------
    # The SENZING_ENGINE_CONFIGURATION_JSON environment variable provides the
    # JSON configuration that tells Senzing where to find resources and how to
    # connect to the database. For this example, the SQL.CONNECTION points to
    # a SQLite path which we intercept to create an in-memory database.
    engine_config_json = os.getenv("SENZING_ENGINE_CONFIGURATION_JSON", None)
    if not engine_config_json:
        print(
            "The environment variable SENZING_ENGINE_CONFIGURATION_JSON must be set with a proper JSON configuration.",
            file=sys.stderr,
        )
        print(
            "Please see https://senzing.zendesk.com/hc/en-us/articles/360038774134-G2Module-Configuration-and-the-Senzing-API",
            file=sys.stderr,
        )
        sys.exit(-1)

    # -------------------------------------------------------------------------
    # Step 3: Create the in-memory SQLite database
    # -------------------------------------------------------------------------
    # Extract the database path from the Senzing configuration's SQL.CONNECTION
    # URI (e.g., "sqlite3://na:na@/path/to/G2C.db") and open it as a named
    # in-memory database with shared cache. This allows both this program's
    # connection and Senzing's internal connections to access the same in-memory
    # database instance.
    #
    # Key SQLite URI parameters:
    #   mode=memory  - database exists only in RAM, not on disk
    #   cache=shared - multiple connections can access the same in-memory DB
    #
    # Journal mode is set to OFF because durability is unnecessary for an
    # in-memory database (there is nothing to recover from).
    engine_config = json.loads(engine_config_json)
    uri = engine_config["SQL"]["CONNECTION"]
    parsed = urlparse(uri)

    conn = sqlite3.connect(
        "file:" + parsed.path[1:] + "?mode=memory&cache=shared", autocommit=True
    )
    cur = conn.cursor()
    cur.execute("pragma journal_mode = OFF")

    # Load the Senzing SQLite schema from the resource path. This creates the
    # tables, indexes, and triggers that the Senzing engine requires.
    resource_path = engine_config["PIPELINE"]["RESOURCEPATH"]
    with open(
        os.path.join(resource_path, "schema/szcore-schema-sqlite-create.sql")
    ) as schema_file:
        for line in schema_file:
            line = line.strip()
            if not line:
                continue
            cur.execute(line)

    try:
        # ---------------------------------------------------------------------
        # Step 4: Initialize the Senzing SDK via the Abstract Factory
        # ---------------------------------------------------------------------
        # SzAbstractFactoryCore creates all Senzing SDK objects configured
        # for native (in-process) execution. The factory takes:
        #   - instance_name: identifies this application in logs
        #   - engine_config_json: the full Senzing configuration
        #   - verbose_logging: enables detailed Senzing debug output
        factory = SzAbstractFactoryCore(
            "mem_load", engine_config_json, verbose_logging=args.debugTrace
        )

        # ---------------------------------------------------------------------
        # Step 5: Register data sources in the Senzing configuration
        # ---------------------------------------------------------------------
        # Before loading records, all DATA_SOURCE values must be registered
        # in the Senzing configuration. This step:
        #   1. Creates a config manager to manage Senzing configurations
        #   2. Creates a new config from the default template
        #   3. Scans the entire input file to discover all unique DATA_SOURCE
        #      values
        #   4. Registers each data source in the configuration
        #   5. Persists the configuration as the new default
        sz_configmgr = factory.create_configmanager()
        sz_config = sz_configmgr.create_config_from_template()

        with open(args.fileToProcess, "r") as file:
            known_datasources = set()
            for line in file:
                rec = json.loads(line)
                data_source = rec["DATA_SOURCE"].upper()
                if not data_source in known_datasources:
                    sz_config.register_data_source(data_source)
                    known_datasources.add(data_source)
                    print(f"Added DATA_SOURCE: {data_source}")

        # Persist new default config to Senzing Repository
        try:
            config_id = sz_configmgr.set_default_config(
                sz_config.export(), "New default configuration added."
            )
        except SzError:
            raise

        # ---------------------------------------------------------------------
        # Step 6: Load records and perform entity resolution
        # ---------------------------------------------------------------------
        # Create the Senzing engine and process all records from the input
        # file using multithreaded ingestion. Each record is parsed, and its
        # DATA_SOURCE + RECORD_ID are used to add it to the engine, which
        # performs real-time entity resolution as records are added.
        sz_engine = factory.create_engine()

        with open(args.fileToProcess, "r") as file:
            futures_add(sz_engine, file)

        # ---------------------------------------------------------------------
        # Step 7: Process redo records
        # ---------------------------------------------------------------------
        # During ingestion, the engine may have created redo records — deferred
        # entity re-evaluations. For example, adding record C might reveal that
        # previously separate entities A and B should now be merged. Processing
        # redos ensures all entity relationships are fully resolved.
        print(f"Redo created: {sz_engine.count_redo_records()}")
        futures_redo(sz_engine)

        # ---------------------------------------------------------------------
        # Step 8: Backup the in-memory database to disk
        # ---------------------------------------------------------------------
        # Copy the complete in-memory Senzing datastore to a persistent SQLite
        # file (backup.db). The backup uses WAL journal mode for better
        # concurrent read performance on the output file.
        #
        # The pages parameter (10000) controls how many pages are copied per
        # batch, with the progress callback reporting after each batch.
        dst = sqlite3.connect("backup.db")
        dst_cur = dst.cursor()
        dst_cur.execute("pragma journal_mode = WAL")
        dst_cur.close()
        with dst:
            conn.backup(dst, pages=10000, progress=progress)
        dst.close()
        conn.close()

    except SzError:
        raise

except SzError as err:
    print(err)
    sys.exit(-1)
