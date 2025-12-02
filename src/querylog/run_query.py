#!/usr/bin/env python3
"""
Script to run any SQL query against BigQuery with cost validation and flexible output formats.
This script executes SQL queries against BigQuery with dry run validation and exports results
to JSON, CSV, or other formats based on the output file extension.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any

import pandas as pd
from google.cloud import bigquery

from querylog.config import get_billing_project, get_bq_region


def read_sql_file(file_path: str) -> str:
    """
    Read SQL query from file.

    Args:
        file_path: Path to the SQL file

    Returns:
        SQL query string
    """
    try:
        with open(file_path, encoding="utf-8") as file:
            return file.read()
    except FileNotFoundError:
        print(f"Error: SQL file not found at {file_path}")
        sys.exit(1)
    except Exception as e:
        print(f"Error reading SQL file: {e}")
        sys.exit(1)


def perform_dry_run(query: str, client: bigquery.Client) -> tuple[int, float, float]:
    """
    Perform a dry run to estimate query cost and data processing.

    Args:
        query: SQL query string
        client: BigQuery client

    Returns:
        Tuple of (bytes_processed, gb_processed, estimated_cost_usd)
    """
    try:
        print("🔍 Performing dry run to estimate query cost...")

        # Configure dry run job
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        job_config.labels = {
            "type": "script",
            "category": "querylog",
            "sub_category": "dry_run",
        }

        # Execute dry run
        dry_run_job = client.query(query, job_config=job_config)

        # Extract and normalize numeric stats in a safe manner
        bytes_processed = int(getattr(dry_run_job, "total_bytes_processed", 0) or 0)
        gb_processed = float(bytes_processed) / float(1024**3)

        # BigQuery pricing: $6.25 per TB processed (as of 2024)
        estimated_cost_usd = (float(bytes_processed) / float(1024**4)) * 6.25

        print("\n📊 DRY RUN RESULTS:")
        print(f"   • Bytes to process: {bytes_processed:,}")
        print(f"   • GB to process: {gb_processed:.2f} GB")
        print(f"   • Estimated cost: ${estimated_cost_usd:.4f} USD")

        return bytes_processed, gb_processed, estimated_cost_usd

    except Exception as e:
        print(f"❌ Dry run failed: {e}")
        print(
            "   This might be due to table access permissions or query syntax errors."
        )
        sys.exit(1)


def ask_user_confirmation(gb_processed: float) -> bool:
    """
    Ask user for confirmation if data shuffle exceeds 100GB.

    Args:
        gb_processed: GB of data to be processed

    Returns:
        True if user confirms execution, False otherwise
    """
    if gb_processed > 100:
        print(
            f"\n⚠️  WARNING: Data shuffle ({gb_processed:.2f} GB) exceeds 100GB threshold!"
        )
        print("   This query may be expensive and time-consuming to execute.")

        while True:
            response = (
                input("   Do you want to proceed anyway? (y/N): ").strip().lower()
            )
            if response in ["y", "yes"]:
                print("   ✅ Proceeding with query execution...")
                return True
            elif response in ["n", "no", ""]:
                print("   ❌ Query execution cancelled by user.")
                return False
            else:
                print("   Please enter 'y' for yes or 'n' for no.")
    else:
        print(f"✅ Data shuffle ({gb_processed:.2f} GB) is within acceptable limits.")
        return True


def run_bigquery_query(query: str, client: bigquery.Client) -> tuple[pd.DataFrame, Any]:
    """
    Execute BigQuery query and return results as DataFrame.

    Args:
        query: SQL query string
        client: BigQuery client

    Returns:
        DataFrame with query results
    """
    try:
        print("🚀 Executing BigQuery query...")
        job_config = bigquery.QueryJobConfig()
        job_config.labels = {
            "type": "script",
            "category": "querylog",
            "sub_category": "query",
        }
        query_job = client.query(query, job_config=job_config)
        results = query_job.result()

        # Try to convert to DataFrame using the Storage API (requires bigquery.readsessions.create)
        try:
            df = results.to_dataframe()
        except Exception as df_error:
            # Check if this is a permission error for BigQuery Storage API
            error_msg = str(df_error)
            if "bigquery.readsessions.create" in error_msg or "Permission" in error_msg:
                print(
                    "   ⚠️  BigQuery Storage API not available (missing bigquery.readsessions.create permission)"
                )
                print(
                    "   📥 Falling back to legacy results API (may be slower for large result sets)..."
                )

                # Fallback: manually construct DataFrame from results
                rows = list(results)
                if rows:
                    # Extract column names from schema
                    columns = [field.name for field in results.schema]
                    # Convert rows to list of dicts
                    data = [
                        {col: row[i] for i, col in enumerate(columns)} for row in rows
                    ]
                    df = pd.DataFrame(data)
                else:
                    # Empty result set
                    columns = [field.name for field in results.schema]
                    df = pd.DataFrame(columns=columns)
            else:
                # Different error, re-raise it
                raise

        print(f"✅ Query completed successfully. Retrieved {len(df)} rows.")

        # Display execution stats (safely access attributes)
        actual_bytes = int(getattr(query_job, "total_bytes_processed", 0) or 0)
        print(f"   • Actual bytes processed: {actual_bytes:,}")

        # Safely compute query duration; some QueryJob implementations may not
        # expose `started`/`ended` or support subtraction in a typed manner.
        started = getattr(query_job, "started", None)
        ended = getattr(query_job, "ended", None)
        duration_str = "N/A"
        if started and ended:
            try:
                duration = ended - started
                duration_str = str(duration)
            except Exception:
                duration_str = (
                    f"Could not compute duration (started={started}, ended={ended})"
                )

        print(f"   • Query duration: {duration_str}")

        # Return both DataFrame and the underlying query job so callers can inspect
        # the destination table (temporary table) if present.
        return df, query_job

    except Exception as e:
        print(f"❌ Error executing BigQuery query: {e}")
        sys.exit(1)


def save_query_plan_and_stats(
    query_job: Any, output_path: Path, row_count: int, client: bigquery.Client
) -> None:
    """
    Save query plan and statistics to a JSON file with destination table info at the top.

    Args:
        query_job: BigQuery QueryJob object
        output_path: Path where to save the JSON file (with .query_stats.json extension)
        row_count: Number of rows returned by the query
        client: BigQuery client (for accessing project info)
    """
    try:
        stats = {}

        # Destination table information (at the top)
        dest = getattr(query_job, "destination", None)
        if dest:
            try:
                proj = getattr(dest, "project", client.project) or client.project
                dataset = getattr(dest, "dataset_id", getattr(dest, "dataset", ""))
                table = getattr(dest, "table_id", getattr(dest, "table", ""))
                stats["destination_table"] = f"{proj}.{dataset}.{table}"
            except Exception:
                stats["destination_table"] = str(dest)
        else:
            stats["destination_table"] = None

        stats["row_count"] = row_count

        # Basic job information
        stats["job_id"] = getattr(query_job, "job_id", None)

        # Statistics
        stats["total_bytes_processed"] = int(
            getattr(query_job, "total_bytes_processed", 0) or 0
        )
        stats["total_bytes_billed"] = int(
            getattr(query_job, "total_bytes_billed", 0) or 0
        )
        stats["billing_tier"] = getattr(query_job, "billing_tier", None)
        stats["total_slot_ms"] = int(getattr(query_job, "total_slot_ms", 0) or 0)
        stats["cache_hit"] = getattr(query_job, "cache_hit", None)
        stats["num_dml_affected_rows"] = int(
            getattr(query_job, "num_dml_affected_rows", 0) or 0
        )

        # Query plan stages (execution plan)
        query_plan = getattr(query_job, "query_plan", None)
        if query_plan:
            stats["query_plan"] = []
            for stage in query_plan:
                stage_info = {
                    "name": getattr(stage, "name", None),
                    "id": int(getattr(stage, "id", 0) or 0),
                    "status": getattr(stage, "status", None),
                    "shuffle_output_bytes": int(
                        getattr(stage, "shuffle_output_bytes", 0) or 0
                    ),
                    "shuffle_output_bytes_spilled": int(
                        getattr(stage, "shuffle_output_bytes_spilled", 0) or 0
                    ),
                    "records_read": int(getattr(stage, "records_read", 0) or 0),
                    "records_written": int(getattr(stage, "records_written", 0) or 0),
                }

                # Add steps within the stage if available
                steps = getattr(stage, "steps", None)
                if steps:
                    stage_info["steps"] = []
                    for step in steps:
                        step_info = {
                            "kind": getattr(step, "kind", None),
                            "substeps": getattr(step, "substeps", []),
                        }
                        stage_info["steps"].append(step_info)

                # Add input stages
                input_stages = getattr(stage, "input_stages", None)
                if input_stages:
                    stage_info["input_stages"] = [int(s) for s in input_stages]

                stats["query_plan"].append(stage_info)

        # Save to JSON file
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(stats, f, indent=2)

        print(f"✅ Query plan and statistics saved to: {output_path}")

    except Exception as e:
        print(f"⚠️  Warning: Could not save query plan and statistics: {e}")


def save_results(df: pd.DataFrame, output_path: str) -> None:
    """
    Save DataFrame to file based on extension (JSON, CSV, etc.).

    Args:
        df: DataFrame to save
        output_path: Path where to save the file
    """
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # Determine format from file extension
        file_extension = Path(output_path).suffix.lower()

        if file_extension == ".json":
            df.to_json(output_path, index=False, orient="records")
        elif file_extension == ".csv":
            df.to_csv(output_path, index=False, encoding="utf-8")
        elif file_extension == ".parquet":
            df.to_parquet(output_path, index=False)
        elif file_extension == ".xlsx":
            df.to_excel(output_path, index=False)
        else:
            # Default to CSV if extension is unknown
            print(
                f"   Unknown file extension '{file_extension}', defaulting to CSV format"
            )
            df.to_csv(output_path, index=False, encoding="utf-8")

        print(f"✅ Results saved to: {output_path}")

    except Exception as e:
        print(f"❌ Error saving results file: {e}")
        sys.exit(1)


def main():
    """Main function to execute the script."""
    parser = argparse.ArgumentParser(
        description="Run any SQL query against BigQuery with cost validation and flexible output formats"
    )
    parser.add_argument(
        "sql_file", help="Path to SQL file containing the query to execute"
    )
    parser.add_argument(
        "--output_file",
        default=None,
        help="Output file path (format determined by extension: .json, .csv, .parquet, .xlsx)",
    )
    parser.add_argument(
        "--project-id",
        default=None,
        help="BigQuery project ID (default: use default credentials project)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Skip cost validation and execute query directly",
    )

    args = parser.parse_args()

    # Get absolute path for SQL file
    sql_file_path = Path(args.sql_file)
    if not sql_file_path.is_absolute():
        # Assume relative to current working directory
        sql_file_path = Path.cwd() / sql_file_path

    print(f"📁 Reading SQL query from: {sql_file_path}")
    sql_query = read_sql_file(str(sql_file_path))

    # Initialize BigQuery client
    try:
        if args.project_id:
            client = bigquery.Client(project=args.project_id)
        else:
            # Use project ID from config as default
            project_id = get_billing_project()
            # Use location from config
            location = get_bq_region()
            client = bigquery.Client(project=project_id, location=location)
        print(f"🔗 Connected to BigQuery project: {client.project}")
    except Exception as e:
        print(f"❌ Error initializing BigQuery client: {e}")
        print("   Make sure you have proper authentication set up (gcloud auth login)")
        print("   and that PROJECT_ID is set in your .env file")
        sys.exit(1)

    # Perform dry run unless forced
    if not args.force:
        _, gb_processed, _ = perform_dry_run(sql_query, client)

        # Ask for user confirmation if data shuffle is high
        if not ask_user_confirmation(gb_processed):
            sys.exit(0)

    # Execute query
    df, query_job = run_bigquery_query(sql_query, client)

    # Display basic info about results
    print("\n📋 Query Results Summary:")
    print(f"   • Total rows: {len(df):,}")
    print(f"   • Total columns: {len(df.columns)}")
    print(f"   • Columns: {', '.join(df.columns.tolist())}")

    # Show sample of results
    if len(df) > 0:
        print("\n📊 Sample Results (first 5 rows):")
        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", None)

        print(df.head().to_string(index=False))

    # Save results
    if args.output_file:
        # Get absolute path for output file
        output_path = Path(args.output_file)
        if not output_path.is_absolute():
            # Assume relative to current working directory
            output_path = Path.cwd() / output_path

        save_results(df, str(output_path))

    # Save query plan and statistics to a JSON file (includes destination table info at the top)
    query_stats_path = sql_file_path.parent / f"{sql_file_path.stem}.query_stats.json"
    save_query_plan_and_stats(query_job, query_stats_path, len(df), client)

    print("\n🎉 Script completed successfully!")


if __name__ == "__main__":
    main()
