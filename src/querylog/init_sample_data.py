#!/usr/bin/env python3
"""
Script to copy BigQuery public sample datasets to your project.
This script copies tables from bigquery-public-data datasets
to your project specified in the .env file.

Supports regex filtering on both datasets and tables.
"""

import argparse
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from google.cloud import bigquery

from querylog.config import (
    get_billing_project,
    get_bq_region,
    get_dataset_filter_regex,
    get_table_filter_regex,
)

# Size threshold for confirmation (in GB)
SIZE_THRESHOLD_GB = 50


def get_datasets(
    client: bigquery.Client, project_id: str, dataset_pattern: str | None = None
) -> list[str]:
    """
    Get list of datasets from a project, optionally filtered by regex pattern.

    Args:
        client: BigQuery client
        project_id: Project ID to list datasets from
        dataset_pattern: Optional regex pattern to filter dataset names

    Returns:
        List of dataset IDs
    """
    datasets = client.list_datasets(project_id)
    dataset_ids = [dataset.dataset_id for dataset in datasets]

    if dataset_pattern:
        pattern = re.compile(dataset_pattern)
        dataset_ids = [ds for ds in dataset_ids if pattern.search(ds)]

    return dataset_ids


def get_sample_tables(
    client: bigquery.Client,
    dataset_id: str,
    table_pattern: str | None = None,
) -> list[str]:
    """
    Get list of tables from a dataset, optionally filtered by regex pattern.
    Excludes tables that are known to have access restrictions.

    Args:
        client: BigQuery client
        dataset_id: Full dataset ID (project.dataset)
        table_pattern: Optional regex pattern to filter table names

    Returns:
        List of table names
    """
    tables = client.list_tables(dataset_id)
    table_ids = [table.table_id for table in tables]

    if table_pattern:
        pattern = re.compile(table_pattern)
        table_ids = [tbl for tbl in table_ids if pattern.search(tbl)]

    return table_ids


def check_table_size_and_confirm(
    client: bigquery.Client, table_id: str, skip_confirmation: bool = False
) -> bool:
    """
    Check table size and ask for user confirmation if it exceeds threshold.

    Args:
        client: BigQuery client
        table_id: Full table ID (project.dataset.table)
        skip_confirmation: If True, skip all confirmations

    Returns:
        True if should proceed with copy, False otherwise
    """
    if skip_confirmation:
        return True

    try:
        table = client.get_table(table_id)
        size_gb = table.num_bytes / (1024**3) if table.num_bytes else 0

        if size_gb > SIZE_THRESHOLD_GB:
            print(
                f"\n⚠️  WARNING: Table {table_id} is {size_gb:.2f} GB "
                f"(threshold: {SIZE_THRESHOLD_GB} GB)"
            )
            response = (
                input("   Do you want to copy this table? [y/N]: ").strip().lower()
            )
            return response in ["y", "yes"]

        return True

    except Exception as e:
        print(f"⚠️  Warning: Could not check size for {table_id}: {e}")
        # Proceed by default if we can't check size
        return True


def create_dataset_if_not_exists(
    client: bigquery.Client, project_id: str, dataset_id: str, location: str
) -> None:
    """
    Create a dataset if it doesn't already exist.

    Args:
        client: BigQuery client
        project_id: Target project ID
        dataset_id: Dataset ID to create
        location: BigQuery location (e.g., 'EU', 'US')
    """
    full_dataset_id = f"{project_id}.{dataset_id}"

    try:
        client.get_dataset(full_dataset_id)
        print(f"✅ Dataset {full_dataset_id} already exists")
    except Exception:
        # Dataset doesn't exist, create it
        dataset = bigquery.Dataset(full_dataset_id)
        dataset.location = location
        dataset = client.create_dataset(dataset)
        print(f"✨ Created dataset {full_dataset_id} in {location}")


def submit_copy_job(
    client: bigquery.Client,
    source_table: str,
    destination_table: str,
) -> tuple[str, bigquery.CopyJob | None]:
    """
    Submit a copy job without waiting for completion.

    Args:
        client: BigQuery client
        source_table: Source table ID (project.dataset.table)
        destination_table: Destination table ID (project.dataset.table)

    Returns:
        Tuple of (table_name, copy_job or None)
    """
    try:
        # Check if destination table already exists
        try:
            client.get_table(destination_table)
            return destination_table, None  # Signal: already exists
        except Exception:
            pass  # Table doesn't exist, proceed with copy

        # Submit copy job without waiting
        job_config = bigquery.CopyJobConfig()
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY

        copy_job = client.copy_table(
            source_table,
            destination_table,
            job_config=job_config,
        )

        return destination_table, copy_job

    except Exception as e:
        print(f"❌ Error submitting copy for {destination_table}: {str(e)}")
        return destination_table, None


def wait_for_copy_job(
    client: bigquery.Client,
    destination_table: str,
    copy_job: bigquery.CopyJob,
) -> tuple[bool, str]:
    """
    Wait for a copy job to complete and return status.

    Args:
        client: BigQuery client
        destination_table: Destination table ID
        copy_job: The copy job to wait for

    Returns:
        Tuple of (success: bool, message: str)
    """
    try:
        copy_job.result()

        # Get table info for reporting
        table = client.get_table(destination_table)
        size_mb = table.num_bytes / (1024 * 1024) if table.num_bytes else 0

        return True, f"Copied {table.num_rows:,} rows ({size_mb:.2f} MB)"

    except Exception as e:
        return False, f"Error: {str(e)}"


def main() -> None:
    """
    Main function to copy BigQuery sample datasets to your project.
    Supports regex filtering on datasets and tables.
    """
    # Get default values from environment
    default_dataset_pattern = get_dataset_filter_regex()
    default_table_pattern = get_table_filter_regex()

    parser = argparse.ArgumentParser(
        description="Copy BigQuery public datasets to your project with optional regex filtering.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Copy all tables from bigquery-public-data dataset
  poetry run init_sample_data

  # Copy only tables matching 'shakespeare' or 'natality' from samples dataset
  poetry run init_sample_data --dataset-pattern "^samples$" --table-pattern "(shakespeare|natality)"

  # Copy all tables from datasets starting with 'github'
  poetry run init_sample_data --dataset-pattern "^github"

  # Copy only 'commits' and 'languages' tables from github datasets
  poetry run init_sample_data --dataset-pattern "^github" --table-pattern "(commits|languages)"

  # Skip confirmation for large tables (over 50GB)
  poetry run init_sample_data --skip-confirmation

Environment Variables:
  DATASET_FILTER_REGEX - Default regex pattern for datasets
  TABLE_FILTER_REGEX   - Default regex pattern for tables
        """,
    )

    parser.add_argument(
        "--source-project",
        default="bigquery-public-data",
        help="Source project ID (default: bigquery-public-data)",
    )

    parser.add_argument(
        "--dataset-pattern",
        default=default_dataset_pattern,
        help=f"Regex pattern to filter source datasets (default from .env: {default_dataset_pattern or 'None'})",
    )

    parser.add_argument(
        "--table-pattern",
        default=default_table_pattern,
        help=f"Regex pattern to filter tables within datasets (default from .env: {default_table_pattern or 'None'})",
    )

    parser.add_argument(
        "--skip-confirmation",
        action="store_true",
        help=f"Skip confirmation prompts for tables larger than {SIZE_THRESHOLD_GB}GB",
    )

    args = parser.parse_args()

    start_time = time.time()

    # Build description of what we're filtering
    filter_desc = []
    if args.dataset_pattern:
        filter_desc.append(f"datasets matching '{args.dataset_pattern}'")
    else:
        filter_desc.append("all datasets")

    if args.table_pattern:
        filter_desc.append(f"tables matching '{args.table_pattern}'")
    else:
        filter_desc.append("all tables")

    print(
        f"🚀 Initializing sample data from {args.source_project} ({', '.join(filter_desc)})..."
    )
    print()

    # Get configuration
    project_id = get_billing_project()
    location = get_bq_region()

    print(f"📍 Source project: {args.source_project}")
    print(f"📍 Target project: {project_id}")
    print(f"📍 Location: {location}")
    print()

    # Initialize BigQuery client
    client = bigquery.Client(project=project_id)

    # Get list of datasets
    print("📋 Fetching list of datasets...")
    try:
        source_datasets = get_datasets(
            client, args.source_project, args.dataset_pattern
        )
        print(f"✅ Found {len(source_datasets)} dataset(s) in {args.source_project}")
        for ds in source_datasets:
            print(f"   - {ds}")
        print()
    except Exception as e:
        print(f"❌ Error fetching datasets: {e}")
        sys.exit(1)

    if not source_datasets:
        print("⚠️  No datasets found matching the pattern. Exiting.")
        sys.exit(0)

    # Process each dataset
    total_jobs_map = {}  # destination_table -> (source_dataset, table_name, copy_job)
    total_skip_count = 0

    for source_dataset in source_datasets:
        print(f"📦 Processing dataset: {source_dataset}")
        print()

        # Create destination dataset if it doesn't exist
        create_dataset_if_not_exists(client, project_id, source_dataset, location)

        # Get list of tables from this dataset
        print(f"📋 Fetching tables from {source_dataset}...")
        try:
            source_dataset_full = f"{args.source_project}.{source_dataset}"
            tables = get_sample_tables(client, source_dataset_full, args.table_pattern)
            print(f"✅ Found {len(tables)} table(s) in {source_dataset}")
            print()
        except Exception as e:
            print(f"⚠️  Error fetching tables from {source_dataset}: {e}")
            print()
            continue

        if not tables:
            print(f"⏭️  No tables found in {source_dataset} matching the pattern.")
            print()
            continue

        # Phase 1: Submit all copy jobs in parallel
        print(
            f"📦 Submitting copy jobs for {source_dataset} (jobs run in parallel on BigQuery)..."
        )
        print()

        for table_name in tables:
            source_table = f"{args.source_project}.{source_dataset}.{table_name}"
            destination_table = f"{project_id}.{source_dataset}.{table_name}"

            # Check table size and get confirmation if needed
            if not check_table_size_and_confirm(
                client, source_table, args.skip_confirmation
            ):
                print(
                    f"  ⏭️  {source_dataset}.{table_name} - skipped by user (large table)"
                )
                total_skip_count += 1
                continue

            dest_table, copy_job = submit_copy_job(
                client, source_table, destination_table
            )

            if copy_job is None:
                # Check if it was already skipped
                try:
                    client.get_table(dest_table)
                    print(
                        f"  ⏭️  {source_dataset}.{table_name} - already exists, skipping"
                    )
                    total_skip_count += 1
                except Exception:
                    # It was an error during submission
                    pass
            else:
                total_jobs_map[dest_table] = (source_dataset, table_name, copy_job)
                print(f"  ✓ {source_dataset}.{table_name} - job submitted")

        print()

    if not total_jobs_map:
        print("✨ All tables already exist or no tables to copy. Done!")
        sys.exit(0)

    print(
        f"⏳ Waiting for {len(total_jobs_map)} job(s) to complete in parallel (this may take a few minutes)..."
    )
    print()

    # Phase 2: Wait for all jobs to complete and collect results
    success_count = 0
    error_count = 0

    # Use ThreadPoolExecutor to wait for jobs concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(wait_for_copy_job, client, dest_table, copy_job): (
                source_dataset,
                table_name,
                dest_table,
            )
            for dest_table, (
                source_dataset,
                table_name,
                copy_job,
            ) in total_jobs_map.items()
        }

        for future in as_completed(futures):
            source_dataset, table_name, dest_table = futures[future]
            success, message = future.result()

            if success:
                print(f"  ✅ {source_dataset}.{table_name} - {message}")
                success_count += 1
            else:
                print(f"  ❌ {source_dataset}.{table_name} - {message}")
                error_count += 1

    # Summary
    elapsed_time = time.time() - start_time
    print()
    print("=" * 60)
    print("📊 Summary:")
    print(f"  ✅ Successfully copied: {success_count} table(s)")
    print(f"  ⏭️  Skipped (already exists): {total_skip_count} table(s)")
    print(f"  ❌ Errors: {error_count} table(s)")
    print(f"  ⏱️  Total time: {elapsed_time:.1f} seconds")
    print("=" * 60)

    if error_count > 0:
        print()
        print("⚠️  Some tables failed to copy. Check the errors above.")
        sys.exit(1)
    else:
        print()
        print("✨ Sample data initialization complete!")
        if source_datasets:
            print("   You can now query tables from:")
            for ds in source_datasets:
                print(f"   - {project_id}.{ds}.*")


if __name__ == "__main__":
    main()
