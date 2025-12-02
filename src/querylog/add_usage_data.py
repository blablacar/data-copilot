#!/usr/bin/env python3
"""
Script to add usage.sql files to existing table structure from INFORMATION_SCHEMA.JOBS data.

Queries INFORMATION_SCHEMA.JOBS directly from BigQuery to extract table usage information
and creates usage.sql files in the existing table directory structure:
table_catalog/table_schema/table_name/usage.sql

The script:
1. Queries INFORMATION_SCHEMA.JOBS for job history within the specified lookback period
2. Extracts referenced tables from each query
3. Aggregates usage patterns (top queries per table, total query count)
4. Generates usage.sql files for each table with real query examples
5. Creates a HIGH_USAGE_TABLES.md file listing the most frequently accessed tables
"""

import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from google.cloud import bigquery

from querylog.config import (
    get_billing_project,
    get_bq_region,
    get_dataset_filter_regex,
    get_table_filter_regex,
)


def format_usage_queries(queries: list[str]) -> str:
    """
    Format usage queries for the usage.sql file.

    Args:
        queries: List of SQL queries that use this table

    Returns:
        Formatted string with usage queries as actual SQL
    """
    if not queries:
        return "-- No usage queries found for this table\n"

    usage_lines = []
    usage_lines.append("-- Usage queries for this table")
    usage_lines.append(f"-- Total queries found: {len(queries)}")
    usage_lines.append("")

    for i, query in enumerate(queries, 1):
        usage_lines.append(f"-- Query {i}:")
        usage_lines.append("-- " + "-" * 50)
        usage_lines.append("")

        # Add the actual SQL query (not commented)
        usage_lines.append(query.strip())

        usage_lines.append("")
        usage_lines.append("-- " + "=" * 80)
        usage_lines.append("")

    return "\n".join(usage_lines)


def create_high_usage_tables_list(data: list[dict[Any, Any]]) -> list[dict[str, Any]]:
    """
    Create a list of tables with total_queries >= min_queries.

    Args:
        data: List of table usage records

    Returns:
        List of tables with high usage, sorted by total_queries descending
    """
    high_usage_tables = []

    for record in data:
        if isinstance(record, dict) and "total_queries" in record:
            try:
                total_queries = int(record.get("total_queries", 0))
                table_info = {
                    "table_catalog": record.get("table_catalog", ""),
                    "table_schema": record.get("table_schema", ""),
                    "table_name": record.get("table_name", ""),
                    "total_queries": total_queries,
                    "full_table_name": f"{record.get('table_catalog', '')}.{record.get('table_schema', '')}.{record.get('table_name', '')}",
                }
                high_usage_tables.append(table_info)
            except (ValueError, TypeError):
                continue

    # Sort by total_queries descending, then by full_table_name ascending
    high_usage_tables.sort(key=lambda x: (-x["total_queries"], x["full_table_name"]))
    return high_usage_tables


def save_high_usage_tables_list(
    high_usage_tables: list[dict[str, Any]], output_file: str = "high_usage_tables.md"
) -> None:
    """
    Save the list of high usage tables to a markdown file.

    Args:
        high_usage_tables: List of high usage table records
        output_file: Output file path for the high usage tables list
    """
    try:
        # Group tables by catalog and schema for organized output
        tables_by_catalog = {}
        for table in high_usage_tables:
            catalog = table["table_catalog"]
            schema = table["table_schema"]

            if catalog not in tables_by_catalog:
                tables_by_catalog[catalog] = {}
            if schema not in tables_by_catalog[catalog]:
                tables_by_catalog[catalog][schema] = []

            tables_by_catalog[catalog][schema].append(table)

        # Generate markdown content
        markdown_lines = []
        markdown_lines.append("# High Usage Tables - Query Analysis")
        markdown_lines.append("")

        # Add summary statistics
        if high_usage_tables:
            # Top 200 tables
            markdown_lines.append("## Top 200 Most Used Tables")
            markdown_lines.append("")
            for i, table in enumerate(high_usage_tables[:200], 1):
                markdown_lines.append(
                    f"{i:2d}. **{table['full_table_name']}** - {table['total_queries']} queries"
                )
            markdown_lines.append("")

        # Write to file
        output_file = (
            output_file.replace(".json", ".md")
            if output_file.endswith(".json")
            else output_file
        )
        if not output_file.endswith(".md"):
            output_file += ".md"

        with open(output_file, "w", encoding="utf-8") as f:
            f.write("\n".join(markdown_lines))

        print(f"High usage tables list saved to: {output_file}")
        print(f"Found {len(high_usage_tables)} tables with >= 40 total queries")

    except Exception as e:
        print(f"Error saving high usage tables list: {e}")


def process_usage_data(data: list[dict[str, Any]], tables_dir: str = "tables") -> None:
    """
    Process usage data and create usage.sql files in the table structure.

    Args:
        data: List of table usage records
        tables_dir: Base directory where the table structure exists
    """
    # Validate that data is a list
    if not isinstance(data, list):
        print("Error: Data should be an array of objects")
        return

    print(f"Found {len(data)} table usage records")

    # Create high usage tables list if requested
    high_usage_tables = create_high_usage_tables_list(data)
    # Store in the table directory
    output_file = Path(tables_dir) / "HIGH_USAGE_TABLES.md"
    save_high_usage_tables_list(high_usage_tables, str(output_file))
    print()  # Add blank line for readability

    # Validate base tables directory exists
    base_path = Path(tables_dir)
    if not base_path.exists():
        print(f"Error: Tables directory '{tables_dir}' does not exist")
        print(
            "Please run create_table_structure.py first to create the table structure"
        )
        return

    processed_tables = 0
    skipped_tables = 0
    updated_tables = 0

    # Process each record in the JSON
    for index, record in enumerate(data):
        try:
            # Extract values with validation
            if not isinstance(record, dict):
                print(f"Skipping record {index + 1}: Not a valid object")
                skipped_tables += 1
                continue

            # Required fields
            required_fields = [
                "table_catalog",
                "table_schema",
                "table_name",
            ]
            missing_fields = [field for field in required_fields if field not in record]
            if missing_fields:
                print(
                    f"Skipping record {index + 1}: Missing required fields: {missing_fields}"
                )
                skipped_tables += 1
                continue

            catalog = str(record["table_catalog"]).strip()
            schema = str(record["table_schema"]).strip()
            table_name = str(record["table_name"]).strip()

            # Get queries (could be in top_queries field or similar)
            queries = []
            if "top_queries" in record and record["top_queries"] is not None:
                if isinstance(record["top_queries"], list):
                    queries = [str(q) for q in record["top_queries"] if q is not None]
                else:
                    queries = [str(record["top_queries"])]

            # Get total_queries count if available
            total_queries = record.get("total_queries", len(queries))

            # Check if table directory exists
            table_dir = base_path / catalog / schema / table_name
            if not table_dir.exists():
                print(f"Warning: Table directory does not exist: {table_dir}")
                print(f"Skipping {catalog}.{schema}.{table_name}")
                skipped_tables += 1
                continue

            # Create usage.sql file
            usage_file_path = table_dir / "usage.sql"

            # Check if update is needed
            usage_content = format_usage_queries(queries)

            # Add total queries info to the header
            if total_queries:
                usage_content = usage_content.replace(
                    f"-- Total queries found: {len(queries)}",
                    f"-- Total queries found: {total_queries} (showing top {len(queries)})",
                )

            with open(usage_file_path, "w", encoding="utf-8") as f:
                f.write(usage_content)

            print(
                f"Updated: {usage_file_path} ({len(queries)} queries, {total_queries} total)"
            )
            updated_tables += 1

            processed_tables += 1

        except Exception as e:
            print(f"Error processing record {index + 1}: {e}")
            skipped_tables += 1
            continue

    print("\nCompleted!")
    print(f"Processed: {processed_tables} tables")
    print(f"Updated: {updated_tables} usage files")
    print(f"Skipped: {skipped_tables} records")


def query_information_schema_jobs(
    project_id: str,
    region: str,
    lookback_days: int = 60,
    dataset_filter_regex: str | None = None,
    table_filter_regex: str | None = None,
) -> list[dict[str, Any]]:
    """
    Query INFORMATION_SCHEMA.JOBS to extract table usage information.

    Args:
        project_id: GCP project ID to query
        region: BigQuery region
        lookback_days: Number of days to look back for job history (default: 60)
        dataset_filter_regex: Optional regex pattern to filter datasets
        table_filter_regex: Optional regex pattern to filter tables

    Returns:
        List of table usage records with the same structure as fact_data_usage output
    """
    client = bigquery.Client(project=project_id)

    # Calculate the lookback date
    lookback_date = (datetime.now() - timedelta(days=lookback_days)).strftime(
        "%Y-%m-%d"
    )

    # Build WHERE clause for dataset and table filters if provided
    dataset_filter_clause = ""
    if dataset_filter_regex:
        dataset_filter_clause = (
            f"\n        AND REGEXP_CONTAINS(table_schema, r'{dataset_filter_regex}')"
        )

    table_filter_clause = ""
    if table_filter_regex:
        table_filter_clause = (
            f"\n        AND REGEXP_CONTAINS(table_name, r'{table_filter_regex}')"
        )

    # Query to extract table usage from INFORMATION_SCHEMA.JOBS
    query = f"""
    WITH jobs_with_tables AS (
      SELECT
        job_id,
        user_email,
        creation_time,
        query,
        referenced_tables
      FROM
        `{project_id}`.`region-{region}`.INFORMATION_SCHEMA.JOBS
      WHERE
        creation_time >= '{lookback_date}'
        AND job_type = 'QUERY'
        AND state = 'DONE'
        AND error_result IS NULL
        AND query IS NOT NULL
        AND ARRAY_LENGTH(referenced_tables) > 0
    ),
    tables_list AS (
      SELECT
        table_catalog,
        table_schema,
        table_name
      FROM
        `{project_id}`.`region-{region}`.INFORMATION_SCHEMA.TABLES
      WHERE
        1=1
        {dataset_filter_clause}
        {table_filter_clause}
    ),
    queries_by_table AS (
      SELECT
        ref_table.project_id AS table_catalog,
        ref_table.dataset_id AS table_schema,
        ref_table.table_id AS table_name,
        jobs.query,
        jobs.job_id,
        jobs.user_email,
        jobs.creation_time
      FROM
        jobs_with_tables AS jobs,
        UNNEST(referenced_tables) AS ref_table
      WHERE
        -- Filter out system tables and temp tables
        NOT STARTS_WITH(ref_table.dataset_id, '_')
        AND NOT STARTS_WITH(ref_table.table_id, 'anon')
    )
    SELECT
      tables_list.table_catalog,
      tables_list.table_schema,
      tables_list.table_name,
      ARRAY_AGG(DISTINCT queries_by_table.query IGNORE NULLS ORDER BY queries_by_table.query DESC LIMIT 10) AS top_queries,
      COUNT(DISTINCT queries_by_table.job_id) AS total_queries
    FROM
      tables_list
    JOIN
      queries_by_table
    ON
      queries_by_table.table_catalog = tables_list.table_catalog
      AND queries_by_table.table_schema = tables_list.table_schema
      AND queries_by_table.table_name = tables_list.table_name
    GROUP BY
      tables_list.table_catalog,
      tables_list.table_schema,
      tables_list.table_name
    HAVING
      total_queries > 0
    ORDER BY
      total_queries DESC,
      tables_list.table_catalog,
      tables_list.table_schema,
      tables_list.table_name
    """

    print(f"Querying INFORMATION_SCHEMA.JOBS from {project_id}.region-{region}...")
    print(f"Looking back {lookback_days} days (since {lookback_date})")
    if dataset_filter_regex:
        print(f"Using dataset filter regex: {dataset_filter_regex}")
    if table_filter_regex:
        print(f"Using table filter regex: {table_filter_regex}")

    try:
        query_job = client.query(query)
        results = list(query_job)

        print(
            f"Query completed successfully. Found {len(results)} tables with usage data."
        )

        # Convert results to list of dictionaries
        data = []
        for row in results:
            record = {
                "table_catalog": row.table_catalog,
                "table_schema": row.table_schema,
                "table_name": row.table_name,
                "top_queries": list(row.top_queries) if row.top_queries else [],
                "total_queries": row.total_queries,
            }
            data.append(record)

        return data

    except Exception as e:
        print(f"Error querying INFORMATION_SCHEMA.JOBS: {e}")
        raise


def main():
    """Main function with command line argument parsing."""
    parser = argparse.ArgumentParser(
        description="Add usage.sql files to existing table structure by querying INFORMATION_SCHEMA.JOBS"
    )
    parser.add_argument(
        "--project-id",
        type=str,
        help="GCP project ID to query. If not provided, uses PROJECT_ID from .env",
    )
    parser.add_argument(
        "--region",
        type=str,
        help="BigQuery region (e.g., 'eu', 'us'). If not provided, uses BQ_REGION from .env",
    )
    parser.add_argument(
        "-t",
        "--tables-dir",
        default="tables",
        help="Base directory where the table structure exists (default: tables)",
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=60,
        help="Number of days to look back for job history (default: 60)",
    )
    parser.add_argument(
        "--dataset-filter",
        type=str,
        help="Regex pattern to filter datasets (e.g., '^(staging|prod)_.*'). Overrides DATASET_FILTER_REGEX from .env",
    )
    parser.add_argument(
        "--table-filter",
        type=str,
        help="Regex pattern to filter tables (e.g., '^fact_.*'). Overrides TABLE_FILTER_REGEX from .env",
    )

    args = parser.parse_args()

    try:
        # Get project ID from args or config
        project_id = args.project_id or get_billing_project()
        print(f"Using project ID: {project_id}")

        # Get region from args or config
        region = args.region or get_bq_region().lower()
        print(f"Using BigQuery region: {region}")

        # Get dataset filter regex from arguments or config
        dataset_filter_regex = args.dataset_filter or get_dataset_filter_regex()
        if dataset_filter_regex:
            print(f"Dataset filter regex: {dataset_filter_regex}")

        # Get table filter regex from arguments or config
        table_filter_regex = args.table_filter or get_table_filter_regex()
        if table_filter_regex:
            print(f"Table filter regex: {table_filter_regex}")

        print()  # Blank line for readability

        data = query_information_schema_jobs(
            project_id=project_id,
            region=region,
            lookback_days=args.lookback_days,
            dataset_filter_regex=dataset_filter_regex,
            table_filter_regex=table_filter_regex,
        )

        # Process the data directly
        print("\nProcessing usage data...")
        process_usage_data(data, args.tables_dir)

    except Exception as e:
        print(f"Error: {e}")
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
