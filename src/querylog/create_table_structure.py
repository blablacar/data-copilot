#!/usr/bin/env python3
"""
Script to create directory structure from BigQuery table metadata.
Retrieves table information directly from BigQuery INFORMATION_SCHEMA and creates a file arborescence with:
table_catalog/table_schema/table_name/ddl.sql
table_catalog/table_schema/table_name/preview.sql

The content of ddl.sql is fetched from BigQuery INFORMATION_SCHEMA.
The content of preview.sql is sample data from BigQuery preview API.
"""

import argparse
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
from google.cloud import bigquery

from querylog.config import (
    get_billing_project,
    get_bq_region,
    get_dataset_filter_regex,
    get_table_filter_regex,
)


def fetch_tables_from_bigquery(
    client: bigquery.Client,
    project_id: str,
    dataset_filter_regex: str | None = None,
    table_filter_regex: str | None = None,
) -> pd.DataFrame:
    """
    Fetch table metadata from BigQuery INFORMATION_SCHEMA.

    Args:
        client: BigQuery client
        project_id: Project ID to fetch tables from
        dataset_filter_regex: Optional regex pattern to filter datasets
        table_filter_regex: Optional regex pattern to filter tables

    Returns:
        DataFrame with columns: table_catalog, table_schema, table_name, wildcard_table_name, ddl
    """
    # Get region from config
    location = get_bq_region()
    region = f"region-{location.lower()}"

    # Build WHERE clause for dataset and table filters if provided
    where_conditions = []
    if dataset_filter_regex:
        where_conditions.append(
            f"REGEXP_CONTAINS(table_schema, r'{dataset_filter_regex}')"
        )
    if table_filter_regex:
        where_conditions.append(f"REGEXP_CONTAINS(table_name, r'{table_filter_regex}')")

    where_clause = ""
    if where_conditions:
        where_clause = "WHERE " + " AND ".join(where_conditions)

    query = f"""
    SELECT
        table_catalog,
        table_schema,
        table_name,
        ddl
    FROM
        `{project_id}.{region}.INFORMATION_SCHEMA.TABLES`
    {where_clause}
    ORDER BY
        table_catalog, table_schema, table_name
    """

    print("Fetching table metadata from BigQuery INFORMATION_SCHEMA...")
    if dataset_filter_regex:
        print(f"Using dataset filter regex: {dataset_filter_regex}")
    if table_filter_regex:
        print(f"Using table filter regex: {table_filter_regex}")

    try:
        df = client.query(query).to_dataframe()
        print(f"Successfully fetched {len(df)} tables from BigQuery")

        # Add wildcard_table_name column (same as table_name for non-wildcard tables)
        df["wildcard_table_name"] = df["table_name"]

        return df
    except Exception as e:
        print(f"Error fetching tables from BigQuery: {e}")
        raise


def _format_preview_value(value, field_type: str, max_length: int = 100) -> str:
    """
    Format a value for preview display, truncating large objects like GEOGRAPHY.

    Args:
        value: The value to format
        field_type: The BigQuery field type (e.g., 'GEOGRAPHY', 'STRING', 'INTEGER')
        max_length: Maximum length for string representation

    Returns:
        Formatted string representation of the value
    """
    if value is None:
        return "NULL"

    # Convert to string
    str_value = str(value)

    # Check if this is a geospatial type or a very large object
    if field_type in ("GEOGRAPHY", "GEOMETRY") or len(str_value) > max_length:
        # Truncate and add indicator
        truncated = str_value[:max_length]
        return f"{truncated}... [TRUNCATED]"

    return str_value


def get_table_preview(
    client: bigquery.Client, table_id: str, max_results: int = 10
) -> str:
    """
    Get a preview of table data using BigQuery API.

    Args:
        client: BigQuery client
        table_id: Full table ID (project.dataset.table)
        max_results: Maximum number of rows to preview

    Returns:
        Formatted string with table preview data
    """
    try:
        # Get table schema
        table = client.get_table(table_id)

        # Get preview rows
        rows_iter = client.list_rows(table_id, max_results=max_results)
        rows = list(rows_iter)

        if not rows:
            return "-- No data available in this table\n"

        # Create formatted output
        preview_lines = []
        preview_lines.append(f"-- Preview of table: {table_id}")
        preview_lines.append(f"-- Total rows in table: {table.num_rows}")
        preview_lines.append(f"-- Showing first {len(rows)} rows")
        preview_lines.append("")

        # Get column names and types
        field_names = [field.name for field in rows_iter.schema]
        field_types = [field.field_type for field in rows_iter.schema]

        # Create header
        format_string = "{!s:<20} " * len(field_names)
        preview_lines.append("-- " + format_string.format(*field_names))
        preview_lines.append("-- " + "-" * (20 * len(field_names)))

        # Add data rows
        for row in rows:
            # Convert row values to strings, handling None values and truncating large objects
            row_values = [
                _format_preview_value(value, field_type)
                for value, field_type in zip(row, field_types, strict=True)
            ]
            preview_lines.append("-- " + format_string.format(*row_values))

        return "\n".join(preview_lines) + "\n"

    except Exception as e:
        return f"-- Error retrieving preview for {table_id}: {str(e)}\n"


def should_update_preview(
    ddl_file_path: Path, preview_file_path: Path, current_ddl: str
) -> bool:
    """
    Determine if the preview should be updated based on:
    1. preview.sql file is missing
    2. preview.sql file starts with "-- Error retrieving preview"
    3. ddl.sql file content is different from the current DDL

    Args:
        ddl_file_path: Path to the ddl.sql file
        preview_file_path: Path to the preview.sql file
        current_ddl: Current DDL content from CSV

    Returns:
        True if preview should be updated, False otherwise
    """
    # Check if preview.sql is missing
    if not preview_file_path.exists():
        return True

    # Check if preview.sql starts with error message
    try:
        with open(preview_file_path, encoding="utf-8") as f:
            preview_content = f.read().strip()
            if preview_content.startswith("-- Error retrieving preview"):
                return True
    except Exception:
        # If we can't read the preview file, regenerate it
        return True

    # Check if ddl.sql content is different from current DDL
    if ddl_file_path.exists():
        try:
            with open(ddl_file_path, encoding="utf-8") as f:
                existing_ddl = f.read().strip()
                if existing_ddl != current_ddl.strip():
                    return True
        except Exception:
            # If we can't read the DDL file, we'll recreate it anyway
            return True

    return False


def extract_table_description(ddl: str) -> str:
    """
    Extract table description from DDL by looking for table-level comments
    or the first column description if no table description is found.

    Args:
        ddl: The DDL string for the table

    Returns:
        A description string for the table
    """
    try:
        # Look for table-level description in OPTIONS
        table_desc_match = re.search(
            r'CREATE TABLE.*?OPTIONS\s*\(\s*description\s*=\s*"([^"]*)"',
            ddl,
            re.DOTALL | re.IGNORECASE,
        )
        if table_desc_match:
            return table_desc_match.group(1).strip()

        return "No description available"

    except Exception:
        return "No description available"


def fetch_and_save_preview(
    bigquery_client: bigquery.Client,
    table_id: str,
    preview_file_path: Path,
    max_preview_rows: int,
) -> tuple[str, str]:
    """
    Fetch preview data from BigQuery and save it to a file.

    Args:
        bigquery_client: BigQuery client
        table_id: Full table ID (project.dataset.table)
        preview_file_path: Path where the preview.sql file should be saved
        max_preview_rows: Maximum number of rows to include in preview

    Returns:
        Tuple of (table_id, status_message)
    """
    try:
        preview_content = get_table_preview(bigquery_client, table_id, max_preview_rows)
        with open(preview_file_path, "w", encoding="utf-8") as f:
            f.write(preview_content)
        return (table_id, f"Created: {preview_file_path}")
    except Exception as e:
        return (table_id, f"Error fetching preview for {table_id}: {str(e)}")


def create_table_structure(
    output_dir: str = "tables",
    enable_preview: bool = True,
    max_preview_rows: int = 10,
    dataset_filter_regex: str | None = None,
    table_filter_regex: str | None = None,
) -> None:
    """
    Create directory structure from BigQuery table metadata.

    Args:
        output_dir: Base directory where the structure will be created
        enable_preview: Whether to fetch preview data from BigQuery
        max_preview_rows: Maximum number of rows to include in preview
        dataset_filter_regex: Optional regex pattern to filter datasets
        table_filter_regex: Optional regex pattern to filter tables
    """
    # Initialize BigQuery client
    try:
        billing_project = get_billing_project()
        location = get_bq_region()
        bigquery_client = bigquery.Client(project=billing_project, location=location)
        print(
            f"BigQuery client initialized successfully (Project: {billing_project}, Location: {location})"
        )
    except Exception as e:
        print(f"Error: Could not initialize BigQuery client: {e}")
        raise

    # Fetch tables from BigQuery
    try:
        df = fetch_tables_from_bigquery(
            bigquery_client, billing_project, dataset_filter_regex, table_filter_regex
        )
    except Exception as e:
        print(f"Failed to fetch tables from BigQuery: {e}")
        return

    if df.empty:
        print("No tables found matching the specified criteria")
        return

    # Validate required columns
    required_columns = [
        "table_catalog",
        "table_schema",
        "table_name",
        "wildcard_table_name",
        "ddl",
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"Error: Missing required columns: {missing_columns}")
        return

    # Create base output directory
    base_path = Path(output_dir)
    base_path.mkdir(exist_ok=True)

    created_tables = 0
    preview_tasks = []  # Store (table_id, preview_file_path) tuples for parallel execution

    # Process each row in the DataFrame
    for index, row in df.iterrows():
        try:
            # Extract values
            catalog = str(row["table_catalog"]).strip()
            schema = str(row["table_schema"]).strip()
            table_name = str(row["table_name"]).strip()
            wildcard_table_name = str(row["wildcard_table_name"]).strip()
            ddl = str(row["ddl"]).strip()

            # Skip rows with missing data
            if (
                pd.isna(row["table_catalog"])
                or pd.isna(row["table_schema"])
                or pd.isna(row["table_name"])
                or pd.isna(row["wildcard_table_name"])
                or pd.isna(row["ddl"])
            ):
                print(f"Skipping row {index + 1}: Missing data")
                continue

            # Create directory structure: catalog/schema/wildcard_table_name/
            table_dir = base_path / catalog / schema / wildcard_table_name
            table_dir.mkdir(parents=True, exist_ok=True)

            # Create ddl.sql file
            ddl_file_path = table_dir / "ddl.sql"
            with open(ddl_file_path, "w", encoding="utf-8") as f:
                f.write(ddl)

            print(f"Created: {ddl_file_path}")

            # Queue preview fetching for parallel execution if enabled
            if enable_preview:
                preview_file_path = table_dir / "preview.sql"
                table_id = f"{catalog}.{schema}.{table_name}"

                # Check if preview needs to be updated
                if should_update_preview(ddl_file_path, preview_file_path, ddl):
                    preview_tasks.append((table_id, preview_file_path))
                else:
                    print(f"Preview already up-to-date for {table_id}, skipping...")

            created_tables += 1

        except Exception as e:
            print(f"Error processing row {index + 1}: {e}")
            continue

    # Execute preview fetching in parallel
    if preview_tasks and enable_preview:
        print(f"\nFetching {len(preview_tasks)} table previews in parallel...")
        with ThreadPoolExecutor(max_workers=4) as executor:
            # Submit all preview tasks
            future_to_table = {
                executor.submit(
                    fetch_and_save_preview,
                    bigquery_client,
                    table_id,
                    preview_file_path,
                    max_preview_rows,
                ): table_id
                for table_id, preview_file_path in preview_tasks
            }

            # Process completed tasks as they finish
            for future in as_completed(future_to_table):
                table_id, message = future.result()
                print(message)
    print(
        f"\nCompleted! Created {created_tables} table directories in '{output_dir}' directory."
    )


def main():
    """Main function with command line argument parsing."""
    parser = argparse.ArgumentParser(
        description="Create directory structure from BigQuery table metadata"
    )
    parser.add_argument(
        "-o",
        "--output",
        default="tables",
        help="Output directory for the table structure (default: tables)",
    )
    parser.add_argument(
        "--skip-preview",
        action="store_true",
        help="Skip fetching BigQuery preview data for tables",
    )
    parser.add_argument(
        "--max-preview-rows",
        type=int,
        default=10,
        help="Maximum number of rows to include in preview (default: 10)",
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

    # Handle preview options
    enable_preview = not args.skip_preview

    if not enable_preview:
        print("Preview functionality disabled - skipping BigQuery data fetching")

    # Get dataset filter regex from arguments or config
    dataset_filter_regex = args.dataset_filter or get_dataset_filter_regex()
    if dataset_filter_regex:
        print(f"Using dataset filter regex: {dataset_filter_regex}")

    # Get table filter regex from arguments or config
    table_filter_regex = args.table_filter or get_table_filter_regex()
    if table_filter_regex:
        print(f"Using table filter regex: {table_filter_regex}")

    create_table_structure(
        args.output,
        enable_preview,
        args.max_preview_rows,
        dataset_filter_regex,
        table_filter_regex,
    )
    return 0


if __name__ == "__main__":
    exit(main())
