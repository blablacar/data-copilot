#!/usr/bin/env python3
"""
Configuration management for querylog package.
Loads environment variables from .env file and provides helper functions.
"""

import os
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from .env file in the repository root
# Look for .env file starting from the current file's location
current_file = Path(__file__).resolve()
# Navigate up to find the repository root (where .env should be)
repo_root = current_file.parent.parent.parent
env_file = repo_root / ".env"

if env_file.exists():
    load_dotenv(env_file)
else:
    # Try to load from current directory as fallback
    load_dotenv()


def get_billing_project() -> str:
    """
    Get the BigQuery billing project ID from environment variables.

    Returns:
        Project ID string

    Raises:
        ValueError: If PROJECT_ID is not set in environment
    """
    project_id = os.getenv("PROJECT_ID")
    if not project_id:
        raise ValueError(
            "PROJECT_ID environment variable is not set. "
            "Please create a .env file based on .env.example"
        )
    return project_id


def get_bq_region() -> str:
    """
    Get the BigQuery region from environment variables.

    Returns:
        Region string (default: 'EU' if not set)
    """
    return os.getenv("BQ_REGION", "EU")


def get_dataset_filter_regex() -> str | None:
    """
    Get the dataset filter regex pattern from environment variables.

    Returns:
        Regex pattern string if set, None otherwise
    """
    regex = os.getenv("DATASET_FILTER_REGEX")
    # Return None if empty string or not set
    return regex if regex and regex.strip() else None


def get_table_filter_regex() -> str | None:
    """
    Get the table filter regex pattern from environment variables.

    Returns:
        Regex pattern string if set, None otherwise
    """
    regex = os.getenv("TABLE_FILTER_REGEX")
    # Return None if empty string or not set
    return regex if regex and regex.strip() else None
