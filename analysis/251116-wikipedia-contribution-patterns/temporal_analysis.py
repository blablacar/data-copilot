#!/usr/bin/env python3
"""
Complete Phase 5 analysis for Wikipedia contribution patterns.
Generates visualizations for temporal patterns (day of week, hour of day, monthly trends)
and saves aggregated CSV summaries.
"""

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from google.cloud import bigquery

BASE_PATH = Path(__file__).parent

PALETTE = ["#044752", "#560C3B", "#2ED1FF", "#F53F5B", "#73E5F2", "#9EF769"]
sns.set_theme(style="whitegrid")


def read_tmp_table_id(query_stem: str) -> str:
    """Read temporary table ID from file."""
    tmp_file = BASE_PATH / f"{query_stem}.tmp_destination_table.txt"
    if not tmp_file.exists():
        raise FileNotFoundError(f"Temporary table file not found: {tmp_file}")
    table_id = tmp_file.read_text().strip()
    if not table_id or table_id.startswith("No destination"):
        raise ValueError(f"Temporary table file is empty or invalid: {tmp_file}")
    return table_id


def stream_results(table_id: str) -> pd.DataFrame:
    """Stream results from BigQuery temporary table."""
    client = bigquery.Client()
    query = f"SELECT * FROM `{table_id}`"
    return client.query(query).to_dataframe()


def plot_day_of_week(df: pd.DataFrame) -> None:
    """Plot edit volume by day of week."""
    dow_df = df[df["pattern_type"] == "DAY_OF_WEEK"].copy()
    if dow_df.empty:
        return

    plt.figure(figsize=(10, 6))
    sns.barplot(
        data=dow_df,
        x="time_label",
        y="total_edits",
        palette=PALETTE,
        order=[
            "Sunday",
            "Monday",
            "Tuesday",
            "Wednesday",
            "Thursday",
            "Friday",
            "Saturday",
        ],
    )
    plt.title("Wikipedia Edits by Day of Week (Human Contributors, 2001-2010)")
    plt.xlabel("Day of Week")
    plt.ylabel("Total Edits")
    plt.xticks(rotation=0)
    plt.tight_layout()
    plt.savefig(BASE_PATH / "wikipedia_day_of_week.png", dpi=300)
    plt.close()

    # Save summary CSV
    dow_df.to_csv(BASE_PATH / "day_of_week_summary.csv", index=False)


def plot_hour_of_day(df: pd.DataFrame) -> None:
    """Plot edit volume by hour of day."""
    hour_df = df[df["pattern_type"] == "HOUR_OF_DAY"].copy()
    if hour_df.empty:
        return

    plt.figure(figsize=(12, 6))
    sns.lineplot(
        data=hour_df,
        x="time_dimension",
        y="total_edits",
        marker="o",
        color=PALETTE[0],
        linewidth=2,
    )
    plt.title("Wikipedia Edits by Hour of Day (UTC, Human Contributors, 2001-2010)")
    plt.xlabel("Hour of Day (0-23)")
    plt.ylabel("Total Edits")
    plt.xticks(range(0, 24))
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(BASE_PATH / "wikipedia_hour_of_day.png", dpi=300)
    plt.close()

    # Save summary CSV
    hour_df.to_csv(BASE_PATH / "hour_of_day_summary.csv", index=False)


def plot_monthly_trend(df: pd.DataFrame) -> None:
    """Plot monthly edit volume trend."""
    monthly_df = df[df["pattern_type"] == "MONTHLY_TREND"].copy()
    if monthly_df.empty:
        return

    # Convert time_label to datetime for better plotting
    monthly_df["date"] = pd.to_datetime(monthly_df["time_label"] + "-01")
    monthly_df = monthly_df.sort_values("date")

    plt.figure(figsize=(14, 6))
    sns.lineplot(
        data=monthly_df,
        x="date",
        y="total_edits",
        marker="o",
        color=PALETTE[1],
        linewidth=2,
    )
    plt.title("Monthly Wikipedia Edit Volume (Human Contributors, 2001-2010)")
    plt.xlabel("Month")
    plt.ylabel("Total Edits")
    plt.xticks(rotation=45)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(BASE_PATH / "wikipedia_monthly_trend.png", dpi=300)
    plt.close()

    # Save summary CSV
    monthly_df.to_csv(BASE_PATH / "monthly_trend_summary.csv", index=False)


def plot_unique_contributors_by_hour(df: pd.DataFrame) -> None:
    """Plot unique contributors by hour of day."""
    hour_df = df[df["pattern_type"] == "HOUR_OF_DAY"].copy()
    if hour_df.empty:
        return

    plt.figure(figsize=(12, 6))
    sns.lineplot(
        data=hour_df,
        x="time_dimension",
        y="unique_contributors",
        marker="o",
        color=PALETTE[2],
        linewidth=2,
    )
    plt.title("Unique Human Contributors by Hour of Day (2001-2010)")
    plt.xlabel("Hour of Day (0-23)")
    plt.ylabel("Unique Contributors")
    plt.xticks(range(0, 24))
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(BASE_PATH / "wikipedia_contributors_by_hour.png", dpi=300)
    plt.close()


def main() -> None:
    """Main execution function."""
    try:
        # Load temporal patterns data
        print("Loading temporal patterns data...")
        temporal_table_id = read_tmp_table_id("temporal_patterns")
        temporal_df = stream_results(temporal_table_id)

        if temporal_df.empty:
            print("No temporal data returned; aborting.")
            return

        print(f"Loaded {len(temporal_df)} temporal pattern rows.")

        # Generate visualizations
        print("Generating temporal pattern visualizations...")
        plot_day_of_week(temporal_df)
        plot_hour_of_day(temporal_df)
        plot_monthly_trend(temporal_df)
        plot_unique_contributors_by_hour(temporal_df)

        print("\n✅ Temporal analysis completed successfully!")
        print("Generated files:")
        print("  - wikipedia_day_of_week.png")
        print("  - wikipedia_hour_of_day.png")
        print("  - wikipedia_monthly_trend.png")
        print("  - wikipedia_contributors_by_hour.png")
        print("  - day_of_week_summary.csv")
        print("  - hour_of_day_summary.csv")
        print("  - monthly_trend_summary.csv")

    except Exception as e:
        print(f"❌ Error in temporal analysis: {e}")
        import traceback

        traceback.print_exc()
        return


if __name__ == "__main__":
    main()
