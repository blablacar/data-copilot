#!/usr/bin/env python3
"""
COVID-19 Impact Analysis on US Commercial Traffic by Industry
Generates visualizations and statistics showing drop and recovery patterns
"""

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from google.cloud import bigquery

# Color palette (from style guide)
COLORS = {
    "primary": "#044752",
    "secondary": "#560C3B",
    "accent_cyan": "#2ED1FF",
    "accent_red": "#F53F5B",
    "accent_light_cyan": "#73E5F2",
    "accent_lime": "#9EF769",
    "neutral_light_gray": "#E6ECED",
    "neutral_light_blue": "#ABE5FF",
    "neutral_pale_cyan": "#C7F5FA",
    "neutral_pale_green": "#D7FBC2",
}


def read_tmp_table_id(base_path: Path, query_name: str) -> str:
    """Read the temporary table ID from the .tmp_destination_table.txt file"""
    tmp_file = base_path / f"{query_name}.tmp_destination_table.txt"
    if not tmp_file.exists():
        raise FileNotFoundError(f"Temporary table file not found: {tmp_file}")

    table_id = tmp_file.read_text().strip()
    if not table_id:
        raise ValueError("Temporary table file is empty")

    return table_id


def stream_results_from_bigquery(table_id: str) -> pd.DataFrame:
    """Stream results from BigQuery temporary table into a pandas DataFrame"""
    client = bigquery.Client()
    query = f"SELECT * FROM `{table_id}`"
    df = client.query(query).to_dataframe()
    return df


def calculate_statistics(df: pd.DataFrame) -> dict:
    """Calculate key statistics from the data"""
    stats = {}

    # Convert date to datetime
    df["date"] = pd.to_datetime(df["date"])

    # Overall statistics
    baseline_period = df[df["date"] < "2020-03-15"]
    covid_peak = df[(df["date"] >= "2020-03-15") & (df["date"] <= "2020-06-30")]
    recovery_2021 = df[(df["date"] >= "2021-01-01") & (df["date"] <= "2021-12-31")]
    recovery_2022 = df[(df["date"] >= "2022-01-01") & (df["date"] <= "2022-12-31")]

    # Industry-level metrics
    industry_stats = []
    for industry in df["industry"].unique():
        ind_data = df[df["industry"] == industry]

        baseline_avg = ind_data[ind_data["date"] < "2020-03-15"][
            "percent_of_baseline"
        ].mean()
        peak_avg = ind_data[
            (ind_data["date"] >= "2020-03-15") & (ind_data["date"] <= "2020-06-30")
        ]["percent_of_baseline"].mean()
        recovery_2021_avg = ind_data[
            (ind_data["date"] >= "2021-01-01") & (ind_data["date"] <= "2021-12-31")
        ]["percent_of_baseline"].mean()
        recovery_2022_avg = ind_data[
            (ind_data["date"] >= "2022-01-01") & (ind_data["date"] <= "2022-12-31")
        ]["percent_of_baseline"].mean()

        drop = (
            peak_avg - baseline_avg
            if not pd.isna(baseline_avg) and not pd.isna(peak_avg)
            else None
        )

        industry_stats.append(
            {
                "industry": industry,
                "baseline_avg": baseline_avg,
                "peak_drop_avg": peak_avg,
                "drop_percentage": drop,
                "recovery_2021_avg": recovery_2021_avg,
                "recovery_2022_avg": recovery_2022_avg,
            }
        )

    stats["industry_stats"] = pd.DataFrame(industry_stats)
    stats["overall_baseline"] = baseline_period["percent_of_baseline"].mean()
    stats["overall_peak"] = covid_peak["percent_of_baseline"].mean()
    stats["overall_2021"] = recovery_2021["percent_of_baseline"].mean()
    stats["overall_2022"] = recovery_2022["percent_of_baseline"].mean()

    return stats


def create_timeline_visualization(df: pd.DataFrame, output_path: Path):
    """Create timeline showing commercial traffic by industry over time"""
    df["date"] = pd.to_datetime(df["date"])

    # Calculate monthly averages for cleaner visualization
    df["year_month"] = df["date"].dt.to_period("M")
    monthly_data = (
        df.groupby(["year_month", "industry"])["percent_of_baseline"]
        .mean()
        .reset_index()
    )
    monthly_data["date"] = monthly_data["year_month"].dt.to_timestamp()

    # Create figure
    fig, ax = plt.subplots(figsize=(16, 10))

    # Get top industries by data availability
    top_industries = df.groupby("industry").size().nlargest(10).index

    # Plot each industry
    colors_list = [
        COLORS["primary"],
        COLORS["secondary"],
        COLORS["accent_cyan"],
        COLORS["accent_red"],
        COLORS["accent_light_cyan"],
        COLORS["accent_lime"],
        "#FF6B6B",
        "#4ECDC4",
        "#45B7D1",
        "#FFA07A",
    ]

    for idx, industry in enumerate(top_industries):
        industry_data = monthly_data[monthly_data["industry"] == industry]
        ax.plot(
            industry_data["date"],
            industry_data["percent_of_baseline"],
            label=industry,
            linewidth=2,
            color=colors_list[idx % len(colors_list)],
        )

    # Add baseline line at 100%
    ax.axhline(
        y=100,
        color="black",
        linestyle="--",
        linewidth=1.5,
        alpha=0.7,
        label="Baseline (100%)",
    )

    # Add COVID start marker
    ax.axvline(
        x=pd.Timestamp("2020-03-15"),
        color="red",
        linestyle=":",
        linewidth=2,
        alpha=0.5,
        label="COVID-19 Start",
    )

    ax.set_xlabel("Date", fontsize=12, fontweight="bold")
    ax.set_ylabel("Percent of Baseline Activity (%)", fontsize=12, fontweight="bold")
    ax.set_title(
        "US Commercial Traffic by Industry: COVID-19 Impact and Recovery Timeline",
        fontsize=14,
        fontweight="bold",
        pad=20,
    )
    ax.legend(bbox_to_anchor=(1.05, 1), loc="upper left", fontsize=9)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(
        output_path / "commercial_traffic_timeline.png", dpi=300, bbox_inches="tight"
    )
    plt.close()


def create_drop_comparison(stats: dict, output_path: Path):
    """Create bar chart comparing the impact drop across industries"""
    industry_stats = stats["industry_stats"].copy()
    industry_stats = industry_stats.dropna(subset=["drop_percentage"])
    industry_stats = industry_stats.sort_values("drop_percentage")

    fig, ax = plt.subplots(figsize=(12, 10))

    # Create color map based on drop severity
    colors = [
        COLORS["accent_red"] if x < 0 else COLORS["accent_lime"]
        for x in industry_stats["drop_percentage"]
    ]

    bars = ax.barh(
        industry_stats["industry"],
        industry_stats["drop_percentage"],
        color=colors,
        alpha=0.8,
    )

    # Add value labels
    for bar, value in zip(bars, industry_stats["drop_percentage"], strict=True):
        ax.text(
            value + 1 if value > 0 else value - 1,
            bar.get_y() + bar.get_height() / 2,
            f"{value:.1f}%",
            va="center",
            ha="left" if value > 0 else "right",
            fontsize=9,
        )

    ax.axvline(x=0, color="black", linestyle="-", linewidth=1.5)
    ax.set_xlabel(
        "Change from Baseline (Percentage Points)", fontsize=12, fontweight="bold"
    )
    ax.set_ylabel("Industry", fontsize=12, fontweight="bold")
    ax.set_title(
        "COVID-19 Peak Impact by Industry (March-June 2020)\nChange from Pre-COVID Baseline",
        fontsize=14,
        fontweight="bold",
        pad=20,
    )
    ax.grid(True, alpha=0.3, axis="x")

    plt.tight_layout()
    plt.savefig(
        output_path / "industry_impact_comparison.png", dpi=300, bbox_inches="tight"
    )
    plt.close()


def create_recovery_analysis(stats: dict, output_path: Path):
    """Create visualization showing recovery patterns across industries"""
    industry_stats = stats["industry_stats"].copy()
    industry_stats = industry_stats.dropna(
        subset=[
            "baseline_avg",
            "peak_drop_avg",
            "recovery_2021_avg",
            "recovery_2022_avg",
        ]
    )
    industry_stats = industry_stats.sort_values("recovery_2022_avg", ascending=False)

    # Prepare data for grouped bar chart
    x = np.arange(len(industry_stats))
    width = 0.2

    fig, ax = plt.subplots(figsize=(16, 10))

    ax.bar(
        x - 1.5 * width,
        industry_stats["baseline_avg"],
        width,
        label="Pre-COVID Baseline",
        color=COLORS["neutral_light_gray"],
        alpha=0.9,
    )
    ax.bar(
        x - 0.5 * width,
        industry_stats["peak_drop_avg"],
        width,
        label="COVID Peak (Mar-Jun 2020)",
        color=COLORS["accent_red"],
        alpha=0.9,
    )
    ax.bar(
        x + 0.5 * width,
        industry_stats["recovery_2021_avg"],
        width,
        label="2021 Average",
        color=COLORS["accent_cyan"],
        alpha=0.9,
    )
    ax.bar(
        x + 1.5 * width,
        industry_stats["recovery_2022_avg"],
        width,
        label="2022 Average",
        color=COLORS["accent_lime"],
        alpha=0.9,
    )

    ax.axhline(
        y=100,
        color="black",
        linestyle="--",
        linewidth=1.5,
        alpha=0.5,
        label="Baseline Level (100%)",
    )

    ax.set_xlabel("Industry", fontsize=12, fontweight="bold")
    ax.set_ylabel("Percent of Baseline Activity (%)", fontsize=12, fontweight="bold")
    ax.set_title(
        "Commercial Traffic Recovery by Industry: Comparison Across Time Periods",
        fontsize=14,
        fontweight="bold",
        pad=20,
    )
    ax.set_xticks(x)
    ax.set_xticklabels(industry_stats["industry"], rotation=45, ha="right", fontsize=9)
    ax.legend(fontsize=10)
    ax.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    plt.savefig(
        output_path / "industry_recovery_comparison.png", dpi=300, bbox_inches="tight"
    )
    plt.close()


def save_statistics_tables(stats: dict, output_path: Path):
    """Save statistical summaries as CSV files"""
    # Industry statistics
    industry_stats = stats["industry_stats"].copy()
    industry_stats = industry_stats.sort_values("drop_percentage")
    industry_stats.to_csv(
        output_path / "industry_statistics.csv", index=False, float_format="%.2f"
    )

    # Most impacted industries
    worst_impacted = industry_stats.nsmallest(10, "drop_percentage")[
        ["industry", "baseline_avg", "peak_drop_avg", "drop_percentage"]
    ]
    worst_impacted.to_csv(
        output_path / "worst_impacted_industries.csv", index=False, float_format="%.2f"
    )

    # Best recovery industries
    industry_stats["recovery_improvement"] = (
        industry_stats["recovery_2022_avg"] - industry_stats["peak_drop_avg"]
    )
    best_recovery = industry_stats.nlargest(10, "recovery_improvement")[
        ["industry", "peak_drop_avg", "recovery_2022_avg", "recovery_improvement"]
    ]
    best_recovery.to_csv(
        output_path / "best_recovery_industries.csv", index=False, float_format="%.2f"
    )

    # Overall summary
    summary = pd.DataFrame(
        {
            "Period": [
                "Pre-COVID Baseline",
                "COVID Peak (Mar-Jun 2020)",
                "2021 Average",
                "2022 Average",
            ],
            "Average_Activity_Level": [
                stats["overall_baseline"],
                stats["overall_peak"],
                stats["overall_2021"],
                stats["overall_2022"],
            ],
        }
    )
    summary.to_csv(
        output_path / "overall_summary.csv", index=False, float_format="%.2f"
    )


def main():
    """Main execution function"""
    base_path = Path(__file__).parent

    print("📊 Starting COVID-19 commercial traffic analysis...")

    # Read temporary table ID
    print("📥 Reading temporary table ID...")
    table_id = read_tmp_table_id(base_path, "commercial_traffic_by_industry")
    print(f"   Table ID: {table_id}")

    # Stream data from BigQuery
    print("🔄 Streaming data from BigQuery...")
    df = stream_results_from_bigquery(table_id)
    print(f"   Loaded {len(df):,} rows")
    print(f"   Industries: {df['industry'].nunique()}")
    print(f"   Date range: {df['date'].min()} to {df['date'].max()}")

    # Calculate statistics
    print("📈 Calculating statistics...")
    stats = calculate_statistics(df)

    # Create visualizations
    print("🎨 Creating visualizations...")
    print("   - Timeline visualization...")
    create_timeline_visualization(df, base_path)
    print("   - Impact comparison...")
    create_drop_comparison(stats, base_path)
    print("   - Recovery analysis...")
    create_recovery_analysis(stats, base_path)

    # Save statistics tables
    print("💾 Saving statistics tables...")
    save_statistics_tables(stats, base_path)

    print("✅ Analysis complete!")
    print(f"   Output directory: {base_path}")


if __name__ == "__main__":
    main()
