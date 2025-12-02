#!/usr/bin/env python3
"""Phase 5 analysis for Wikipedia contribution patterns.
Reads temporary BigQuery table from contribution_overview query, derives
aggregated summaries, and produces visualizations per style guide.
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from google.cloud import bigquery

BASE_PATH = Path(__file__).parent
QUERY_STEM = "contribution_overview"
TMP_FILE = BASE_PATH / f"{QUERY_STEM}.tmp_destination_table.txt"
SAMPLE_FILE = BASE_PATH / f"{QUERY_STEM}.sample.csv"

OUTPUT_SUMMARY_CSV = BASE_PATH / "overview_summary.csv"
OUTPUT_TYPE_CSV = BASE_PATH / "contributor_type_summary.csv"
OUTPUT_YEARLY_CSV = BASE_PATH / "yearly_trend_summary.csv"
OUTPUT_NAMESPACE_CSV = BASE_PATH / "namespace_distribution_summary.csv"
OUTPUT_TOP_CONTRIB_CSV = BASE_PATH / "top_human_contributors_summary.csv"

PLOT_PREFIX = "wikipedia_"

PALETTE = [
    "#044752",
    "#560C3B",
    "#2ED1FF",
    "#F53F5B",
    "#73E5F2",
    "#9EF769",
]

sns.set_theme(style="whitegrid")


def read_tmp_table_id(path: Path) -> str:
    if not TMP_FILE.exists():
        raise FileNotFoundError(f"Temporary table file not found: {TMP_FILE}")
    table_id = TMP_FILE.read_text().strip()
    if not table_id or table_id.startswith("No destination"):
        raise ValueError("Temporary table file is empty or invalid")
    return table_id


def stream_results(table_id: str) -> pd.DataFrame:
    client = bigquery.Client()
    query = f"SELECT * FROM `{table_id}`"
    df = client.query(query).to_dataframe()
    return df


def split_categories(
    df: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    type_df = df[df["metric_category"] == "CONTRIBUTOR_TYPE_SUMMARY"].copy()
    yearly_df = df[df["metric_category"] == "YEARLY_TREND"].copy()
    namespace_df = df[df["metric_category"] == "NAMESPACE_DISTRIBUTION"].copy()
    top_df = df[df["metric_category"] == "TOP_HUMAN_CONTRIBUTORS"].copy()
    return type_df, yearly_df, namespace_df, top_df


def derive_overview(type_df: pd.DataFrame) -> pd.DataFrame:
    # Basic ratios
    out = type_df.copy()
    out["minor_edit_rate"] = out["minor_edits"] / out["total_edits"].replace(0, pd.NA)
    out["avg_chars_per_edit"] = out["avg_article_length"]
    return out


def save_csv(df: pd.DataFrame, path: Path) -> None:
    df.to_csv(path, index=False)


def plot_contributor_type(type_df: pd.DataFrame) -> None:
    plt.figure(figsize=(10, 6))
    ordered = type_df.sort_values("total_edits", ascending=False)
    sns.barplot(
        data=ordered,
        x="dimension_1",
        y="total_edits",
        hue="dimension_1",
        palette=PALETTE[: len(ordered)],
        legend=False,
    )
    plt.title("Total Edits by Contributor Type (2001-2010)")
    plt.xlabel("Contributor Type")
    plt.ylabel("Total Edits")
    plt.xticks(rotation=20)
    plt.tight_layout()
    plt.savefig(BASE_PATH / f"{PLOT_PREFIX}contributor_type.png", dpi=300)
    plt.close()


def plot_yearly_trend(yearly_df: pd.DataFrame) -> None:
    if yearly_df.empty:
        return
    agg = (
        yearly_df.groupby(["year_value", "dimension_1"], dropna=True)["total_edits"]
        .sum()
        .reset_index()
    )
    plt.figure(figsize=(12, 6))
    sns.lineplot(
        data=agg,
        x="year_value",
        y="total_edits",
        hue="dimension_1",
        palette=PALETTE,
        marker="o",
    )
    plt.title("Yearly Edit Volume by Contributor Type")
    plt.xlabel("Year")
    plt.ylabel("Total Edits")
    plt.legend(title="Type")
    plt.tight_layout()
    plt.savefig(BASE_PATH / f"{PLOT_PREFIX}yearly_trend.png", dpi=300)
    plt.close()


def plot_namespace_distribution(ns_df: pd.DataFrame) -> None:
    if ns_df.empty:
        return
    agg = ns_df.groupby(["dimension_2"], dropna=True)["total_edits"].sum().reset_index()
    agg_sorted = agg.sort_values("total_edits", ascending=False)
    plt.figure(figsize=(12, 6))
    sns.barplot(
        data=agg_sorted,
        x="dimension_2",
        y="total_edits",
        hue="dimension_2",
        palette=PALETTE[: len(agg_sorted)],
        legend=False,
    )
    plt.title("Namespace Distribution of Edits")
    plt.xlabel("Namespace Category")
    plt.ylabel("Total Edits")
    plt.xticks(rotation=25, ha="right")
    plt.tight_layout()
    plt.savefig(BASE_PATH / f"{PLOT_PREFIX}namespace_distribution.png", dpi=300)
    plt.close()


def plot_top_humans(top_df: pd.DataFrame) -> None:
    if top_df.empty:
        return
    # Show distribution of total edits among top contributors (histogram)
    plt.figure(figsize=(10, 6))
    sns.histplot(top_df["total_edits"], bins=50, color=PALETTE[0])
    plt.title("Distribution of Total Edits Among Heavy Human Contributors (100+ edits)")
    plt.xlabel("Total Edits (per contributor)")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.savefig(BASE_PATH / f"{PLOT_PREFIX}top_contributor_hist.png", dpi=300)
    plt.close()

    # Show top 20 contributors by rank
    if "contributor_rank" in top_df.columns:
        top_20 = top_df.nsmallest(20, "contributor_rank")
        plt.figure(figsize=(10, 6))
        sns.barplot(
            data=top_20, x="contributor_rank", y="total_edits", color=PALETTE[1]
        )
        plt.title("Top 20 Human Contributors by Edit Volume")
        plt.xlabel("Contributor Rank")
        plt.ylabel("Total Edits")
        plt.tight_layout()
        plt.savefig(BASE_PATH / f"{PLOT_PREFIX}top_20_contributors.png", dpi=300)
        plt.close()


def main() -> None:
    try:
        table_id = read_tmp_table_id(BASE_PATH)
        df = stream_results(table_id)
        if df.empty:
            print("No data returned from temporary table; aborting analysis.")
            return
        type_df, yearly_df, ns_df, top_df = split_categories(df)
        overview_df = derive_overview(type_df)

        save_csv(overview_df, OUTPUT_SUMMARY_CSV)
        save_csv(type_df, OUTPUT_TYPE_CSV)
        save_csv(yearly_df, OUTPUT_YEARLY_CSV)
        save_csv(ns_df, OUTPUT_NAMESPACE_CSV)
        save_csv(top_df, OUTPUT_TOP_CONTRIB_CSV)

        plot_contributor_type(type_df)
        plot_yearly_trend(yearly_df)
        plot_namespace_distribution(ns_df)
        plot_top_humans(top_df)

        print("Analysis outputs generated:")
        for f in [
            OUTPUT_TYPE_CSV,
            OUTPUT_YEARLY_CSV,
            OUTPUT_NAMESPACE_CSV,
            OUTPUT_TOP_CONTRIB_CSV,
        ]:
            print("  -", f.name)
    except Exception as e:
        print(f"Error in analysis: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
