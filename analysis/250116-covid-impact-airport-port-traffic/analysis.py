#!/usr/bin/env python3
"""
COVID-19 Impact Analysis: Airport and Port Traffic
Analyzes regional differences in impact and recovery for US airports and ports
"""

from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from google.cloud import bigquery

# Set style for visualizations
sns.set_style("whitegrid")
plt.rcParams["figure.dpi"] = 300
plt.rcParams["savefig.dpi"] = 300
plt.rcParams["font.size"] = 10

# Color palette
PRIMARY = "#044752"
SECONDARY = "#560C3B"
ACCENT_CYAN = "#2ED1FF"
ACCENT_RED = "#F53F5B"
ACCENT_GREEN = "#9EF769"
LIGHT_GRAY = "#E6ECED"


def read_tmp_table_id(base_path: Path, query_name: str) -> str:
    """Read the temporary table ID from the .tmp_destination_table.txt file"""
    tmp_file = base_path / f"{query_name}.tmp_destination_table.txt"
    if not tmp_file.exists():
        raise FileNotFoundError(f"Temporary table file not found: {tmp_file}")

    table_id = tmp_file.read_text().strip()
    if not table_id:
        raise ValueError(f"Temporary table file is empty: {tmp_file}")

    return table_id


def stream_results_from_bigquery(table_id: str) -> pd.DataFrame:
    """Stream results from BigQuery temporary table into a pandas DataFrame"""
    client = bigquery.Client()
    query = f"SELECT * FROM `{table_id}`"
    df = client.query(query).to_dataframe()
    return df


def analyze_airport_data(df_airports: pd.DataFrame) -> dict:
    """Analyze airport traffic data and return key metrics"""

    # Calculate key statistics
    stats = {
        "total_airports": df_airports["airport_name"].nunique(),
        "states": df_airports["state_region"].unique().tolist(),
        "date_range": (
            df_airports.groupby(["year", "month"]).first().index.min(),
            df_airports.groupby(["year", "month"]).first().index.max(),
        ),
    }

    # Find lowest traffic points by airport
    lowest_by_airport = df_airports.loc[
        df_airports.groupby("airport_name")["avg_percent_of_baseline"].idxmin()
    ][
        [
            "airport_name",
            "city",
            "state_region",
            "year",
            "month",
            "avg_percent_of_baseline",
        ]
    ].copy()
    lowest_by_airport = lowest_by_airport.sort_values("avg_percent_of_baseline")

    # Calculate recovery metrics (using data from last 3 months of dataset)
    last_months = df_airports.groupby(["year", "month"]).first().index.max()
    recovery_data = (
        df_airports[
            (df_airports["year"] == last_months[0])
            & (
                df_airports["month"].isin(
                    [last_months[1], last_months[1] - 1, last_months[1] - 2]
                )
            )
        ]
        .groupby(["airport_name", "city", "state_region"])["avg_percent_of_baseline"]
        .mean()
        .reset_index()
    )
    recovery_data.columns = ["airport_name", "city", "state_region", "recovery_level"]
    recovery_data = recovery_data.sort_values("recovery_level", ascending=False)

    stats["lowest_by_airport"] = lowest_by_airport
    stats["recovery_data"] = recovery_data

    return stats


def analyze_port_data(df_ports: pd.DataFrame) -> dict:
    """Analyze port traffic data and return key metrics"""

    # Calculate key statistics
    stats = {
        "total_ports": df_ports["port"].nunique(),
        "cities": df_ports["city"].unique().tolist(),
        "date_range": (
            df_ports.groupby(["year", "month"]).first().index.min(),
            df_ports.groupby(["year", "month"]).first().index.max(),
        ),
    }

    # Find worst decline periods by port
    worst_by_port = df_ports.loc[
        df_ports.groupby("port")["avg_vehicle_change"].idxmin()
    ][["port", "city", "year", "month", "avg_vehicle_change"]].copy()
    worst_by_port = worst_by_port.sort_values("avg_vehicle_change")

    # Calculate average change by port over full period
    avg_by_port = (
        df_ports.groupby(["port", "city"])
        .agg(
            {
                "avg_vehicle_change": "mean",
                "avg_truck_change": "mean",
                "avg_nontruck_change": "mean",
                "avg_wait_time": "mean",
            }
        )
        .reset_index()
    )

    stats["worst_by_port"] = worst_by_port
    stats["avg_by_port"] = avg_by_port

    return stats


def plot_airport_timeline(df_airports: pd.DataFrame, output_path: Path):
    """Create timeline visualization of airport traffic recovery"""

    # Prepare data - create date column
    df_plot = df_airports.copy()
    df_plot["date"] = pd.to_datetime(df_plot[["year", "month"]].assign(day=1))

    # Aggregate by state and month
    state_monthly = (
        df_plot.groupby(["state_region", "date"])["avg_percent_of_baseline"]
        .mean()
        .reset_index()
    )

    # Create figure
    fig, ax = plt.subplots(figsize=(14, 8))

    # Plot each state
    colors = [PRIMARY, ACCENT_RED, ACCENT_CYAN, ACCENT_GREEN, SECONDARY]
    for idx, state in enumerate(sorted(state_monthly["state_region"].unique())):
        state_data = state_monthly[state_monthly["state_region"] == state]
        ax.plot(
            state_data["date"],
            state_data["avg_percent_of_baseline"],
            marker="o",
            linewidth=2.5,
            markersize=6,
            label=state,
            color=colors[idx % len(colors)],
        )

    # Add baseline reference line
    ax.axhline(
        y=100,
        color="gray",
        linestyle="--",
        linewidth=1.5,
        alpha=0.7,
        label="Pre-COVID Baseline",
    )

    # Formatting
    ax.set_xlabel("Date", fontsize=12, fontweight="bold")
    ax.set_ylabel("Traffic (% of Baseline)", fontsize=12, fontweight="bold")
    ax.set_title(
        "US Airport Traffic Recovery by State\n(Baseline: Feb 1 - Mar 15, 2020)",
        fontsize=14,
        fontweight="bold",
        pad=20,
    )
    ax.legend(loc="best", frameon=True, fontsize=10)
    ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path, bbox_inches="tight")
    plt.close()


def plot_airport_impact_comparison(df_airports: pd.DataFrame, output_path: Path):
    """Create bar chart comparing worst impact by airport"""

    # Get lowest point for each airport
    lowest_by_airport = df_airports.loc[
        df_airports.groupby("airport_name")["avg_percent_of_baseline"].idxmin()
    ][["airport_name", "city", "state_region", "avg_percent_of_baseline"]].copy()

    lowest_by_airport = lowest_by_airport.sort_values("avg_percent_of_baseline").head(
        15
    )
    lowest_by_airport["label"] = (
        lowest_by_airport["airport_name"]
        + "\n("
        + lowest_by_airport["city"]
        + ", "
        + lowest_by_airport["state_region"]
        + ")"
    )

    # Create figure
    fig, ax = plt.subplots(figsize=(12, 8))

    bars = ax.barh(
        range(len(lowest_by_airport)),
        lowest_by_airport["avg_percent_of_baseline"],
        color=ACCENT_RED,
        alpha=0.8,
        edgecolor=PRIMARY,
        linewidth=1.5,
    )

    # Add value labels
    for idx, (_bar, value) in enumerate(
        zip(bars, lowest_by_airport["avg_percent_of_baseline"], strict=True)
    ):
        ax.text(
            value + 1, idx, f"{value:.1f}%", va="center", fontsize=9, fontweight="bold"
        )

    ax.set_yticks(range(len(lowest_by_airport)))
    ax.set_yticklabels(lowest_by_airport["label"], fontsize=9)
    ax.set_xlabel(
        "Lowest Traffic Point (% of Baseline)", fontsize=11, fontweight="bold"
    )
    ax.set_title(
        "Airports Most Impacted by COVID-19\n(Lowest Monthly Average Traffic)",
        fontsize=13,
        fontweight="bold",
        pad=20,
    )
    ax.axvline(x=100, color="gray", linestyle="--", linewidth=1.5, alpha=0.7)
    ax.grid(True, axis="x", alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path, bbox_inches="tight")
    plt.close()


def plot_port_timeline(df_ports: pd.DataFrame, output_path: Path):
    """Create timeline visualization of port traffic changes"""

    # Prepare data
    df_plot = df_ports.copy()
    df_plot["date"] = pd.to_datetime(df_plot[["year", "month"]].assign(day=1))

    # Create figure with subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10), sharex=True)

    colors = {"Port of Seattle": PRIMARY, "Port of Tacoma": ACCENT_RED}

    # Plot 1: Vehicle volume changes
    for port in sorted(df_plot["port"].unique()):
        port_data = df_plot[df_plot["port"] == port]
        ax1.plot(
            port_data["date"],
            port_data["avg_vehicle_change"],
            marker="o",
            linewidth=2.5,
            markersize=6,
            label=port,
            color=colors.get(port, ACCENT_CYAN),
        )

    ax1.axhline(
        y=0, color="gray", linestyle="--", linewidth=1.5, alpha=0.7, label="No Change"
    )
    ax1.set_ylabel("Week-over-Week Change (%)", fontsize=11, fontweight="bold")
    ax1.set_title(
        "US Port Traffic: Vehicle Volume Changes",
        fontsize=13,
        fontweight="bold",
        pad=15,
    )
    ax1.legend(loc="best", frameon=True, fontsize=10)
    ax1.grid(True, alpha=0.3)

    # Plot 2: Wait times
    for port in sorted(df_plot["port"].unique()):
        port_data = df_plot[df_plot["port"] == port]
        ax2.plot(
            port_data["date"],
            port_data["avg_wait_time"],
            marker="s",
            linewidth=2.5,
            markersize=6,
            label=port,
            color=colors.get(port, ACCENT_CYAN),
        )

    ax2.set_xlabel("Date", fontsize=11, fontweight="bold")
    ax2.set_ylabel("Average Wait Time (minutes)", fontsize=11, fontweight="bold")
    ax2.set_title(
        "US Port Traffic: Average Wait Times", fontsize=13, fontweight="bold", pad=15
    )
    ax2.legend(loc="best", frameon=True, fontsize=10)
    ax2.grid(True, alpha=0.3)

    plt.tight_layout()
    plt.savefig(output_path, bbox_inches="tight")
    plt.close()


def plot_comparison_airports_vs_ports(
    df_airports: pd.DataFrame, df_ports: pd.DataFrame, output_path: Path
):
    """Create comparison visualization between airports and ports"""

    # Prepare airport data (convert to comparable metric)
    df_air = df_airports.copy()
    df_air["date"] = pd.to_datetime(df_air[["year", "month"]].assign(day=1))
    air_monthly = df_air.groupby("date")["avg_percent_of_baseline"].mean().reset_index()
    air_monthly["deviation_from_baseline"] = (
        air_monthly["avg_percent_of_baseline"] - 100
    )

    # Prepare port data (cumulative impact)
    df_port = df_ports.copy()
    df_port["date"] = pd.to_datetime(df_port[["year", "month"]].assign(day=1))
    port_monthly = df_port.groupby("date")["avg_vehicle_change"].mean().reset_index()

    # Create figure
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 6))

    # Plot 1: Airports
    ax1.fill_between(
        air_monthly["date"],
        0,
        air_monthly["deviation_from_baseline"],
        where=(air_monthly["deviation_from_baseline"] < 0),
        color=ACCENT_RED,
        alpha=0.6,
        label="Below Baseline",
    )
    ax1.fill_between(
        air_monthly["date"],
        0,
        air_monthly["deviation_from_baseline"],
        where=(air_monthly["deviation_from_baseline"] >= 0),
        color=ACCENT_GREEN,
        alpha=0.6,
        label="Above Baseline",
    )
    ax1.plot(
        air_monthly["date"],
        air_monthly["deviation_from_baseline"],
        color=PRIMARY,
        linewidth=2.5,
        marker="o",
        markersize=5,
    )
    ax1.axhline(y=0, color="gray", linestyle="--", linewidth=1.5, alpha=0.7)
    ax1.set_xlabel("Date", fontsize=11, fontweight="bold")
    ax1.set_ylabel("Deviation from Baseline (%)", fontsize=11, fontweight="bold")
    ax1.set_title(
        "Airport Traffic Impact\n(vs Pre-COVID Baseline)",
        fontsize=12,
        fontweight="bold",
        pad=15,
    )
    ax1.legend(loc="best", frameon=True, fontsize=9)
    ax1.grid(True, alpha=0.3)

    # Plot 2: Ports
    ax2.bar(
        port_monthly["date"],
        port_monthly["avg_vehicle_change"],
        color=[
            ACCENT_RED if x < 0 else ACCENT_GREEN
            for x in port_monthly["avg_vehicle_change"]
        ],
        alpha=0.7,
        edgecolor=PRIMARY,
        linewidth=1,
    )
    ax2.axhline(y=0, color="gray", linestyle="--", linewidth=1.5, alpha=0.7)
    ax2.set_xlabel("Date", fontsize=11, fontweight="bold")
    ax2.set_ylabel("Week-over-Week Change (%)", fontsize=11, fontweight="bold")
    ax2.set_title(
        "Port Traffic Impact\n(Week-over-Week Changes)",
        fontsize=12,
        fontweight="bold",
        pad=15,
    )
    ax2.grid(True, alpha=0.3, axis="y")

    plt.tight_layout()
    plt.savefig(output_path, bbox_inches="tight")
    plt.close()


def main():
    """Main analysis function"""

    base_path = Path(__file__).parent

    print("=" * 80)
    print("COVID-19 IMPACT ANALYSIS: US AIRPORT AND PORT TRAFFIC")
    print("=" * 80)
    print()

    # Load airport data
    print("📊 Loading airport traffic data from BigQuery...")
    airport_table_id = read_tmp_table_id(base_path, "airport_traffic_analysis")
    df_airports = stream_results_from_bigquery(airport_table_id)
    print(f"   ✓ Loaded {len(df_airports)} airport records")
    print(
        f"   ✓ Covering {df_airports['airport_name'].nunique()} airports across {df_airports['state_region'].nunique()} states"
    )
    print()

    # Load port data
    print("📊 Loading port traffic data from BigQuery...")
    port_table_id = read_tmp_table_id(base_path, "port_traffic_analysis")
    df_ports = stream_results_from_bigquery(port_table_id)
    print(f"   ✓ Loaded {len(df_ports)} port records")
    print(
        f"   ✓ Covering {df_ports['port'].nunique()} ports in {df_ports['city'].nunique()} cities"
    )
    print()

    # Analyze airport data
    print("🔍 Analyzing airport traffic patterns...")
    airport_stats = analyze_airport_data(df_airports)
    print(f"   ✓ Analyzed {airport_stats['total_airports']} airports")
    print(f"   ✓ States: {', '.join(airport_stats['states'])}")
    print()

    # Analyze port data
    print("🔍 Analyzing port traffic patterns...")
    port_stats = analyze_port_data(df_ports)
    print(f"   ✓ Analyzed {port_stats['total_ports']} ports")
    print(f"   ✓ Cities: {', '.join(port_stats['cities'])}")
    print()

    # Generate visualizations
    print("📈 Generating visualizations...")

    print("   → Creating airport timeline...")
    plot_airport_timeline(df_airports, base_path / "airport_recovery_timeline.png")

    print("   → Creating airport impact comparison...")
    plot_airport_impact_comparison(
        df_airports, base_path / "airport_impact_comparison.png"
    )

    print("   → Creating port timeline...")
    plot_port_timeline(df_ports, base_path / "port_traffic_timeline.png")

    print("   → Creating airports vs ports comparison...")
    plot_comparison_airports_vs_ports(
        df_airports, df_ports, base_path / "airports_vs_ports_comparison.png"
    )

    print("   ✓ All visualizations created")
    print()

    # Save summary statistics
    print("💾 Saving summary statistics...")

    # Airport summary
    airport_summary = airport_stats["lowest_by_airport"].head(10)
    airport_summary.to_csv(base_path / "airport_worst_impact.csv", index=False)

    recovery_summary = airport_stats["recovery_data"].head(10)
    recovery_summary.to_csv(base_path / "airport_recovery_levels.csv", index=False)

    # Port summary
    port_summary = port_stats["worst_by_port"]
    port_summary.to_csv(base_path / "port_worst_periods.csv", index=False)

    port_avg = port_stats["avg_by_port"]
    port_avg.to_csv(base_path / "port_average_metrics.csv", index=False)

    print("   ✓ Summary files saved")
    print()

    # Print key findings
    print("=" * 80)
    print("KEY FINDINGS")
    print("=" * 80)
    print()

    print("🛫 AIRPORTS - Hardest Hit:")
    for _idx, row in airport_stats["lowest_by_airport"].head(5).iterrows():
        print(
            f"   • {row['airport_name']} ({row['city']}, {row['state_region']}): "
            f"{row['avg_percent_of_baseline']:.1f}% of baseline in {row['year']}-{row['month']:02d}"
        )
    print()

    print("🛫 AIRPORTS - Best Recovery (Latest Period):")
    for _idx, row in airport_stats["recovery_data"].head(5).iterrows():
        print(
            f"   • {row['airport_name']} ({row['city']}, {row['state_region']}): "
            f"{row['recovery_level']:.1f}% of baseline"
        )
    print()

    print("⚓ PORTS - Worst Decline Periods:")
    for _idx, row in port_stats["worst_by_port"].iterrows():
        print(
            f"   • {row['port']} ({row['city']}): "
            f"{row['avg_vehicle_change']:.1f}% change in {row['year']}-{row['month']:02d}"
        )
    print()

    print("⚓ PORTS - Overall Average Metrics:")
    for _idx, row in port_stats["avg_by_port"].iterrows():
        print(f"   • {row['port']} ({row['city']}):")
        print(f"      - Vehicle change: {row['avg_vehicle_change']:.1f}%")
        print(f"      - Truck change: {row['avg_truck_change']:.1f}%")
        print(f"      - Wait time: {row['avg_wait_time']:.1f} minutes")
    print()

    print("=" * 80)
    print("✅ Analysis complete! Check the generated PNG files and CSV summaries.")
    print("=" * 80)


if __name__ == "__main__":
    main()
