"""Generate interactive HTML chart for WRDS CRSP Compustat."""

import pandas as pd
import plotly.express as px
import os
from pathlib import Path

# Get the project root (one level up from src/)
PROJECT_ROOT = Path(__file__).parent.parent
DATA_DIR = PROJECT_ROOT / "_data"
OUTPUT_DIR = PROJECT_ROOT / "_output"


def generate_crsp_returns_chart():
    """Generate CRSP monthly stock returns time series chart."""
    # Load CRSP monthly stock returns data
    df = pd.read_parquet(DATA_DIR / "ftsfr_CRSP_monthly_stock_ret.parquet")

    # Get a subset of unique_ids for cleaner visualization
    unique_ids = df['unique_id'].unique()[:5]
    df_subset = df[df['unique_id'].isin(unique_ids)]

    # Create line chart
    fig = px.line(
        df_subset.sort_values("ds"),
        x="ds",
        y="y",
        color="unique_id",
        title="CRSP Monthly Stock Returns (Fama-French 1993)",
        labels={
            "ds": "Date",
            "y": "Return",
            "unique_id": "Series"
        }
    )

    # Update layout
    fig.update_layout(
        template="plotly_white",
        hovermode="x unified"
    )

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Save chart
    output_path = OUTPUT_DIR / "crsp_returns_replication.html"
    fig.write_html(str(output_path))
    print(f"Chart saved to {output_path}")

    return fig


def generate_crsp_cumulative_returns_chart():
    """Generate CRSP monthly stock cumulative returns time series chart."""
    # Load CRSP monthly stock returns data
    df = pd.read_parquet(DATA_DIR / "ftsfr_CRSP_monthly_stock_ret.parquet")

    # Get the same subset of unique_ids as the returns chart (first 5)
    unique_ids = df['unique_id'].unique()[:5]
    df_subset = df[df['unique_id'].isin(unique_ids)]

    # Calculate cumulative returns
    df_subset = df_subset.sort_values(['unique_id', 'ds'])
    df_subset['cumulative'] = df_subset.groupby('unique_id')['y'].transform(
        lambda x: (1 + x).cumprod()
    )

    # Create line chart
    fig = px.line(
        df_subset,
        x="ds",
        y="cumulative",
        color="unique_id",
        title="CRSP Monthly Stock Cumulative Returns (Fama-French 1993)",
        labels={
            "ds": "Date",
            "cumulative": "Cumulative Return (Growth of $1)",
            "unique_id": "Series"
        }
    )

    # Update layout
    fig.update_layout(
        template="plotly_white",
        hovermode="x unified",
        yaxis_type="log"
    )

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Save chart
    output_path = OUTPUT_DIR / "crsp_cumulative_returns.html"
    fig.write_html(str(output_path))
    print(f"Chart saved to {output_path}")

    return fig


if __name__ == "__main__":
    generate_crsp_returns_chart()
    generate_crsp_cumulative_returns_chart()
