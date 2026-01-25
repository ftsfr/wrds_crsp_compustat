"""
dodo.py - Doit build automation for WRDS CRSP/Compustat pipeline

Run with: doit
"""

import platform
import subprocess
import sys
from pathlib import Path

import chartbook

sys.path.insert(1, "./src/")

BASE_DIR = chartbook.env.get_project_root()
DATA_DIR = BASE_DIR / "_data"
OUTPUT_DIR = BASE_DIR / "_output"
OS_TYPE = "nix" if platform.system() != "Windows" else "windows"


def jupyter_execute_notebook(notebook):
    """Execute a notebook and save the output in-place."""
    subprocess.run(
        [
            "jupyter",
            "nbconvert",
            "--to",
            "notebook",
            "--execute",
            "--inplace",
            notebook,
        ],
        check=True,
    )


def jupyter_to_html(notebook, output_dir=OUTPUT_DIR):
    """Convert a notebook to HTML."""
    subprocess.run(
        [
            "jupyter",
            "nbconvert",
            "--to",
            "html",
            "--output-dir",
            str(output_dir),
            notebook,
        ],
        check=True,
    )


def task_config():
    """Create necessary directories."""
    return {
        "actions": [
            f"mkdir -p {DATA_DIR}" if OS_TYPE == "nix" else f"mkdir {DATA_DIR}",
            f"mkdir -p {OUTPUT_DIR}" if OS_TYPE == "nix" else f"mkdir {OUTPUT_DIR}",
        ],
        "targets": [DATA_DIR, OUTPUT_DIR],
        "uptodate": [True],
        "verbosity": 2,
    }


def task_pull_crsp_stock():
    """Pull CRSP monthly stock data from WRDS."""
    return {
        "actions": ["python src/pull_CRSP_stock.py"],
        "file_dep": ["src/pull_CRSP_stock.py"],
        "targets": [
            DATA_DIR / "CRSP_MSF_INDEX_INPUTS.parquet",
            DATA_DIR / "CRSP_MSIX.parquet",
        ],
        "verbosity": 2,
    }


def task_pull_crsp_compustat():
    """Pull CRSP/Compustat data from WRDS."""
    return {
        "actions": ["python src/pull_CRSP_Compustat.py"],
        "file_dep": ["src/pull_CRSP_Compustat.py"],
        "targets": [
            DATA_DIR / "Compustat.parquet",
            DATA_DIR / "CRSP_stock_ciz.parquet",
            DATA_DIR / "CRSP_Comp_Link_Table.parquet",
            DATA_DIR / "FF_FACTORS.parquet",
        ],
        "verbosity": 2,
    }


def task_calc_fama_french():
    """Calculate Fama-French 1993 factors."""
    return {
        "actions": ["python src/calc_Fama_French_1993.py"],
        "file_dep": [
            "src/calc_Fama_French_1993.py",
            "src/pull_CRSP_Compustat.py",
            DATA_DIR / "Compustat.parquet",
            DATA_DIR / "CRSP_stock_ciz.parquet",
            DATA_DIR / "CRSP_Comp_Link_Table.parquet",
        ],
        "targets": [
            DATA_DIR / "FF_1993_vwret.parquet",
            DATA_DIR / "FF_1993_vwret_n.parquet",
            DATA_DIR / "FF_1993_factors.parquet",
            DATA_DIR / "FF_1993_nfirms.parquet",
            OUTPUT_DIR / "FF_1993_Comparison.png",
        ],
        "verbosity": 2,
    }


def task_create_ftsfr_datasets():
    """Create standardized FTSFR datasets."""
    return {
        "actions": ["python src/create_ftsfr_datasets.py"],
        "file_dep": [
            "src/create_ftsfr_datasets.py",
            "src/calc_Fama_French_1993.py",
            "src/pull_CRSP_Compustat.py",
            DATA_DIR / "CRSP_stock_ciz.parquet",
        ],
        "targets": [
            DATA_DIR / "ftsfr_CRSP_monthly_stock_ret.parquet",
            DATA_DIR / "ftsfr_CRSP_monthly_stock_retx.parquet",
        ],
        "verbosity": 2,
    }


def task_run_notebooks():
    """Execute and convert summary notebooks."""
    notebooks = ["src/summary_crsp_compustat_ipynb.py"]
    notebook_build_dir = OUTPUT_DIR

    for notebook_py in notebooks:
        notebook_ipynb = notebook_py.replace("_ipynb.py", ".ipynb")
        notebook_name = Path(notebook_ipynb).stem
        notebook_build_path = notebook_build_dir / f"{notebook_name}.ipynb"

        yield {
            "name": notebook_name,
            "actions": [
                f"mkdir -p {notebook_build_dir}" if OS_TYPE == "nix" else f"mkdir {notebook_build_dir}",
                f"ipynb-py-convert {notebook_py} {notebook_ipynb}",
                lambda nb=notebook_ipynb: jupyter_execute_notebook(nb),
                lambda nb=notebook_ipynb: jupyter_to_html(nb),
                f"cp {notebook_ipynb} {notebook_build_path}",
            ],
            "file_dep": [
                notebook_py,
                DATA_DIR / "ftsfr_CRSP_monthly_stock_ret.parquet",
            ],
            "targets": [
                notebook_ipynb,
                OUTPUT_DIR / f"{notebook_name}.html",
                notebook_build_path,
            ],
            "verbosity": 2,
        }


def task_generate_charts():
    """Generate interactive HTML charts."""
    return {
        "actions": ["python src/generate_chart.py"],
        "file_dep": [
            "src/generate_chart.py",
            DATA_DIR / "ftsfr_CRSP_monthly_stock_ret.parquet",
        ],
        "targets": [
            OUTPUT_DIR / "crsp_returns_replication.html",
            OUTPUT_DIR / "crsp_cumulative_returns.html",
        ],
        "verbosity": 2,
        "task_dep": ["create_ftsfr_datasets"],
    }


def task_generate_pipeline_site():
    """Generate the chartbook documentation site."""
    return {
        "actions": ["chartbook build -f"],
        "file_dep": [
            "chartbook.toml",
            OUTPUT_DIR / "summary_crsp_compustat.ipynb",
            OUTPUT_DIR / "crsp_returns_replication.html",
            OUTPUT_DIR / "crsp_cumulative_returns.html",
        ],
        "targets": [BASE_DIR / "docs" / "index.html"],
        "verbosity": 2,
        "task_dep": ["run_notebooks", "generate_charts"],
    }
