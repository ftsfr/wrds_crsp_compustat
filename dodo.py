"""
dodo.py - Doit build automation for WRDS CRSP/Compustat pipeline

Run with: doit
"""

import os
import platform
import shutil
import sys
from pathlib import Path

import chartbook

sys.path.insert(1, "./src/")

BASE_DIR = chartbook.env.get_project_root()
DATA_DIR = BASE_DIR / "_data"
OUTPUT_DIR = BASE_DIR / "_output"
OS_TYPE = "nix" if platform.system() != "Windows" else "windows"


## Helpers for handling Jupyter Notebook tasks
os.environ["PYDEVD_DISABLE_FILE_VALIDATION"] = "1"


# fmt: off
## Helper functions for automatic execution of Jupyter notebooks
def jupyter_execute_notebook(notebook_path):
    return f"jupyter nbconvert --execute --to notebook --ClearMetadataPreprocessor.enabled=True --inplace {notebook_path}"
def jupyter_to_html(notebook_path, output_dir=OUTPUT_DIR):
    return f"jupyter nbconvert --to html --output-dir={output_dir} {notebook_path}"
def jupyter_to_md(notebook_path, output_dir=OUTPUT_DIR):
    """Requires jupytext"""
    return f"jupytext --to markdown --output-dir={output_dir} {notebook_path}"
def jupyter_to_python(notebook_path, notebook, build_dir):
    """Convert a notebook to a python script"""
    return f"jupyter nbconvert --to python {notebook_path} --output _{notebook}.py --output-dir {build_dir}"
def jupyter_clear_output(notebook_path):
    """Clear the output of a notebook"""
    return f"jupyter nbconvert --ClearOutputPreprocessor.enabled=True --ClearMetadataPreprocessor.enabled=True --inplace {notebook_path}"
def jupytext_to_notebook(pyfile_path, notebook_path):
    """Convert a Python script to a Jupyter notebook using jupytext."""
    return f"jupytext --to notebook --output {notebook_path} {pyfile_path}"
# fmt: on


def mkdir_p(path):
    """Create directory and parents if they don't exist (platform-agnostic)."""
    Path(path).mkdir(parents=True, exist_ok=True)


def mv_file(from_path, to_path):
    """Move a file to a destination path using Python (platform-agnostic)."""
    from_path = Path(from_path)
    to_path = Path(to_path)
    to_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(from_path), str(to_path))


##################################
## Begin rest of PyDoit tasks here
##################################


def task_config():
    """Create empty directories for data and output if they don't exist"""
    return {
        "actions": [
            (mkdir_p, [DATA_DIR]),
            (mkdir_p, [OUTPUT_DIR]),
        ],
        "targets": [DATA_DIR, OUTPUT_DIR],
        "file_dep": [".env"],
        "clean": [],
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


notebook_tasks = {
    "summary_crsp_compustat_ipynb": {
        "path": "./src/summary_crsp_compustat_ipynb.py",
        "file_dep": [
            DATA_DIR / "ftsfr_CRSP_monthly_stock_ret.parquet",
        ],
        "targets": [],
    },
}
notebook_files = []
for notebook in notebook_tasks.keys():
    pyfile_path = Path(notebook_tasks[notebook]["path"])
    notebook_files.append(pyfile_path)


# fmt: off
def task_run_notebooks():
    """Convert, execute, and export notebooks to HTML.

    Uses jupytext to convert .py files to .ipynb, then executes and exports to HTML.
    """
    for notebook in notebook_tasks.keys():
        pyfile_path = Path(notebook_tasks[notebook]["path"])
        # Create notebook in src/ directory (same as .py file) so imports work
        notebook_path = pyfile_path.with_suffix(".ipynb")
        output_notebook_path = OUTPUT_DIR / "_notebook_build" / f"{notebook}.ipynb"
        yield {
            "name": notebook,
            "actions": [
                jupytext_to_notebook(pyfile_path, notebook_path),
                jupyter_execute_notebook(notebook_path),
                (mkdir_p, [OUTPUT_DIR / "_notebook_build"]),
                (mv_file, [notebook_path, output_notebook_path]),
                jupyter_to_html(output_notebook_path),
            ],
            "file_dep": [
                pyfile_path,
                *notebook_tasks[notebook]["file_dep"],
            ],
            "targets": [
                OUTPUT_DIR / f"{notebook}.html",
                *notebook_tasks[notebook]["targets"],
            ],
            "clean": True,
            "verbosity": 2,
        }
# fmt: on


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
            *notebook_files,
            OUTPUT_DIR / "crsp_returns_replication.html",
            OUTPUT_DIR / "crsp_cumulative_returns.html",
        ],
        "targets": [BASE_DIR / "docs" / "index.html"],
        "verbosity": 2,
        "task_dep": ["run_notebooks", "generate_charts"],
    }
