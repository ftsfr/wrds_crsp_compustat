"""Helper module to load Ken French data library datasets.

This module downloads data directly from Ken French's website and parses it
in a format compatible with the test suite.
"""

import io
import zipfile

import pandas as pd
import requests


# Base URL for Ken French data library
BASE_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/"


def _download_and_extract_csv(dataset_name):
    """Download and extract CSV data from Ken French's website.

    Parameters
    ----------
    dataset_name : str
        Name of the dataset (e.g., "6_Portfolios_2x3", "F-F_Research_Data_Factors")

    Returns
    -------
    str
        The raw CSV content as a string.
    """
    url = f"{BASE_URL}{dataset_name}_CSV.zip"
    response = requests.get(url)
    response.raise_for_status()

    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        # Get the first CSV file in the archive
        csv_name = [n for n in zf.namelist() if n.endswith(".CSV") or n.endswith(".csv")][0]
        with zf.open(csv_name) as f:
            return f.read().decode("utf-8")


def _parse_csv_sections(csv_content):
    """Parse CSV content into sections (sheets).

    Ken French data files contain multiple sections separated by blank lines
    and section headers.

    Parameters
    ----------
    csv_content : str
        Raw CSV content

    Returns
    -------
    list of pd.DataFrame
        List of DataFrames, one for each section
    """
    lines = csv_content.strip().split("\n")
    sections = []
    current_section_lines = []
    in_data = False
    header_line = None

    for line in lines:
        stripped = line.strip()

        # Skip empty lines or lines that are section separators
        if not stripped:
            if current_section_lines and header_line is not None:
                # End of a section, parse it
                df = _parse_section(header_line, current_section_lines)
                if df is not None and len(df) > 0:
                    sections.append(df)
                current_section_lines = []
                header_line = None
                in_data = False
            continue

        # Check if this is a header line (contains commas and looks like column names)
        parts = [p.strip() for p in stripped.split(",")]

        # Try to detect if this is a numeric data row (first element is a date-like number)
        first_val = parts[0] if parts else ""
        is_numeric_row = first_val.isdigit() and len(first_val) in [6, 8]

        if is_numeric_row:
            in_data = True
            current_section_lines.append(stripped)
        elif in_data:
            # We were in data but hit a non-numeric row - end this section
            if current_section_lines and header_line is not None:
                df = _parse_section(header_line, current_section_lines)
                if df is not None and len(df) > 0:
                    sections.append(df)
                current_section_lines = []
                header_line = None
                in_data = False
            # Check if this could be a new header
            if len(parts) > 1:
                header_line = stripped
        elif len(parts) > 1 and not is_numeric_row:
            # Potential header line
            header_line = stripped

    # Don't forget the last section
    if current_section_lines and header_line is not None:
        df = _parse_section(header_line, current_section_lines)
        if df is not None and len(df) > 0:
            sections.append(df)

    return sections


def _parse_section(header_line, data_lines):
    """Parse a single section into a DataFrame.

    Parameters
    ----------
    header_line : str
        The header line with column names
    data_lines : list of str
        The data lines

    Returns
    -------
    pd.DataFrame or None
        Parsed DataFrame or None if parsing fails
    """
    try:
        # Combine header and data
        csv_text = header_line + "\n" + "\n".join(data_lines)
        df = pd.read_csv(io.StringIO(csv_text))

        # Rename first column to Date
        df.columns = ["Date"] + list(df.columns[1:])

        # Clean column names
        df.columns = [str(c).strip() for c in df.columns]

        # Parse Date column
        date_str = df["Date"].astype(str).str.strip()
        if date_str.str.len().iloc[0] == 6:
            # Monthly format YYYYMM
            df["Date"] = pd.to_datetime(date_str, format="%Y%m")
        elif date_str.str.len().iloc[0] == 8:
            # Daily format YYYYMMDD
            df["Date"] = pd.to_datetime(date_str, format="%Y%m%d")

        return df
    except Exception:
        return None


def load_sheet(dataset_name, sheet_name="0"):
    """Load a sheet from the Ken French data library.

    Parameters
    ----------
    dataset_name : str
        Name of the dataset (e.g., "6_Portfolios_2x3", "F-F_Research_Data_Factors")
    sheet_name : str
        The sheet/table index as a string (e.g., "0", "1", etc.)

    Returns
    -------
    pd.DataFrame
        DataFrame with a "Date" column and the data columns from the selected sheet.
    """
    csv_content = _download_and_extract_csv(dataset_name)
    sections = _parse_csv_sections(csv_content)

    sheet_idx = int(sheet_name)
    if sheet_idx >= len(sections):
        raise ValueError(f"Sheet index {sheet_idx} out of range. Only {len(sections)} sheets available.")

    return sections[sheet_idx]
