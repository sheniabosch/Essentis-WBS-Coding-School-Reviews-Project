import pandas as pd
from src_reviews_cleaning.config import (
    PATH_MASTER_CSV,
    COL_DATA_SOURCE,
)


def load_master_csv() -> pd.DataFrame:
    """
    Load the master reviews CSV from the shared drive.

    This file contains all three original data sources concatenated into
    one wide CSV, identified by the existing data_source column.
    The three sources are:
        - google: scraped from Google Maps (256 rows)
        - clean:  previously cleaned reviews from a prior intern (541 rows)
        - new:    most recent batch of reviews (49 rows)

    The file uses a semicolon separator and may contain multiline fields
    so the python engine is used for robustness.

    Args:
        None

    Returns:
        pd.DataFrame: Full master CSV with all 846 rows and 33 columns.

    Raises:
        FileNotFoundError: If the master CSV does not exist at PATH_MASTER_CSV.
        ValueError: If the data_source column is missing from the file.
    """
    df = pd.read_csv(
        PATH_MASTER_CSV,
        sep=";",
        engine="python",
        quotechar='"',
    )
    if COL_DATA_SOURCE not in df.columns:
        raise ValueError(
            f"Expected column '{COL_DATA_SOURCE}' not found in master CSV. "
            f"Columns present: {df.columns.tolist()}"
        )
    return df


def load_all_raw() -> dict[str, pd.DataFrame]:
    """
    Load the master CSV and split it into separate DataFrames by data source.

    Reads the master CSV once and partitions it into three subsets based on
    the data_source column. Each subset is reset to a clean zero-based index.

    This is the main entry point used by the pipeline. Downstream normalizers
    expect the same column structure as the original raw sources so splitting
    here allows each normalizer to handle only its own columns.

    Args:
        None

    Returns:
        dict[str, pd.DataFrame]: Dictionary with keys "google", "clean", "new".
            Each value is a DataFrame containing only rows from that source
            with the index reset to start from zero.

    Raises:
        FileNotFoundError: If the master CSV does not exist at PATH_MASTER_CSV.
        ValueError: If the data_source column is missing from the file.
    """
    master = load_master_csv()
    return {
        "google": master[master[COL_DATA_SOURCE] == "google"].reset_index(drop=True),
        "clean":  master[master[COL_DATA_SOURCE] == "clean"].reset_index(drop=True),
        "new":    master[master[COL_DATA_SOURCE] == "new"].reset_index(drop=True),
    }