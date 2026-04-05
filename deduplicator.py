import pandas as pd
from thefuzz import fuzz
from src_reviews_cleaning.config import (
    COL_AUTHOR,
    COL_REVIEW_DATE,
    COL_REVIEW_TEXT,
    FUZZY_MATCH_THRESHOLD,
)


def _count_nulls(df: pd.DataFrame) -> pd.Series:
    """
    Count the number of NaN values in each row of a DataFrame.

    Used to rank duplicate rows so the most complete one is kept.

    Args:
        df (pd.DataFrame): Any DataFrame to count nulls across.

    Returns:
        pd.Series: Integer null count per row same index as df.
    """
    return df.isnull().sum(axis=1)


def _normalise_text(text: any) -> str:
    """
    Normalise a review text string for fuzzy comparison.

    Converts to lowercase and strips leading/trailing whitespace.
    This ensures minor capitalisation differences do not prevent
    fuzzy matching from identifying near-duplicate reviews.

    Args:
        text (any): Raw review text value possibly NaN.

    Returns:
        str: Normalised lowercase string or empty string if NaN.
    """
    if pd.isna(text):
        return ""
    return str(text).strip().lower()


def get_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Return all exact duplicate rows for inspection without removing them.

    Uses exact matching on author + review_date + review_text.
    For fuzzy duplicates use remove_duplicates() which handles both.

    Args:
        df (pd.DataFrame): Cleaned DataFrame from clean_all().

    Returns:
        pd.DataFrame: All rows that have at least one exact duplicate
                      sorted so duplicate pairs appear next to each other.
                      Returns empty DataFrame if no exact duplicates found.
    """
    subset = [COL_AUTHOR, COL_REVIEW_DATE, COL_REVIEW_TEXT]
    dupes  = df[df.duplicated(subset=subset, keep=False)]
    return dupes.sort_values(subset).reset_index(drop=True)


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate reviews using a two-stage approach.

    Stage 1 — Exact deduplication:
        Removes rows where author, review_date, and review_text are all
        identical. When duplicates are found the row with the fewest NaN
        values is kept.

    Stage 2 — Fuzzy deduplication:
        Within each group of reviews sharing the same author and review_date,
        compares review texts using fuzzy string matching (token sort ratio).
        If two reviews from the same author on the same date have text
        similarity above FUZZY_MATCH_THRESHOLD (80%) they are considered
        duplicates and the more complete row is kept.

        Token sort ratio is used rather than simple ratio because it is
        robust to word reordering and handles cases where one review is
        a slightly rewritten version of another.

    The temporary _null_count column used for sorting is removed before
    returning so the output schema matches the input schema exactly.

    Args:
        df (pd.DataFrame): Cleaned DataFrame from clean_all().

    Returns:
        pd.DataFrame: DataFrame with duplicates removed and index reset.
                      The row with the fewest NaN values is kept from
                      each duplicate group.

    Side effects:
        Prints the number of exact and fuzzy duplicates removed
        and the final row count.
    """
    # -------------------------------------------------------------------------
    # Stage 1: Exact deduplication
    # -------------------------------------------------------------------------
    df["_null_count"] = _count_nulls(df)
    df = df.sort_values("_null_count", ascending=True)
    before_exact = len(df)
    df = df.drop_duplicates(
        subset=[COL_AUTHOR, COL_REVIEW_DATE, COL_REVIEW_TEXT],
        keep="first"
    )
    exact_removed = before_exact - len(df)
    print(f"Exact duplicates removed: {exact_removed}")

    # -------------------------------------------------------------------------
    # Stage 2: Fuzzy deduplication
    # -------------------------------------------------------------------------
    # Group by author and review_date - only compare within same author/date
    # to keep the operation tractable and avoid false positives
    df = df.reset_index(drop=True)
    indices_to_drop = set()

    # Get groups that have more than one review for the same author + date
    group_cols = [COL_AUTHOR, COL_REVIEW_DATE]
    grouped    = df.groupby(group_cols, dropna=False)

    for _, group in grouped:
        if len(group) < 2:
            # No potential duplicates in this group
            continue

        group_indices = group.index.tolist()

        # Compare every pair within the group
        for i in range(len(group_indices)):
            for j in range(i + 1, len(group_indices)):
                idx_a = group_indices[i]
                idx_b = group_indices[j]

                # Skip if one of the indices is already marked for removal
                if idx_a in indices_to_drop or idx_b in indices_to_drop:
                    continue

                text_a = _normalise_text(df.at[idx_a, COL_REVIEW_TEXT])
                text_b = _normalise_text(df.at[idx_b, COL_REVIEW_TEXT])

                # Empty texts cannot be meaningfully compared
                if not text_a or not text_b:
                    continue

                # Token sort ratio handles word reordering between versions
                similarity = fuzz.token_sort_ratio(text_a, text_b)

                if similarity >= FUZZY_MATCH_THRESHOLD:
                    # Keep the row with fewer nulls - drop the other
                    nulls_a = df.at[idx_a, "_null_count"]
                    nulls_b = df.at[idx_b, "_null_count"]
                    drop_idx = idx_b if nulls_a <= nulls_b else idx_a
                    indices_to_drop.add(drop_idx)

    fuzzy_removed = len(indices_to_drop)
    df = df.drop(index=list(indices_to_drop))
    print(f"Fuzzy duplicates removed: {fuzzy_removed} (threshold: {FUZZY_MATCH_THRESHOLD}%)")

    # Remove the temporary null count column
    df = df.drop(columns=["_null_count"])
    print(f"Rows remaining:           {len(df)}")

    return df.reset_index(drop=True)