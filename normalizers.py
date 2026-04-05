import re
import pandas as pd
from src_reviews_cleaning.config import (
    COL_AUTHOR, COL_REVIEW_DATE, COL_INSTRUCTORS,
    COL_CURRICULUM, COL_JOB_ASSISTANCE, COL_REVIEW_TEXT, COL_COURSE_FORMAT,
    COL_ROLE, COL_VERIFIED, COL_VERIFICATION_SOURCE, COL_LINK,
    COL_REVIEW_SOURCE, COL_BATCH_ID, COL_DATA_SOURCE, COL_REVIEW_RATING,
    COL_COURSE, COL_REVIEW_TEXT_TRANSLATED,
)

# =============================================================================
# SOURCE-SPECIFIC RAW COLUMNS TO KEEP
# Because all sources are concatenated in one master CSV every row has all
# 33 columns. We keep only the columns relevant to each source before renaming
# to prevent duplicate column names after the rename step.
# =============================================================================
GOOGLE_KEEP = [
    "reviewer_name", "rating", "review_date", "review_text",
    "link", "photo_count", "review_images", "data_source",
]

CLEAN_KEEP = [
    "author", "date", "overall_experience", "instructors", "curriculum",
    "job_assistance", "title", "review", "verified", "verification_source",
    "role", "course", "format", "source", "Batch ID", "data_source",
]

NEW_KEEP = [
    "author", "date", "overall_experience", "instructors", "curriculum",
    "job_assistance", "title", "review", "course", "format",
    "source", "Batch ID", "data_source",
]

# =============================================================================
# FINAL COLUMN ORDER
# The standard set of columns every normalized DataFrame must have.
# COL_OVERALL_EXPERIENCE is intentionally excluded here - it is calculated
# in cleaners.py after all individual ratings have been cleaned.
# =============================================================================
FINAL_COLUMNS = [
    COL_BATCH_ID,
    COL_AUTHOR,
    COL_DATA_SOURCE,
    COL_REVIEW_SOURCE,
    COL_REVIEW_DATE,
    COL_REVIEW_RATING,
    COL_INSTRUCTORS,
    COL_CURRICULUM,
    COL_JOB_ASSISTANCE,
    COL_REVIEW_TEXT,
    COL_REVIEW_TEXT_TRANSLATED,
    COL_COURSE_FORMAT,
    COL_COURSE,
    COL_ROLE,
    COL_VERIFIED,
    COL_VERIFICATION_SOURCE,
    COL_LINK,
]


def _strip_emojis(text: str) -> str:
    """
    Remove all emoji and non-standard unicode characters from a string.

    Emojis cause rendering issues in matplotlib tables and CSV exports.
    This function removes any character whose unicode category starts with
    "So" (other symbol), "Cs" (surrogate), "Co" (private use), or "Cn"
    (unassigned), as well as any character above the Basic Multilingual
    Plane (code point > 0xFFFF) which covers most modern emoji.

    Standard Latin, accented characters, punctuation, and CJK characters
    are preserved.

    Args:
        text (str): Raw text potentially containing emoji characters.

    Returns:
        str: Clean text with all emoji characters removed.
             Returns the original value unchanged if it is not a string.
    """
    import unicodedata
    if not isinstance(text, str):
        return text
    return "".join(
        c for c in text
        if unicodedata.category(c) not in ("So", "Cs", "Co", "Cn")
        and ord(c) < 0x10000
    )


def _combine_title_and_review(df: pd.DataFrame, title_col: str, review_col: str) -> pd.Series:
    """
    Combine a title column and a review body column into a single text field.

    The clean and new sources store the review title and body in separate columns.
    This function merges them into one field for consistency with the Google source
    which only has a single review_text column.

    If a title exists it is prepended to the body with ": " as a separator.
    If no title exists the body is returned as-is.
    If both are empty or NaN the result is NaN.

    Args:
        df (pd.DataFrame): DataFrame containing both the title and review columns.
        title_col (str): Name of the column containing the review title.
        review_col (str): Name of the column containing the review body.

    Returns:
        pd.Series: Combined review text one value per row.
                   Rows with no title or body are returned as NaN.
    """
    title  = df[title_col].fillna("").str.strip()
    review = df[review_col].fillna("").str.strip()
    combined = title.where(title == "", title + ": ") + review
    return combined.replace("", pd.NA)


def _add_missing_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add any standard columns that are missing from a DataFrame as NaN.

    Ensures every normalized DataFrame has the full set of standard columns
    before they are concatenated in normalize_all().

    Args:
        df (pd.DataFrame): Partially normalized DataFrame missing some columns.

    Returns:
        pd.DataFrame: DataFrame with all FINAL_COLUMNS present.
    """
    for col in FINAL_COLUMNS:
        if col not in df.columns:
            df[col] = pd.NA
    return df


def normalize_google(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize raw Google Maps reviews to the standard column schema.

    Keeps only Google-specific columns to prevent duplicate column names,
    strips emojis from all text fields, renames columns to the standard
    schema, and hardcodes the review source as "google".

    Key differences from the standard schema:
        - reviewer name is in reviewer_name not author
        - star rating is in rating not review
        - review date is a relative string like "3 months ago"
        - no batch_id, role, verified, course, or course_format column
        - review source must be hardcoded as "google"

    Args:
        df (pd.DataFrame): Raw Google reviews subset from load_all_raw().
                           Expected shape: (256, 33).

    Returns:
        pd.DataFrame: Normalized Google reviews with all FINAL_COLUMNS present.
    """
    # Keep only Google-specific columns to avoid duplicates after renaming
    df = df[[c for c in GOOGLE_KEEP if c in df.columns]].copy()

    # Strip emojis from all object columns before any further processing
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(_strip_emojis)

    # Rename raw Google column names to standard pipeline names
    df = df.rename(columns={
        "reviewer_name": COL_AUTHOR,
        "rating":        COL_REVIEW_RATING,
        "review_date":   COL_REVIEW_DATE,
        "review_text":   COL_REVIEW_TEXT,
        "link":          COL_LINK,
    })

    # Drop columns not needed in the final schema
    df = df.drop(columns=["photo_count", "review_images"], errors="ignore")

    # Hardcode the review source since Google data has no source column
    df[COL_REVIEW_SOURCE] = "google"

    df = _add_missing_columns(df)
    return df[FINAL_COLUMNS]


def normalize_clean(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize the previously cleaned CSV reviews to the standard column schema.

    Keeps only clean-source columns, strips emojis, combines title and review
    body into review_text, and renames columns to the standard schema.

    Key differences from the standard schema:
        - review title and body are in separate columns (title, review)
        - date column is named date not review_date
        - overall_experience is used as the star rating column
        - course format is in format not course_format
        - reviewer batch ID is in Batch ID not batch_id

    Args:
        df (pd.DataFrame): Raw clean reviews subset from load_all_raw().
                           Expected shape: (541, 33).

    Returns:
        pd.DataFrame: Normalized clean reviews with all FINAL_COLUMNS present.
    """
    df = df[[c for c in CLEAN_KEEP if c in df.columns]].copy()

    # Strip emojis from all object columns
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(_strip_emojis)

    # Combine title and review body into one review_text field
    df[COL_REVIEW_TEXT] = _combine_title_and_review(df, title_col="title", review_col="review")
    df = df.drop(columns=["review", "title"], errors="ignore")

    df = df.rename(columns={
        "date":                COL_REVIEW_DATE,
        "overall_experience":  COL_REVIEW_RATING,
        "format":              COL_COURSE_FORMAT,
        "source":              COL_REVIEW_SOURCE,
        "Batch ID":            COL_BATCH_ID,
        "verification_source": COL_VERIFICATION_SOURCE,
        "course":              COL_COURSE,
    })

    df = _add_missing_columns(df)
    return df[FINAL_COLUMNS]


def normalize_new(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize the newest batch of reviews to the standard column schema.

    Keeps only new-source columns, strips emojis, combines title and review
    body into review_text, and renames columns to the standard schema.

    Key differences from the standard schema:
        - review title and body are in separate columns (title, review)
        - date column is named date not review_date
        - overall_experience is used as the star rating column
        - course format is in format not course_format
        - reviewer batch ID is in Batch ID not batch_id

    Args:
        df (pd.DataFrame): Raw new reviews subset from load_all_raw().
                           Expected shape: (49, 33).

    Returns:
        pd.DataFrame: Normalized new reviews with all FINAL_COLUMNS present.
    """
    df = df[[c for c in NEW_KEEP if c in df.columns]].copy()

    # Strip emojis from all object columns
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(_strip_emojis)

    # Combine title and review body into one review_text field
    df[COL_REVIEW_TEXT] = _combine_title_and_review(df, title_col="title", review_col="review")
    df = df.drop(columns=["review", "title"], errors="ignore")

    df = df.rename(columns={
        "date":               COL_REVIEW_DATE,
        "overall_experience": COL_REVIEW_RATING,
        "format":             COL_COURSE_FORMAT,
        "source":             COL_REVIEW_SOURCE,
        "Batch ID":           COL_BATCH_ID,
        "course":             COL_COURSE,
    })

    df = _add_missing_columns(df)
    return df[FINAL_COLUMNS]


def normalize_all(raw: dict) -> pd.DataFrame:
    """
    Normalize all three raw DataFrames and concatenate them into one.

    Calls each source-specific normalizer in sequence then stacks the
    results into a single DataFrame using pd.concat. The index is reset
    to a clean zero-based integer index after concatenation.

    Args:
        raw (dict): Output of load_all_raw(). Expected keys: "google", "clean", "new".

    Returns:
        pd.DataFrame: Single combined DataFrame with all reviews normalized.
                      Shape should be approximately (846, 17) before cleaning.

    Raises:
        KeyError: If any of the expected keys are missing from the raw dict.
    """
    normalized = [
        normalize_google(raw["google"]),
        normalize_clean(raw["clean"]),
        normalize_new(raw["new"]),
    ]
    return pd.concat(normalized, ignore_index=True)