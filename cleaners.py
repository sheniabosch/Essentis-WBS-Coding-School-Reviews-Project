import re
import time
import unicodedata
import pandas as pd
from datetime import datetime, timedelta
from lingua import Language, LanguageDetectorBuilder
from deep_translator import GoogleTranslator
from src_reviews_cleaning.config import (
    RATING_COLUMNS, COL_REVIEW_DATE, COL_VERIFIED, COL_OVERALL_EXPERIENCE,
    COL_COURSE_FORMAT, COL_COURSE, COL_ROLE, COL_REVIEW_TEXT,
    COL_REVIEW_TEXT_TRANSLATED, DATE_FORMAT, MIN_RATING, MAX_RATING,
)

# =============================================================================
# GOOGLE SCRAPE DATE ANCHOR
# Google reviews use relative dates like "3 months ago" instead of absolute
# dates. All relative dates are resolved by subtracting from this anchor date.
# IMPORTANT: If Google data is re-scraped update this date to match the new
# scrape date otherwise all Google dates will be wrong.
# =============================================================================
GOOGLE_SCRAPE_DATE = datetime(2025, 2, 1)

# =============================================================================
# COURSE NAME STANDARDIZATION MAP
# Maps raw course name strings to a fixed set of standard names.
# Keys are lowercase stripped versions of the raw values.
# Add new entries here if new courses are introduced.
# =============================================================================
COURSE_MAP = {
    "full-stack web & app":           "Full-Stack Web & App",
    "full-stack php web development": "Full-Stack PHP Development",
    "data science":                   "Data Science",
    "product design":                 "Product Design",
    "marketing analytics":            "Marketing Analytics",
}

# =============================================================================
# ROLE CATEGORIZATION KEYWORDS
# Maps raw role strings to broad categories using keyword matching.
# Order matters - the first matching category wins.
# =============================================================================
ROLE_KEYWORDS = {
    "Graduate":     ["graduate", "alumni"],
    "Student":      ["student"],
    "Applicant":    ["applicant"],
    "Developer":    ["developer", "engineer", "full stack", "fullstack", "full-stack", "frontend", "backend"],
    "Data Science": ["data scien", "analytics", "ml", "ai"],
    "Designer":     ["design"],
    "Manager":      ["manager", "consultant"],
}


def _parse_relative_date(relative: str, anchor: datetime) -> str:
    """
    Convert a relative date string to an absolute date string.

    Translates Google relative date format (e.g. "3 months ago") into a
    real date by subtracting the stated duration from the scrape anchor date.

    Args:
        relative (str): Relative date string from Google reviews.
                        Expected format: "<number|a> <unit>s? ago"
        anchor (datetime): The scrape date to calculate backwards from.

    Returns:
        str: Absolute date string formatted as DATE_FORMAT (YYYYMMDD),
             or pd.NA if the format is not recognized.
    """
    relative = str(relative).strip().lower()
    match = re.match("(a|[0-9]+)\s+(day|week|month|year)s?\s+ago", relative)
    if not match:
        return pd.NA
    quantity = 1 if match.group(1) == "a" else int(match.group(1))
    unit = match.group(2)
    if unit == "day":
        delta = timedelta(days=quantity)
    elif unit == "week":
        delta = timedelta(weeks=quantity)
    elif unit == "month":
        delta = timedelta(days=quantity * 30)
    elif unit == "year":
        delta = timedelta(days=quantity * 365)
    return (anchor - delta).strftime(DATE_FORMAT)


def clean_empty_strings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert empty or whitespace-only strings to NaN across all object columns.

    Empty strings are not the same as NaN in pandas and can cause issues
    with language detection, word counts, and null value analysis.
    Only object dtype columns are touched.

    Args:
        df (pd.DataFrame): Combined normalized DataFrame from normalize_all().

    Returns:
        pd.DataFrame: DataFrame with empty strings replaced by pd.NA.
    """
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].replace("", pd.NA)
        df[col] = df[col].apply(
            lambda x: pd.NA if isinstance(x, str) and x.strip() == "" else x
        )
    return df


def clean_review_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize all review dates to pandas datetime64.

    Handles two date formats:
    1. Relative strings from Google (e.g. "3 months ago") resolved using
       _parse_relative_date() with GOOGLE_SCRAPE_DATE as anchor.
    2. Absolute date strings from CSV sources parsed directly.

    Final dates are stored as datetime64 so downstream analysis can use
    pandas date methods. The DATE_FORMAT constant (YYYYMMDD) is used for
    parsing relative dates before converting to datetime.

    Args:
        df (pd.DataFrame): Combined normalized DataFrame.

    Returns:
        pd.DataFrame: DataFrame with review_date as datetime64[ns].
    """
    def _convert(val: any) -> any:
        """Convert a single date value to datetime."""
        if pd.isna(val):
            return pd.NA
        val = str(val).strip()
        if "ago" in val:
            relative = _parse_relative_date(val, GOOGLE_SCRAPE_DATE)
            return pd.to_datetime(relative, format=DATE_FORMAT) if pd.notna(relative) else pd.NA
        try:
            return pd.to_datetime(val)
        except Exception:
            return pd.NA

    df[COL_REVIEW_DATE] = pd.to_datetime(
        df[COL_REVIEW_DATE].apply(_convert), errors="coerce"
    )
    return df


def detect_and_translate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect the language of each review and translate non-English reviews.

    Uses lingua for language detection and deep-translator (Google Translate
    backend) for translation. Only non-English reviews are translated.
    The original review_text column is never modified. Translated text is
    stored in review_text_translated. A 2 second delay between translation
    calls avoids rate limiting.

    Args:
        df (pd.DataFrame): Combined normalized DataFrame after date cleaning.

    Returns:
        pd.DataFrame: DataFrame with review_text_translated populated for
                      non-English reviews. English reviews have NaN.
    """
    detector   = LanguageDetectorBuilder.from_all_languages().build()
    translator = GoogleTranslator(source="auto", target="en")
    translated = []
    total      = len(df)

    for i, text in enumerate(df[COL_REVIEW_TEXT]):
        if pd.isna(text):
            translated.append(pd.NA)
            continue
        language = detector.detect_language_of(str(text))
        if language is None or language == Language.ENGLISH:
            translated.append(pd.NA)
            continue
        try:
            result = translator.translate(str(text))
            translated.append(result)
            preview = str(text)[:50]
            print(f"  [{i+1}/{total}] Translated from {language.name}: {preview}...")
            time.sleep(2)
        except Exception as e:
            print(f"  [{i+1}/{total}] Translation failed: {e}")
            translated.append(pd.NA)

    df[COL_REVIEW_TEXT_TRANSLATED] = translated
    non_english = df[COL_REVIEW_TEXT_TRANSLATED].notna().sum()
    print(f"Translation complete. {non_english} non-English reviews translated.")
    return df


def clean_ratings(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize all numeric rating columns to float clamped to 1-5.

    Handles European comma decimals (e.g. "4,7" -> "4.7"), out-of-range
    values which are set to NaN, and non-numeric strings set to NaN.

    Args:
        df (pd.DataFrame): Combined normalized DataFrame.

    Returns:
        pd.DataFrame: DataFrame with all four rating columns as float64.
    """
    for col in RATING_COLUMNS:
        df[col] = pd.to_numeric(
            df[col].astype(str).str.replace(",", ".", regex=False),
            errors="coerce"
        )
        df[col] = df[col].where(df[col].between(MIN_RATING, MAX_RATING))
    return df


def calculate_overall_experience(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate overall_experience as the row-wise mean of all four rating columns.

    NaN values are ignored so partial ratings still produce a valid average.
    Rows where ALL four ratings are NaN receive NaN.
    Must be called after clean_ratings().

    Args:
        df (pd.DataFrame): DataFrame with all four rating columns cleaned.

    Returns:
        pd.DataFrame: DataFrame with overall_experience added as float64
                      rounded to 2 decimal places.
    """
    df[COL_OVERALL_EXPERIENCE] = df[RATING_COLUMNS].mean(axis=1).round(2)
    return df


def clean_verified(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize the verified column to pandas BooleanDtype.

    Uses nullable BooleanDtype so NaN coexists with True/False.
    Handles native booleans and string "TRUE"/"FALSE".

    Args:
        df (pd.DataFrame): Combined normalized DataFrame.

    Returns:
        pd.DataFrame: DataFrame with verified as pandas BooleanDtype.
    """
    def _convert(val: any) -> any:
        if pd.isna(val):
            return pd.NA
        if isinstance(val, bool):
            return val
        val_str = str(val).strip().upper()
        if val_str == "TRUE":
            return True
        if val_str == "FALSE":
            return False
        return pd.NA

    df[COL_VERIFIED] = df[COL_VERIFIED].apply(_convert).astype("boolean")
    return df


def clean_course_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize course_format to "full-time", "part-time", or NaN.

    Only the two valid formats are kept. Everything else is NaN.

    Args:
        df (pd.DataFrame): Combined normalized DataFrame.

    Returns:
        pd.DataFrame: DataFrame with course_format standardized.
    """
    VALID_FORMATS = {"full-time", "part-time"}

    def _convert(val: any) -> any:
        if pd.isna(val):
            return pd.NA
        val_lower = str(val).strip().lower()
        return val_lower if val_lower in VALID_FORMATS else pd.NA

    df[COL_COURSE_FORMAT] = df[COL_COURSE_FORMAT].apply(_convert)
    return df


def clean_course(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize the course column to a fixed set of known course names.

    Maps raw variations to standardized names using COURSE_MAP.
    Values of "Unknown" or anything not in COURSE_MAP are set to NaN.

    Args:
        df (pd.DataFrame): Combined normalized DataFrame.

    Returns:
        pd.DataFrame: DataFrame with course column standardized.
    """
    def _standardize(val: any) -> any:
        if pd.isna(val):
            return pd.NA
        val_lower = str(val).strip().lower()
        if val_lower == "unknown":
            return pd.NA
        return COURSE_MAP.get(val_lower, pd.NA)

    df[COL_COURSE] = df[COL_COURSE].apply(_standardize)
    return df


def clean_role(df: pd.DataFrame) -> pd.DataFrame:
    """
    Categorize the role column into a fixed set of broad role categories.

    Uses keyword matching via ROLE_KEYWORDS. The first matching category wins.
    Unmatched roles are categorized as "Other".

    Args:
        df (pd.DataFrame): Combined normalized DataFrame.

    Returns:
        pd.DataFrame: DataFrame with role column standardized.
    """
    def _categorize(val: any) -> any:
        if pd.isna(val):
            return pd.NA
        val_lower = str(val).strip().lower()
        for category, keywords in ROLE_KEYWORDS.items():
            if any(kw in val_lower for kw in keywords):
                return category
        return "Other"

    df[COL_ROLE] = df[COL_ROLE].apply(_categorize)
    return df


def clean_all(df: pd.DataFrame) -> pd.DataFrame:
    """
    Run all cleaning functions on the combined normalized DataFrame.

    This is the main entry point used by pipeline.py. Steps run in order:
        1. clean_empty_strings       - standardize empty strings to NaN
        2. clean_review_dates        - parse all dates to datetime
        3. detect_and_translate      - translate non-English reviews (~90 sec)
        4. clean_ratings             - standardize all four numeric ratings
        5. calculate_overall_experience - average ratings after cleaning
        6. clean_verified            - normalize boolean column
        7. clean_course_format       - standardize format categories
        8. clean_course              - standardize course names
        9. clean_role                - categorize reviewer roles

    Args:
        df (pd.DataFrame): Combined normalized DataFrame from normalize_all().

    Returns:
        pd.DataFrame: Fully cleaned DataFrame ready for deduplication.
    """
    df = clean_empty_strings(df)
    df = clean_review_dates(df)
    df = detect_and_translate(df)
    df = clean_ratings(df)
    df = calculate_overall_experience(df)
    df = clean_verified(df)
    df = clean_course_format(df)
    df = clean_course(df)
    df = clean_role(df)
    return df