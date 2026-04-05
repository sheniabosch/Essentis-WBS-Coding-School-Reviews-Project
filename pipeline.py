import pandas as pd
from pathlib import Path
from src_reviews_cleaning.loaders import load_all_raw
from src_reviews_cleaning.normalizers import normalize_all
from src_reviews_cleaning.cleaners import clean_all
from src_reviews_cleaning.deduplicator import remove_duplicates
from src_reviews_cleaning.config import PROCESSED_DIR

# =============================================================================
# FINAL COLUMN ORDER
# Defines the exact column order of the pipeline output.
# All 18 columns are listed explicitly so the output is always predictable.
# If a new column is added to the pipeline it must be added here too.
# =============================================================================
FINAL_COLUMN_ORDER = [
    "batch_id",
    "author",
    "data_source",
    "source",
    "review_date",
    "overall_experience",
    "review",
    "instructors",
    "curriculum",
    "job_assistance",
    "review_text",
    "review_text_translated",
    "course_format",
    "course",
    "role",
    "verified",
    "verification_source",
    "link",
]


def run(save_csv: bool = False) -> pd.DataFrame:
    """
    Run the full reviews cleaning pipeline from raw data to clean output.

    This is the single entry point for the entire pipeline. It orchestrates
    all four modules in sequence and returns a fully cleaned deduplicated
    DataFrame ready for analysis.

    Pipeline steps:
        1. Load       - reads master_reviews_raw.csv and splits by data_source
        2. Normalize  - renames columns to standard schema per source
                        and strips emojis from all text fields
        3. Clean      - standardizes dates, ratings, text, course, role, and
                        detects and translates non-English reviews (~90 seconds)
        4. Deduplicate - removes exact duplicates and fuzzy near-duplicates
                         (80% similarity threshold on same author + date)
        5. Reorder    - applies FINAL_COLUMN_ORDER for consistent output

    Expected output:
        - Shape: approximately (843, 18)
        - All columns present and in FINAL_COLUMN_ORDER
        - review_date as datetime64
        - verified as pandas BooleanDtype
        - overall_experience as float64 rounded to 2 decimal places
        - review_text_translated populated for ~41 non-English reviews
        - No emoji characters in any text column

    Args:
        save_csv (bool): If True saves the cleaned DataFrame to
                         PROCESSED_DIR / "all_reviews_cleaned.csv".
                         Default is False.

    Returns:
        pd.DataFrame: Fully cleaned and deduplicated reviews DataFrame
                      with columns in FINAL_COLUMN_ORDER.

    Raises:
        FileNotFoundError: If master_reviews_raw.csv does not exist.
        KeyError: If expected columns are missing after normalization.

    Example:
        from src_reviews_cleaning.pipeline import run
        df = run()
        df = run(save_csv=True)
    """
    # Load raw data from master CSV and split by source
    print("Step 1/4: Loading raw data...")
    raw = load_all_raw()
    for source, frame in raw.items():
        print(f"  {source}: {frame.shape[0]} rows")

    # Normalize each source to the standard column schema
    print("Step 2/4: Normalizing columns...")
    combined = normalize_all(raw)
    print(f"  Combined shape: {combined.shape}")

    # Clean all columns including language detection and translation
    print("Step 3/4: Cleaning data (includes language detection + translation)...")
    cleaned = clean_all(combined)

    # Remove duplicate reviews using exact and fuzzy matching
    print("Step 4/4: Removing duplicates (exact + fuzzy)...")
    final = remove_duplicates(cleaned)

    # Reorder columns to the standard final order
    final = final[FINAL_COLUMN_ORDER]
    print(f"Pipeline complete. Final shape: {final.shape}")

    if save_csv:
        Path(PROCESSED_DIR).mkdir(parents=True, exist_ok=True)
        output_path = Path(PROCESSED_DIR) / "all_reviews_cleaned.csv"
        final.to_csv(output_path, index=False)
        print(f"Saved to: {output_path}")

    return final