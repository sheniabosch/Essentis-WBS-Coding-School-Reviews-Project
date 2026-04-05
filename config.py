from pathlib import Path
from typing import Final

# =============================================================================
# DIRECTORY PATHS
# Base directory for all data on the shared drive.
# All other paths are built relative to this.
# =============================================================================
BASE_DIR      = Path("/content/drive/Shareddrives/essentis_intern_drive")
RAW_DATA_DIR  = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

# =============================================================================
# FILE PATHS
# Input and output file locations.
# PATH_MASTER_CSV is the single source of truth for raw data.
# =============================================================================
PATH_MASTER_CSV    = RAW_DATA_DIR / "master_reviews_raw.csv"
PATH_FINAL_CLEANED = PROCESSED_DIR / "all_reviews_cleaned.csv"

# =============================================================================
# COLUMN NAMES
# All column names used throughout the pipeline are defined here.
# Never hardcode column name strings anywhere else in the pipeline.
# =============================================================================
COL_BATCH_ID:               Final[str] = "batch_id"
COL_AUTHOR:                 Final[str] = "author"
COL_DATA_SOURCE:            Final[str] = "data_source"
COL_REVIEW_SOURCE:          Final[str] = "source"
COL_REVIEW_DATE:            Final[str] = "review_date"
COL_OVERALL_EXPERIENCE:     Final[str] = "overall_experience"
COL_REVIEW_RATING:          Final[str] = "review"
COL_INSTRUCTORS:            Final[str] = "instructors"
COL_CURRICULUM:             Final[str] = "curriculum"
COL_JOB_ASSISTANCE:         Final[str] = "job_assistance"
COL_REVIEW_TEXT:            Final[str] = "review_text"
COL_REVIEW_TEXT_TRANSLATED: Final[str] = "review_text_translated"
COL_COURSE:                 Final[str] = "course"
COL_COURSE_FORMAT:          Final[str] = "course_format"
COL_ROLE:                   Final[str] = "role"
COL_VERIFIED:               Final[str] = "verified"
COL_VERIFICATION_SOURCE:    Final[str] = "verification_source"
COL_LINK:                   Final[str] = "link"

# =============================================================================
# RATING COLUMNS
# The four numeric rating columns used to calculate overall_experience.
# =============================================================================
RATING_COLUMNS: Final[list] = [
    COL_REVIEW_RATING,
    COL_INSTRUCTORS,
    COL_CURRICULUM,
    COL_JOB_ASSISTANCE,
]

# =============================================================================
# VALIDATION CONSTANTS
# =============================================================================
MIN_RATING:  Final[int] = 1
MAX_RATING:  Final[int] = 5

# Date format used throughout the pipeline.
# YYYYMMDD format ensures consistent chronological sorting as a string.
DATE_FORMAT: Final[str] = "%Y%m%d"

# Fuzzy matching threshold for deduplication.
# Reviews with the same author and date whose text similarity exceeds
# this threshold are considered duplicates.
FUZZY_MATCH_THRESHOLD: Final[int] = 80