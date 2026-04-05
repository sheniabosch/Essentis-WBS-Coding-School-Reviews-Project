# src_reviews_cleaning

A modular Python data cleaning pipeline for WBS Coding School student reviews.
Built as part of an internship project at Essentis, February 2026.

---

## Quick Start
```python
from src_reviews_cleaning.pipeline import run
df = run()
```

Returns a fully cleaned pandas DataFrame with approximately 843 rows and 18 columns.
To also save the output as a CSV:
```python
df = run(save_csv=True)
```

Note: the pipeline includes automatic language detection, translation, and fuzzy
deduplication. With approximately 41 non-English reviews at a 2 second delay per
translation, and fuzzy matching across author/date groups, expect the pipeline to
take approximately 90-120 seconds to complete.

---

## File Structure
```
src_reviews_cleaning/
├── __init__.py          makes the folder an importable Python package
├── config.py            all paths, column names, and constants
├── loaders.py           loads and splits the master raw CSV
├── normalizers.py       renames columns to standard schema and strips emojis
├── cleaners.py          cleans all columns and translates non-English reviews
├── deduplicator.py      removes exact and fuzzy duplicate reviews
├── pipeline.py          orchestrates all steps and returns the final DataFrame
├── requirements.txt     Python dependencies
└── README.md            this file
```

---

## Raw Data

One file is loaded from:
    essentis_intern_drive/data/raw/

| File                   | Rows | Notes                                               |
|------------------------|------|-----------------------------------------------------|
| master_reviews_raw.csv | 846  | All three original sources concatenated into one    |
|                        |      | file. Sources identified by the data_source column. |

The master CSV contains all 33 columns from all three original sources side by side.
Rows from each source have NaN in columns that belong to other sources.

| data_source | Rows | Original source                                    |
|-------------|------|----------------------------------------------------|
| google      | 256  | Scraped from Google Maps (February 2025)           |
| clean       | 541  | Previously cleaned reviews from a prior intern     |
| new         |  49  | Most recent batch of reviews                       |

---

## Final Column Schema

| Column                  | Type       | Description                                         |
|-------------------------|------------|-----------------------------------------------------|
| batch_id                | float64    | Encrypted reviewer ID for data privacy              |
| author                  | object     | Name of the reviewer                                |
| data_source             | object     | Which source this row came from (google/clean/new)  |
| source                  | object     | Website the review was posted on                    |
| review_date             | datetime64 | Date the review was posted                          |
| overall_experience      | float64    | Row-wise average of all four ratings (calculated)   |
| review                  | float64    | Single star rating given by the reviewer (1-5)      |
| instructors             | float64    | Rating of the instructors (1-5)                     |
| curriculum              | float64    | Rating of the curriculum (1-5)                      |
| job_assistance          | float64    | Rating of job placement assistance (1-5)            |
| review_text             | object     | Full written review (title + body combined)         |
| review_text_translated  | object     | English translation of non-English reviews only     |
| course_format           | object     | full-time or part-time                              |
| course                  | object     | Standardised course name                            |
| role                    | object     | Reviewer role category                              |
| verified                | boolean    | Whether the reviewer identity was verified          |
| verification_source     | object     | Who verified the reviewer identity                  |
| link                    | object     | URL to the original review                          |

---

## What Each Module Does

### config.py
Single source of truth for all paths, column names, and constants. If a column
name or file path ever needs to change, change it here only. Never hardcode column
name strings anywhere else in the pipeline.

Key constants:
    DATE_FORMAT           YYYYMMDD — ensures chronological string sorting
    FUZZY_MATCH_THRESHOLD 80 — similarity percentage above which two reviews
                               from the same author on the same date are
                               considered duplicates

### loaders.py
Reads master_reviews_raw.csv once and splits it into three subsets by data_source.
Returns a dict with keys "google", "clean", and "new". No cleaning happens here.

### normalizers.py
Each source has its own normalizer that:
    - Keeps only the columns relevant to that source to avoid duplicates
    - Strips all emoji characters from every text column
    - Combines title + review body into a single review_text field (clean + new)
    - Renames raw column names to the standard schema from config.py
    - Adds any missing standard columns as NaN

All three normalized DataFrames are concatenated in normalize_all().

### cleaners.py
Cleans values within columns in this sequence:

    1. clean_empty_strings         converts empty strings to NaN
    2. clean_review_dates          parses all dates to datetime64
                                   relative Google dates use GOOGLE_SCRAPE_DATE anchor
    3. detect_and_translate        detects non-English reviews using lingua
                                   and translates them using deep-translator
                                   2 second delay between calls avoids rate limiting
    4. clean_ratings               standardizes all four numeric ratings to float
                                   handles European comma decimals (e.g. 4,7 -> 4.7)
                                   clamps values to 1-5 range
    5. calculate_overall_experience row-wise average of all four ratings
    6. clean_verified              normalizes to pandas BooleanDtype
    7. clean_course_format         standardizes to full-time / part-time / NaN
    8. clean_course                maps raw names to five standard course names
    9. clean_role                  categorizes job titles into eight role categories

### deduplicator.py
Removes duplicate reviews using a two-stage approach:

    Stage 1 — Exact deduplication:
        Removes rows where author + review_date + review_text are all identical.
        The row with the fewest NaN values is kept from each duplicate group.

    Stage 2 — Fuzzy deduplication:
        Within each author + review_date group, compares review texts using
        token sort ratio fuzzy matching. Reviews with similarity above
        FUZZY_MATCH_THRESHOLD (80%) are treated as duplicates.
        Token sort ratio handles word reordering and minor grammar differences.
        The more complete row is kept from each fuzzy duplicate group.

### pipeline.py
Calls all modules in order and returns the final cleaned DataFrame.
This is the only file notebooks need to import.

---

## Course Categories

    Full-Stack Web & App
    Full-Stack PHP Development
    Data Science
    Product Design
    Marketing Analytics

Unrecognized or missing course values are set to NaN.
To add a new course add an entry to COURSE_MAP in cleaners.py.

---

## Role Categories

    Graduate     graduates and alumni
    Student      current students
    Applicant    prospective students
    Developer    web, software, or full-stack developers and engineers
    Data Science data scientists, analysts, and ML/AI professionals
    Designer     any design role
    Manager      managers and consultants
    Other        anything that does not match the above

---

## Language Detection and Translation

Reviews are automatically detected for language using lingua.
Any review not written in English is translated using deep-translator
(Google Translate backend, free, no API key required).

Current dataset language breakdown:
    English  784 reviews  no translation needed
    German    40 reviews  translated
    French     1 review   translated
    Unknown   21 reviews  empty strings, skipped

Original review_text is never modified.
Translated text is stored in review_text_translated.
English reviews have NaN in review_text_translated.

Note: deep-translator is a free service with rate limits. A 2 second delay
is added between each translation call. If the dataset grows significantly
consider upgrading to the Google Cloud Translation API.

---

## Emoji Handling

All emoji characters are stripped from every text column during normalization.
This prevents rendering issues in matplotlib charts, CSV exports, and the
Looker Studio dashboard. The stripping happens in normalizers.py before any
other processing so downstream code never encounters emoji characters.

---

## Important Notes

1. GOOGLE_SCRAPE_DATE in cleaners.py is set to datetime(2025, 2, 1).
   If Google data is re-scraped update this date to match the new scrape date.
   Forgetting to do this will produce incorrect dates for all Google reviews.

2. verified is NaN for all Google reviews.
   Google does not provide verification data. This is expected.

3. instructors, role, and verified have approximately 577 nulls each.
   These all come from the Google source which does not collect this data.

4. review_text_translated is NaN for all English reviews.
   Only non-English reviews have a translated value. This is expected.

5. overall_experience for Google reviews is based only on the review column.
   Google does not collect instructors, curriculum, or job_assistance ratings
   so the row-wise average for Google rows uses only the one available rating.

---

## How To Add A New Data Source

1. Add the new source rows to master_reviews_raw.csv with a unique data_source label.
2. Add a KEEP list for the new source columns in normalizers.py.
3. Add a new normalizer function in normalizers.py following the existing pattern.
4. Add the new normalizer call to normalize_all() in normalizers.py.
5. Update load_all_raw() in loaders.py to split the new source label.
6. Run the full pipeline and check the output shape and null counts.

No changes are needed in cleaners.py, deduplicator.py, or pipeline.py
as long as the new source maps to the same standard column schema.

---

## How To Update

Adding a new course        add entry to COURSE_MAP in cleaners.py
Adding a new role category add entry to ROLE_KEYWORDS in cleaners.py
Re-scraping Google data    update GOOGLE_SCRAPE_DATE in cleaners.py
Changing fuzzy threshold   update FUZZY_MATCH_THRESHOLD in config.py
Adding new reviews         add rows to master_reviews_raw.csv with correct data_source

---

## Dependencies

    pandas>=2.0.0
    numpy>=1.24.0
    vaderSentiment>=3.3.2
    lingua-language-detector>=2.0.0
    deep-translator>=1.11.0
    thefuzz>=0.20.0
    python-Levenshtein>=0.21.0
    openpyxl>=3.1.0

Install with:
    !pip install -r /content/drive/Shareddrives/essentis_intern_drive/data/src_reviews_cleaning/requirements.txt