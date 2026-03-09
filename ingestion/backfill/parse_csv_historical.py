"""
parse_csv_historical.py
-----------------------
Parse and normalize IDFM historical validation CSV files
into a clean DataFrame matching the raw_validations BigQuery schema.

Handles multiple source formats:
  - 2023 S1: tab-separated, latin-1, date DD/MM/YYYY, ZDC column 'lda'
  - 2023 S2: tab-separated, UTF-16, date DD/MM/YYYY, ZDC column 'id_zdc'
  - 2024 S1: tab-separated, latin-1, date DD/MM/YYYY, ZDC column 'id_zdc'
  - 2024 T3: tab-separated, UTF-8, date DD/MM/YY, ZDC column 'id_zdc'
  - 2024 T4: semicolon-separated, UTF-8, date '20 octobre 2024', ZDC column 'id_zdc'

Usage:
    python parse_csv_historical.py --file /path/to/2023_S1_NB_FER.txt
    python parse_csv_historical.py --file /path/to/2024_T3_NB_FER.txt --preview
"""

import argparse
import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# Target schema matching raw_validations BigQuery table
TARGET_COLUMNS = [
    "jour",
    "code_stif_trns",
    "code_stif_res",
    "code_stif_arret",
    "libelle_arret",
    "id_zdc",
    "categorie_titre",
    "nb_vald",
]

# French month names -> zero-padded numbers (used for 2024 T4 date format)
FRENCH_MONTHS = {
    "janvier": "01",
    "février": "02",
    "mars": "03",
    "avril": "04",
    "mai": "05",
    "juin": "06",
    "juillet": "07",
    "août": "08",
    "septembre": "09",
    "octobre": "10",
    "novembre": "11",
    "décembre": "12",
}


def detect_encoding(filepath: Path) -> str:
    """
    Detect file encoding by checking BOM first, then attempting UTF-8 decode.
    - UTF-16 LE/BE BOM : FF FE / FE FF
    - UTF-8 BOM        : EF BB BF
    - UTF-8 no BOM     : try decoding a sample (e.g. 2024_T3 files)
    - Default          : latin-1 (safe fallback for old IDFM files)
    """
    with open(filepath, "rb") as f:
        raw = f.read(4096)

    if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
        logger.info("Encoding detected: UTF-16")
        return "utf-16"
    if raw[:3] == b"\xef\xbb\xbf":
        logger.info("Encoding detected: UTF-8 BOM")
        return "utf-8-sig"

    try:
        raw.decode("utf-8")
        logger.info("Encoding detected: UTF-8 (no BOM)")
        return "utf-8"
    except UnicodeDecodeError:
        pass

    logger.info("Encoding detected: latin-1 (default)")
    return "latin-1"


def detect_format(filepath: Path) -> dict:
    """
    Detect the CSV format by reading the first two lines.
    Returns a dict with parsing parameters: separator, date_format, zdc_col, encoding.
    """
    encoding = detect_encoding(filepath)

    with open(filepath, encoding=encoding, errors="replace") as f:
        header = f.readline().strip()
        sample = f.readline().strip()

    logger.info(f"Detected header: {repr(header[:100])}")
    logger.info(f"First data row: {repr(sample[:100])}")

    # Detect separator
    if "\t" in header:
        separator = "\t"
        logger.info("Separator: tab")
    else:
        separator = ";"
        logger.info("Separator: semicolon")

    # # Detect date format from first data row
    # first_value = sample.split(separator)[0].strip()
    # if "/" in first_value:
    #     date_format = "dd/mm/yyyy"    # e.g. 01/01/2023 or 01/07/24
    # else:
    #     date_format = "fr_long"       # e.g. 20 octobre 2024
    # Detect date format from first data row
    first_value = sample.split(separator)[0].strip()
    if "/" in first_value:
        date_format = "dd/mm/yyyy"  # e.g. 01/01/2023 or 01/07/24
    elif "-" in first_value and len(first_value) == 10:
        date_format = "iso"  # e.g. 2024-10-01
    else:
        date_format = "fr_long"  # e.g. 20 octobre 2024

    # Detect ZDC column name
    zdc_col = "lda" if "lda" in header.lower() else "ID_ZDC"

    return {
        "separator": separator,
        "date_format": date_format,
        "zdc_col": zdc_col,
        "encoding": encoding,
    }


def parse_date_fr_long(date_str: str) -> str:
    """
    Convert French long date format to ISO 8601.
    Example: '20 octobre 2024' -> '2024-10-20'
    """
    parts = str(date_str).strip().lower().split()
    if len(parts) != 3:
        return None
    day, month_fr, year = parts
    month = FRENCH_MONTHS.get(month_fr)
    if not month:
        logger.warning(f"Unrecognized French month: {month_fr}")
        return None
    return f"{year}-{month}-{day.zfill(2)}"


def normalize_dataframe(df: pd.DataFrame, fmt: dict) -> pd.DataFrame:
    """
    Normalize a raw DataFrame into the TARGET_COLUMNS schema.

    Steps:
    - Lowercase all column names and strip whitespace
    - Rename ZDC column to 'id_zdc' if needed
    - Parse dates to Python date objects (handles 4-digit and 2-digit year)
    - Replace missing/unknown sentinel values
    - Normalize string columns: strip + uppercase
    - Cast numeric columns to nullable Int64
    - Drop source-level duplicates
    - Drop rows with invalid dates
    """
    # Lowercase column names and strip whitespace
    df.columns = [col.strip().lower() for col in df.columns]
    logger.info(f"Raw columns: {list(df.columns)}")

    # Rename ZDC column if needed
    zdc_col = fmt["zdc_col"].lower()
    if zdc_col in df.columns and zdc_col != "id_zdc":
        df = df.rename(columns={zdc_col: "id_zdc"})

    # # Parse dates — handle both 4-digit (DD/MM/YYYY) and 2-digit (DD/MM/YY) year formats
    # if fmt["date_format"] == "dd/mm/yyyy":
    #     jour_raw = df["jour"].str.strip()
    #     parsed = pd.to_datetime(jour_raw, format="%d/%m/%Y", errors="coerce")
    #     mask = parsed.isna()
    #     if mask.any():
    #         logger.info(f"Retrying {mask.sum()} dates with 2-digit year format (%d/%m/%y)")
    #         parsed[mask] = pd.to_datetime(jour_raw[mask], format="%d/%m/%y", errors="coerce")
    #     df["jour"] = parsed.dt.date
    # else:
    #     df["jour"] = df["jour"].apply(parse_date_fr_long)
    #     df["jour"] = pd.to_datetime(df["jour"], errors="coerce").dt.date

    # Parse dates — handle DD/MM/YYYY, DD/MM/YY, YYYY-MM-DD, and French long format
    if fmt["date_format"] == "dd/mm/yyyy":
        jour_raw = df["jour"].str.strip()
        parsed = pd.to_datetime(jour_raw, format="%d/%m/%Y", errors="coerce")
        mask = parsed.isna()
        if mask.any():
            logger.info(
                f"Retrying {mask.sum()} dates with 2-digit year format (%d/%m/%y)"
            )
            parsed[mask] = pd.to_datetime(
                jour_raw[mask], format="%d/%m/%y", errors="coerce"
            )
        df["jour"] = parsed.dt.date
    elif fmt["date_format"] == "iso":
        df["jour"] = pd.to_datetime(
            df["jour"].str.strip(),
            format="%Y-%m-%d",
            errors="coerce",
        ).dt.date
    else:
        df["jour"] = df["jour"].apply(parse_date_fr_long)
        df["jour"] = pd.to_datetime(df["jour"], errors="coerce").dt.date

    # Replace unknown/missing sentinel values
    df["id_zdc"] = df["id_zdc"].replace("?", None)

    # Normalize string columns: strip whitespace + uppercase for consistency across files
    df["libelle_arret"] = df["libelle_arret"].str.strip().str.upper()
    df["categorie_titre"] = (
        df["categorie_titre"].str.strip().str.upper().replace("?", "NON DEFINI")
    )

    # # Cast numeric columns to nullable Int64 (handles NaN without float conversion)
    # for col in ["code_stif_trns", "code_stif_res", "code_stif_arret", "id_zdc", "nb_vald"]:
    #     if col in df.columns:
    #         df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    # Cast numeric columns to nullable Int64
    # nb_vald may contain thousands separators (e.g. '1 268') — strip spaces first
    for col in [
        "code_stif_trns",
        "code_stif_res",
        "code_stif_arret",
        "id_zdc",
        "nb_vald",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(
                (
                    df[col].str.replace(" ", "", regex=False)
                    if df[col].dtype == object
                    else df[col]
                ),
                errors="coerce",
            ).astype("Int64")

    # Drop source-level duplicates before loading
    before = len(df)
    df = df.drop_duplicates(
        subset=["jour", "code_stif_arret", "categorie_titre"],
        keep="last",
    )
    dupes_removed = before - len(df)
    if dupes_removed > 0:
        logger.info(f"Dropped {dupes_removed} source-level duplicates")

    # Drop rows with unparseable dates
    invalid_dates = df["jour"].isna().sum()
    if invalid_dates > 0:
        logger.warning(f"Dropping {invalid_dates} rows with invalid dates")
        df = df.dropna(subset=["jour"])

    # Validate all target columns are present
    missing_cols = [c for c in TARGET_COLUMNS if c not in df.columns]
    if missing_cols:
        raise ValueError(f"Missing columns after normalization: {missing_cols}")

    df = df[TARGET_COLUMNS]

    logger.info(
        f"Normalized DataFrame: {len(df)} rows, {df['jour'].min()} -> {df['jour'].max()}"
    )

    return df


def parse_file(filepath: str | Path) -> pd.DataFrame:
    """
    Main entry point: parse an IDFM historical CSV file and return
    a normalized DataFrame ready for BigQuery loading.
    """
    filepath = Path(filepath)

    if not filepath.exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    logger.info(f"Parsing: {filepath.name}")

    fmt = detect_format(filepath)

    try:
        df = pd.read_csv(
            filepath,
            sep=fmt["separator"],
            encoding=fmt["encoding"],
            dtype=str,
            on_bad_lines="warn",
        )
    except UnicodeDecodeError:
        logger.warning("Encoding fallback: retrying with latin-1")
        df = pd.read_csv(
            filepath,
            sep=fmt["separator"],
            encoding="latin-1",
            dtype=str,
            on_bad_lines="warn",
        )

    logger.info(f"Raw rows loaded: {len(df)}")

    df = normalize_dataframe(df, fmt)

    return df


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Parse an IDFM historical CSV file into a normalized DataFrame"
    )
    parser.add_argument("--file", required=True, help="Path to the CSV/TXT source file")
    parser.add_argument("--preview", action="store_true", help="Print first 10 rows")
    args = parser.parse_args()

    df = parse_file(args.file)

    print(f"\n✅ Parsing successful: {len(df)} rows")
    print(f"   Period : {df['jour'].min()} -> {df['jour'].max()}")
    print(f"   Columns: {list(df.columns)}")
    print(f"   Types  :\n{df.dtypes}")

    if args.preview:
        print(f"\nPreview:\n{df.head(10).to_string()}")

    # Deduplication check on natural key
    dupes = df.duplicated(subset=["jour", "code_stif_arret", "categorie_titre"]).sum()
    if dupes > 0:
        print(
            f"\n⚠️  {dupes} duplicates found on key (jour, code_stif_arret, categorie_titre)"
        )
    else:
        print("\n✅ No duplicates on deduplication key")
