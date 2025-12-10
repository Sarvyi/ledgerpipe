# save as src/extract_and_standardize.py
import re
from pathlib import Path
import pandas as pd
import pdfplumber
from datetime import datetime
import logging
import sys
from colorama import init as colorama_init, Fore, Style

# ---------- PATH SETUP (project-root aware) ----------

# src/ is one level below project root
ROOT_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = ROOT_DIR / "data"      # put your xls/xlsx/pdf here
OUTPUT_DIR = ROOT_DIR / "output"  # CSV will be written here
OUTPUT_CSV = "combined_transactions.csv"

# ---------- LOGGING SETUP ----------

colorama_init(autoreset=True)

class ColorFormatter(logging.Formatter):
    LEVEL_COLORS = {
        logging.DEBUG: Fore.BLUE,
        logging.INFO: Fore.CYAN,
        logging.WARNING: Fore.YELLOW,
        logging.ERROR: Fore.RED,
        logging.CRITICAL: Fore.RED + Style.BRIGHT,
    }

    def format(self, record):
        color = self.LEVEL_COLORS.get(record.levelno, "")
        reset = Style.RESET_ALL
        message = super().format(record)
        return f"{color}[{record.levelname}] {reset}{message}"

def get_logger(name="fin_ledger"):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = ColorFormatter("%(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

log = get_logger()

# ---------- SCHEMA & MAPPINGS ----------

CANONICAL_COLS = [
    "Date",
    "Narration",
    "Chq./Ref.No.",
    "Value Dt",
    "Withdrawal Amt.",
    "Deposit Amt.",
    "Closing Balance",
    "Bank"
]

EXCEL_COL_MAP = {
    "date": "Date",
    "transaction date": "Date",
    "txn date": "Date",
    "txn. date": "Date",

    "value date": "Value Dt",
    "value dt": "Value Dt",
    "value dt.": "Value Dt",

    "narration": "Narration",
    "details": "Narration",
    "description": "Narration",
    "particulars": "Narration",
    "transaction details": "Narration",

    "chq./ref.no.": "Chq./Ref.No.",
    "chq/ref no": "Chq./Ref.No.",
    "ref no./cheque no.": "Chq./Ref.No.",
    "ref no./cheque no": "Chq./Ref.No.",
    "ref no": "Chq./Ref.No.",
    "cheque no": "Chq./Ref.No.",
    "chq./ref. no.": "Chq./Ref.No.",

    "withdrawal amt.": "Withdrawal Amt.",
    "withdrawal amount": "Withdrawal Amt.",
    "debit": "Withdrawal Amt.",
    "dr": "Withdrawal Amt.",
    "dr.": "Withdrawal Amt.",

    "deposit amt.": "Deposit Amt.",
    "deposit amount": "Deposit Amt.",
    "credit": "Deposit Amt.",
    "cr": "Deposit Amt.",
    "cr.": "Deposit Amt.",

    "closing balance": "Closing Balance",
    "balance": "Closing Balance",
    "balance inr": "Closing Balance",
}

DATE_PATTERNS = [
    r"\d{2}/\d{2}/\d{2,4}",
    r"\d{2}-\d{2}-\d{2,4}",
]
date_regex = re.compile(r"^(" + r"|".join(DATE_PATTERNS) + r")")

# ---------- HELPERS ----------

def parse_date(s):
    if not isinstance(s, str):
        s = str(s)
    s = s.strip()
    for fmt in ("%d/%m/%y", "%d/%m/%Y", "%d-%m-%y", "%d-%m-%Y"):
        try:
            dt = datetime.strptime(s, fmt)
            return dt.strftime("%Y-%m-%d")
        except Exception:
            pass
    try:
        dt = pd.to_datetime(s, dayfirst=True)
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return s

def clean_amount(s):
    """Clean amount values - handles commas, currency symbols, etc."""
    if pd.isna(s):
        return None
    if isinstance(s, (int, float)):
        return float(s)
    s = str(s).strip()
    if s == "" or s == "-":
        return None
    # Remove commas, currency symbols, and other non-numeric characters except . and -
    s = s.replace(",", "")
    s = re.sub(r"[^\d\.\-]", "", s)
    try:
        return float(s)
    except Exception:
        return None

def standardize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Standardize DataFrame to canonical column format"""
    # Create a copy to avoid modifying original
    df = df.copy()
    
    # Remove completely empty rows
    df = df.dropna(how='all')
    
    if len(df) == 0:
        log.warning("  DataFrame is completely empty after dropping null rows")
        return pd.DataFrame(columns=CANONICAL_COLS)
    
    # Try to find the header row if data has pre-header content
    # Look for rows that contain "date" or similar header keywords
    header_found = False
    for idx in range(min(50, len(df))):  # Check first 50 rows
        row = df.iloc[idx]
        row_str = ' '.join([str(val).lower().strip() for val in row if pd.notna(val)])
        
        # Check for various header patterns
        has_date = any(keyword in row_str for keyword in ['date', 'txn date', 'transaction date'])
        has_amount = any(keyword in row_str for keyword in ['debit', 'credit', 'withdrawal', 'deposit', 'balance'])
        
        if has_date and has_amount:
            log.info(f"  Found header at row {idx}: {row_str[:100]}")
            # Set this row as column headers
            df.columns = df.iloc[idx]
            # Keep only rows after the header
            df = df.iloc[idx + 1:].reset_index(drop=True)
            header_found = True
            break
    
    if not header_found:
        log.warning("  Could not auto-detect header row, using existing columns")
        log.info(f"  Current columns: {list(df.columns)[:10]}")
    
    # Clean column names (remove extra spaces, newlines)
    df.columns = [str(col).strip().replace('\n', ' ') for col in df.columns]
    
    # Map columns to canonical names
    col_map = {}
    for c in df.columns:
        key = str(c).strip().lower()
        if key in EXCEL_COL_MAP:
            col_map[c] = EXCEL_COL_MAP[key]
            log.info(f"  Mapped column: '{c}' -> '{EXCEL_COL_MAP[key]}'")
    
    if not col_map:
        log.warning("  No columns matched! Available columns:")
        for c in df.columns:
            log.warning(f"    - '{c}' (lowercased: '{str(c).strip().lower()}')")
    
    df = df.rename(columns=col_map)

    # Add missing canonical columns
    for c in CANONICAL_COLS:
        if c not in df.columns:
            df[c] = None

    # Parse dates
    df["Date"] = df["Date"].apply(lambda x: parse_date(x) if pd.notna(x) else None)
    df["Value Dt"] = df["Value Dt"].apply(lambda x: parse_date(x) if pd.notna(x) else None)

    # Clean amounts
    for amt_col in ["Withdrawal Amt.", "Deposit Amt.", "Closing Balance"]:
        df[amt_col] = df[amt_col].apply(clean_amount)

    # Filter out rows without valid dates (likely header/footer rows)
    before_filter = len(df)
    df = df[df["Date"].notna()].reset_index(drop=True)
    after_filter = len(df)
    
    if before_filter > after_filter:
        log.info(f"  Filtered {before_filter - after_filter} rows without valid dates")
    
    # Return only canonical columns
    return df[CANONICAL_COLS]

# ---------- EXCEL PROCESSING ----------

def process_excel(path: Path, bank_name=None) -> pd.DataFrame:
    """Process Excel files with better error handling and header detection"""
    suffix = path.suffix.lower()

    # Default bank name from filename (without extension)
    if bank_name is None:
        bank_name = path.stem

    # FIRST: Check if it's actually a text/HTML file disguised as Excel
    try:
        with open(path, "rb") as f:
            first_bytes = f.read(100)
        
        # Check if it starts with text characters (not binary Excel format)
        is_text_file = first_bytes.startswith(b'Account') or \
                       first_bytes.startswith(b'<html') or \
                       first_bytes.startswith(b'<HTML') or \
                       first_bytes.startswith(b'<!DOCTYPE') or \
                       b'<table' in first_bytes.lower()
        
        if is_text_file:
            log.info(f"  Detected text/HTML file disguised as {suffix}: {path.name}")
            return process_text_as_excel(path, bank_name=bank_name)
    except Exception as e:
        log.warning(f"  Could not check file format: {e}")

    # Try reading as actual Excel
    try:
        if suffix == ".xls":
            df_dict = pd.read_excel(path, sheet_name=None, engine="xlrd", header=None)
        elif suffix == ".xlsx":
            df_dict = pd.read_excel(path, sheet_name=None, engine="openpyxl", header=None)
        else:
            log.warning(f"Unknown Excel extension for {path.name}, trying default engine")
            df_dict = pd.read_excel(path, sheet_name=None, header=None)

        # Concatenate all sheets
        df = pd.concat(df_dict.values(), ignore_index=True, sort=False)
        log.info(f"  Raw data shape: {df.shape}")
        
        # Standardize will handle header detection and column mapping
        df = standardize_df(df)
        df["Bank"] = bank_name
        log.info(f"✓ Parsed as real Excel: {path.name} ({len(df)} rows)")
        return df

    except Exception as e:
        log.warning(f"Failed to read as Excel ({path.name}): {str(e)[:100]}")
        # Fall back to text processing
        return process_text_as_excel(path, bank_name=bank_name)


def process_text_as_excel(path: Path, bank_name=None) -> pd.DataFrame:
    """Process text/HTML files that are disguised as Excel files"""
    if bank_name is None:
        bank_name = path.stem

    try:
        # First try HTML parsing
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            content = f.read()
        
        if "<html" in content.lower() or "<table" in content.lower():
            log.info(f"  Trying HTML table extraction...")
            html_tables = pd.read_html(path)
            if html_tables:
                df = pd.concat(html_tables, ignore_index=True, sort=False)
                df = standardize_df(df)
                df["Bank"] = bank_name
                log.info(f"✓ Parsed HTML-table as {path.suffix}: {path.name} ({len(df)} rows)")
                return df
    except Exception as e:
        log.warning(f"  HTML parsing failed: {e}")
    
    # Try reading as tab-separated or whitespace-separated text
    try:
        log.info(f"  Trying tab-delimited text parsing...")
        df = pd.read_csv(path, sep='\t', encoding='utf-8', engine='python', 
                        on_bad_lines='skip', header=None)
        
        if len(df) > 0:
            df = standardize_df(df)
            if len(df) > 0:
                df["Bank"] = bank_name
                log.info(f"✓ Parsed as tab-delimited text: {path.name} ({len(df)} rows)")
                return df
    except Exception as e:
        log.warning(f"  Tab-delimited parsing failed: {e}")
    
    # Try reading as CSV with multiple delimiters
    for delimiter in [',', '|', ';']:
        try:
            log.info(f"  Trying delimiter '{delimiter}'...")
            df = pd.read_csv(path, sep=delimiter, encoding='utf-8', engine='python',
                           on_bad_lines='skip', header=None)
            if len(df) > 0 and len(df.columns) > 3:
                df = standardize_df(df)
                if len(df) > 0:
                    df["Bank"] = bank_name
                    log.info(f"✓ Parsed with delimiter '{delimiter}': {path.name} ({len(df)} rows)")
                    return df
        except Exception:
            continue
    
    log.error(f"✗ All parsing methods failed for {path.name}")
    return pd.DataFrame(columns=CANONICAL_COLS)

# ---------- PDF PROCESSING ----------

def parse_pdf_text_lines_to_rows(lines):
    rows = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        m = date_regex.match(line)
        if not m:
            continue
        date_token = m.group(0)
        rest = line[m.end():].strip()
        parts = re.split(r"\s{2,}|\t", rest)
        parts = [p.strip() for p in parts if p.strip() != ""]

        row = {c: None for c in CANONICAL_COLS}
        row["Date"] = parse_date(date_token)
        if len(parts) >= 1:
            row["Narration"] = parts[0]

        trailing = parts[-3:] if len(parts) >= 3 else parts[1:]
        amounts = [clean_amount(x) for x in trailing]

        if len(trailing) >= 3:
            row["Withdrawal Amt."] = amounts[-3]
            row["Deposit Amt."] = amounts[-2]
            row["Closing Balance"] = amounts[-1]
        elif len(trailing) == 2:
            row["Closing Balance"] = amounts[-1]
            row["Withdrawal Amt."] = amounts[-2] if amounts[-2] is not None else None
        elif len(trailing) == 1:
            row["Closing Balance"] = amounts[-1]

        if len(parts) >= 2:
            for p in parts[1:3]:
                if re.search(r"\d{6,}", p):
                    row["Chq./Ref.No."] = p
                elif re.match(r"\d{2}/\d{2}/\d{2,4}", p) or re.match(r"\d{2}-\d{2}-\d{2,4}", p):
                    row["Value Dt"] = parse_date(p)

        rows.append(row)
    return rows

def process_pdf(path: Path, bank_name=None) -> pd.DataFrame:
    if bank_name is None:
        bank_name = path.stem

    all_rows = []
    try:
        with pdfplumber.open(path) as pdf:
            for page in pdf.pages:
                try:
                    table = page.extract_table()
                except Exception:
                    table = None

                if table and len(table) > 1:
                    header = table[0]
                    data_rows = table[1:]
                    df_page = pd.DataFrame(data_rows, columns=header)
                    df_page = standardize_df(df_page)
                    df_page["Bank"] = bank_name
                    all_rows.append(df_page)
                else:
                    text = page.extract_text() or ""
                    lines = text.splitlines()
                    parsed_rows = parse_pdf_text_lines_to_rows(lines)
                    if parsed_rows:
                        df_page = pd.DataFrame(parsed_rows)
                        df_page["Bank"] = bank_name
                        all_rows.append(df_page)
    except Exception as e:
        log.error(f"Failed to process pdf {path}: {e}")

    if all_rows:
        result = pd.concat(all_rows, ignore_index=True, sort=False)
        log.info(f"✓ Parsed PDF: {path.name} ({len(result)} rows)")
        return result
    else:
        log.warning(f"✗ No data extracted from PDF: {path.name}")
        return pd.DataFrame(columns=CANONICAL_COLS)

# ---------- FOLDER PROCESSING ----------

def process_folder(folder: Path) -> pd.DataFrame:
    """Process all Excel and PDF files in folder"""
    if not folder.exists():
        raise FileNotFoundError(f"{folder} not found")

    all_dfs = []
    file_count = 0
    
    log.info("=" * 70)
    log.info("Starting file processing...")
    log.info("=" * 70)
    
    for f in sorted(folder.iterdir()):
        if f.is_dir():
            continue

        suffix = f.suffix.lower()
        if suffix in [".xls", ".xlsx"]:
            file_count += 1
            log.info(f"\n[{file_count}] Processing Excel: {f.name}")
            df = process_excel(f, bank_name=f.stem)
            
            # CRITICAL: Check length, not empty property
            if len(df) > 0:
                all_dfs.append(df)
                log.info(f"    → Added to collection (running total: {len(all_dfs)} files, {sum(len(d) for d in all_dfs)} rows)")
            else:
                log.warning(f"    → Skipped (no valid rows)")
                
        elif suffix == ".pdf":
            file_count += 1
            log.info(f"\n[{file_count}] Processing PDF: {f.name}")
            df = process_pdf(f, bank_name=f.stem)
            
            # CRITICAL: Check length, not empty property
            if len(df) > 0:
                all_dfs.append(df)
                log.info(f"    → Added to collection (running total: {len(all_dfs)} files, {sum(len(d) for d in all_dfs)} rows)")
            else:
                log.warning(f"    → Skipped (no valid rows)")
        else:
            log.debug(f"Skipping unsupported file: {f.name}")

    log.info("\n" + "=" * 70)
    log.info(f"Processing complete: {len(all_dfs)} files with data collected")
    log.info("=" * 70)
    
    # Concatenate all collected DataFrames
    if all_dfs:
        log.info(f"\nMerging {len(all_dfs)} DataFrames...")
        result = pd.concat(all_dfs, ignore_index=True, sort=False)
        
        # Ensure all canonical columns exist
        for c in CANONICAL_COLS:
            if c not in result.columns:
                result[c] = None
        
        # Drop rows where both Date and Narration are null
        before_drop = len(result)
        result = result.dropna(subset=["Date", "Narration"], how="all").reset_index(drop=True)
        after_drop = len(result)
        
        if before_drop > after_drop:
            log.info(f"Removed {before_drop - after_drop} empty rows")
        
        log.info(f"✓ Final merged DataFrame: {len(result)} rows")
        return result[CANONICAL_COLS]
    else:
        log.warning("⚠ No valid DataFrames found to merge")
        return pd.DataFrame(columns=CANONICAL_COLS)

# ---------- MAIN ----------

def debug_file(filepath):
    """Debug a specific file to see what's going on"""
    path = Path(filepath)
    print("\n" + "="*80)
    print(f"DEBUGGING FILE: {path.name}")
    print("="*80)
    
    # Check file format
    with open(path, 'rb') as f:
        first_bytes = f.read(200)
    print(f"\nFirst 200 bytes: {first_bytes[:200]}")
    
    # Try reading as Excel
    try:
        print("\n--- Attempting Excel read with header=None ---")
        if path.suffix.lower() == '.xls':
            df = pd.read_excel(path, engine='xlrd', header=None, nrows=30)
        else:
            df = pd.read_excel(path, engine='openpyxl', header=None, nrows=30)
        
        print(f"Shape: {df.shape}")
        print(f"\nFirst 30 rows:")
        for idx, row in df.iterrows():
            print(f"Row {idx}: {list(row)[:8]}")
        
        print(f"\nColumn types: {df.dtypes.tolist()}")
        
    except Exception as e:
        print(f"Excel read failed: {e}")
        
        # Try as text
        try:
            print("\n--- Attempting text read ---")
            with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                lines = f.readlines()[:30]
            for i, line in enumerate(lines):
                print(f"Line {i}: {line.rstrip()[:100]}")
        except Exception as e2:
            print(f"Text read failed: {e2}")
    
    print("="*80 + "\n")

if __name__ == "__main__":
    import sys
    
    # Check if debug mode
    if len(sys.argv) > 1 and sys.argv[1] == "--debug":
        if len(sys.argv) < 3:
            print("Usage: python extract_and_standardize.py --debug <filepath>")
            sys.exit(1)
        debug_file(sys.argv[2])
        sys.exit(0)
    
    log.info("\n" + "="*70)
    log.info("FINANCIAL TRANSACTION EXTRACTOR")
    log.info("="*70)
    log.info(f"Project root  : {ROOT_DIR}")
    log.info(f"Data folder   : {DATA_DIR}")
    log.info(f"Output folder : {OUTPUT_DIR}")
    log.info("="*70 + "\n")

    df = process_folder(DATA_DIR)
    
    log.info("\n" + "="*70)
    log.info("RESULTS")
    log.info("="*70)
    log.info(f"Total rows extracted: {len(df)}")

    if len(df) > 0:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        out_path = OUTPUT_DIR / OUTPUT_CSV
        df.to_csv(out_path, index=False)

        log.info(f"✓ CSV written to: {out_path}")
        log.info("\nPreview of first 5 rows:")
        print("\n" + df.head().to_string())
        
        # Show summary statistics
        log.info("\n" + "-"*70)
        log.info("DATA SUMMARY:")
        log.info(f"  Date range: {df['Date'].min()} to {df['Date'].max()}")
        log.info(f"  Total withdrawals: {df['Withdrawal Amt.'].notna().sum()}")
        log.info(f"  Total deposits: {df['Deposit Amt.'].notna().sum()}")
        log.info("="*70)
    else:
        log.error("⚠ No data extracted - please check your input files!")
        log.error("="*70)
