# save as diagnose_bank_statement.py
import sys
from pathlib import Path
import pandas as pd

def diagnose_file(filepath):
    """Comprehensive diagnostic for bank statement files"""
    path = Path(filepath)
    
    if not path.exists():
        print(f"❌ File not found: {filepath}")
        return
    
    print("="*80)
    print(f"DIAGNOSING: {path.name}")
    print("="*80)
    
    # 1. File size and extension
    file_size = path.stat().st_size
    print(f"\n📊 Basic Info:")
    print(f"  Extension: {path.suffix}")
    print(f"  File size: {file_size:,} bytes ({file_size/1024:.2f} KB)")
    
    # 2. First bytes (detect file type)
    print(f"\n🔍 First 200 bytes (raw):")
    with open(path, 'rb') as f:
        first_bytes = f.read(200)
    print(f"  {first_bytes[:200]}")
    
    # 3. Detect file format
    print(f"\n📝 File Format Detection:")
    is_html = b'<html' in first_bytes.lower() or b'<table' in first_bytes.lower()
    is_text = first_bytes.startswith(b'Account') or first_bytes.startswith(b'Txn Date')
    is_excel_binary = first_bytes.startswith(b'\xd0\xcf\x11\xe0') or first_bytes.startswith(b'PK')
    
    if is_html:
        print("  ✓ Detected: HTML file")
    elif is_text:
        print("  ✓ Detected: Plain text file")
    elif is_excel_binary:
        print("  ✓ Detected: Real Excel binary file")
    else:
        print("  ⚠️ Unknown format")
    
    # 4. Try reading as text
    print(f"\n📄 First 30 lines as text:")
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()[:30]
        for i, line in enumerate(lines, 1):
            print(f"  {i:2d}: {line.rstrip()[:100]}")
    except Exception as e:
        print(f"  ❌ Could not read as text: {e}")
    
    # 5. Try HTML parsing
    if is_html:
        print(f"\n🌐 Trying HTML table parsing:")
        try:
            tables = pd.read_html(filepath)
            print(f"  ✓ Found {len(tables)} table(s)")
            for i, table in enumerate(tables):
                print(f"\n  Table {i+1}: Shape {table.shape}")
                print(f"  Columns: {list(table.columns)}")
                print(f"  First 3 rows:")
                print(table.head(3).to_string(index=False))
        except Exception as e:
            print(f"  ❌ HTML parsing failed: {e}")
    
    # 6. Try tab-delimited
    print(f"\n📋 Trying tab-delimited parsing:")
    try:
        df = pd.read_csv(filepath, sep='\t', encoding='utf-8', nrows=5, header=None)
        print(f"  ✓ Shape: {df.shape}")
        print(f"  First 5 rows:")
        print(df.to_string(index=False))
    except Exception as e:
        print(f"  ❌ Tab-delimited failed: {e}")
    
    # 7. Try as Excel (if has Excel extension)
    if path.suffix.lower() in ['.xls', '.xlsx']:
        print(f"\n📊 Trying Excel parsing:")
        try:
            if path.suffix.lower() == '.xls':
                df = pd.read_excel(filepath, engine='xlrd', nrows=5, header=None)
            else:
                df = pd.read_excel(filepath, engine='openpyxl', nrows=5, header=None)
            print(f"  ✓ Shape: {df.shape}")
            print(f"  First 5 rows:")
            print(df.to_string(index=False))
        except Exception as e:
            print(f"  ❌ Excel parsing failed: {e}")
    
    # 8. Look for transaction patterns
    print(f"\n🔎 Looking for transaction data:")
    try:
        with open(path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        # Look for date patterns
        import re
        date_patterns = [
            r'\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}',
            r'\d{1,2}/\d{1,2}/\d{2,4}',
            r'\d{1,2}-\d{1,2}-\d{2,4}'
        ]
        
        for pattern in date_patterns:
            matches = re.findall(pattern, content[:5000])
            if matches:
                print(f"  ✓ Found date pattern '{pattern}': {len(matches)} matches")
                print(f"    Examples: {matches[:3]}")
        
        # Look for amount patterns
        amount_pattern = r'\b\d{1,3}(?:,\d{3})*(?:\.\d{2})?\b'
        amounts = re.findall(amount_pattern, content[:5000])
        if amounts:
            print(f"  ✓ Found amount pattern: {len(amounts)} matches")
            print(f"    Examples: {amounts[:5]}")
            
    except Exception as e:
        print(f"  ❌ Pattern search failed: {e}")
    
    print("\n" + "="*80)
    print("DIAGNOSIS COMPLETE")
    print("="*80)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python diagnose_bank_statement.py <filepath>")
        print("\nExample:")
        print("  python diagnose_bank_statement.py data/finan_sbi_1-2025--11-2025.xls")
        sys.exit(1)

    diagnose_file(sys.argv[1])