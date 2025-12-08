import csv
import os
from django.conf import settings
from marketdata.models import Symbol, Sectors

CSV_COLUMNS = {
    "id": 0,             # for reporting only
    "symbol": 1,         # SBIN, TCS, etc.
    "sector": 2,         # Abrasives, Agriculture, etc.
    "company_name": 5,   # Company Name
}

def import_symbols_from_csv(csv_name="Analyst_Workday.csv"):
    # Build full path
    csv_path = os.path.join(
        settings.BASE_DIR,
        "marketdata",
        "utils",
        "data",
        csv_name
    )

    print(f"\n📄 Using CSV file: {csv_path}\n")

    added = []
    skipped = []

    with open(csv_path, newline='', encoding="utf-8") as f:
        reader = csv.reader(f)

        for row in reader:
            row_id = row[CSV_COLUMNS["id"]].strip()
            symbol = row[CSV_COLUMNS["symbol"]].strip()
            sector_name = row[CSV_COLUMNS["sector"]].strip()
            company = row[CSV_COLUMNS["company_name"]].strip()

            # Skip if any required field is missing
            if not symbol or not sector_name or not company:
                skipped.append((row_id, symbol))
                print(f"⛔ Skipped row {row_id}: Missing required fields")
                continue

            # Ensure Sector exists
            sector_obj, _ = Sectors.objects.get_or_create(name=sector_name)

            # Create Symbol
            obj, created = Symbol.objects.get_or_create(
                symbol=symbol,
                defaults={
                    "company_name": company,
                    "sector": sector_obj,
                    "market_type": "NSE",
                }
            )

            if created:
                added.append((row_id, symbol))
                print(f"✅ Added: Row {row_id} — {symbol}")
            else:
                print(f"ℹ️ Already exists: {symbol}")

    print("\n==================== IMPORT SUMMARY ====================")
    print(f"✔ Added symbols ({len(added)}): {added}")
    print(f"❌ Skipped symbols ({len(skipped)}): {skipped}")
    print("========================================================\n")

    return added, skipped
