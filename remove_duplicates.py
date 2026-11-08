import os
import csv
import re

def clean_csv(file_path):
    print(f"Cleaning {file_path}...")
    if not os.path.exists(file_path):
        return

    fieldnames = []
    rows = []
    with open(file_path, 'r', newline='', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        for row in reader:
            rows.append(row)

    if not rows:
        return

    # Deduplicate by (Title, Phone, Website)
    unique_rows = []
    seen = set()
    for row in rows:
        key = (
            str(row.get('Title', '')).strip().lower(),
            str(row.get('Phone', '')).strip().lower(),
            str(row.get('Website', '')).strip().lower()
        )
        if key not in seen:
            unique_rows.append(row)
            seen.add(key)

    # Fix IDs - ensuring they are sequential LEAD-XXXX
    # This might break cross-run sequentiality if done partially,
    # but here we are cleaning the whole file.
    for i, row in enumerate(unique_rows):
        row['lead_id'] = f"LEAD-{i+1:04d}"

    with open(file_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(unique_rows)

    print(f"  Removed {len(rows) - len(unique_rows)} duplicates. Final count: {len(unique_rows)}")

def main():
    folder = 'Leads_Generated'
    for filename in os.listdir(folder):
        if filename.endswith('.csv'):
            clean_csv(os.path.join(folder, filename))

if __name__ == "__main__":
    main()
