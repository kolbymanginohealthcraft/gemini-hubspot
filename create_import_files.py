#!/usr/bin/env python3
"""
Create separate import files for new records and updates:
1. Split facilities into new vs existing
2. Split companies into new vs existing  
3. Split contacts into new vs existing
4. Output files ready for HubSpot import
"""

import pandas as pd
import os
from datetime import datetime

def log_step(step_name, details=""):
    """Log processing steps"""
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] {step_name}")
    if details:
        print(f"    {details}")

def create_import_files():
    """Create separate files for new records and updates"""
    
    # Create output directories
    os.makedirs("import_files", exist_ok=True)
    os.makedirs("import_files/new_records", exist_ok=True)
    os.makedirs("import_files/updates", exist_ok=True)
    
    log_step("Loading formatted data")
    
    # Load formatted data
    facilities_df = pd.read_csv("formatted_data/formatted_facilities.csv")
    companies_df = pd.read_csv("formatted_data/formatted_companies.csv")
    contacts_df = pd.read_csv("formatted_data/formatted_contacts.csv")
    
    log_step("  Loaded data", f"{len(facilities_df)} facilities, {len(companies_df)} companies, {len(contacts_df)} contacts")
    
    # Split facilities into new vs existing
    log_step("Splitting facilities")
    facilities_new = facilities_df[facilities_df['Record ID'].isna() | (facilities_df['Record ID'] == '')].copy()
    facilities_existing = facilities_df[facilities_df['Record ID'].notna() & (facilities_df['Record ID'] != '')].copy()
    
    # Remove Record ID column from new records (not needed for creation)
    if 'Record ID' in facilities_new.columns:
        facilities_new = facilities_new.drop('Record ID', axis=1)
    
    log_step("  Facilities split", f"New: {len(facilities_new)}, Existing: {len(facilities_existing)}")
    
    # Split companies into new vs existing
    log_step("Splitting companies")
    companies_new = companies_df[companies_df['Record ID'].isna() | (companies_df['Record ID'] == '')].copy()
    companies_existing = companies_df[companies_df['Record ID'].notna() & (companies_df['Record ID'] != '')].copy()
    
    # Remove Record ID column from new records
    if 'Record ID' in companies_new.columns:
        companies_new = companies_new.drop('Record ID', axis=1)
    
    log_step("  Companies split", f"New: {len(companies_new)}, Existing: {len(companies_existing)}")
    
    # Split contacts into new vs existing
    log_step("Splitting contacts")
    contacts_new = contacts_df[contacts_df['Record ID'].isna() | (contacts_df['Record ID'] == '')].copy()
    contacts_existing = contacts_df[contacts_df['Record ID'].notna() & (contacts_df['Record ID'] != '')].copy()
    
    # Remove Record ID column from new records
    if 'Record ID' in contacts_new.columns:
        contacts_new = contacts_new.drop('Record ID', axis=1)
    
    log_step("  Contacts split", f"New: {len(contacts_new)}, Existing: {len(contacts_existing)}")
    
    # Save new record files
    log_step("Saving new record files")
    if not facilities_new.empty:
        facilities_new.to_csv("import_files/new_records/facilities_new.csv", index=False)
        log_step("  Saved", "import_files/new_records/facilities_new.csv")
    
    if not companies_new.empty:
        companies_new.to_csv("import_files/new_records/companies_new.csv", index=False)
        log_step("  Saved", "import_files/new_records/companies_new.csv")
    
    if not contacts_new.empty:
        contacts_new.to_csv("import_files/new_records/contacts_new.csv", index=False)
        log_step("  Saved", "import_files/new_records/contacts_new.csv")
    
    # Save update files
    log_step("Saving update files")
    if not facilities_existing.empty:
        facilities_existing.to_csv("import_files/updates/facilities_updates.csv", index=False)
        log_step("  Saved", "import_files/updates/facilities_updates.csv")
    
    if not companies_existing.empty:
        companies_existing.to_csv("import_files/updates/companies_updates.csv", index=False)
        log_step("  Saved", "import_files/updates/companies_updates.csv")
    
    if not contacts_existing.empty:
        contacts_existing.to_csv("import_files/updates/contacts_updates.csv", index=False)
        log_step("  Saved", "import_files/updates/contacts_updates.csv")
    
    # Generate summary
    print("\n" + "=" * 80)
    print("IMPORT FILES CREATED")
    print("=" * 80)
    
    print(f"\nNEW RECORDS (Create in HubSpot):")
    print(f"  Facilities: {len(facilities_new)} records -> import_files/new_records/facilities_new.csv")
    print(f"  Companies: {len(companies_new)} records -> import_files/new_records/companies_new.csv")
    print(f"  Contacts: {len(contacts_new)} records -> import_files/new_records/contacts_new.csv")
    
    print(f"\nUPDATES (Update existing in HubSpot):")
    print(f"  Facilities: {len(facilities_existing)} records -> import_files/updates/facilities_updates.csv")
    print(f"  Companies: {len(companies_existing)} records -> import_files/updates/companies_updates.csv")
    print(f"  Contacts: {len(contacts_existing)} records -> import_files/updates/contacts_updates.csv")
    
    total_new = len(facilities_new) + len(companies_new) + len(contacts_new)
    total_updates = len(facilities_existing) + len(companies_existing) + len(contacts_existing)
    
    print(f"\nTOTALS:")
    print(f"  New records: {total_new}")
    print(f"  Updates: {total_updates}")
    print(f"  Grand total: {total_new + total_updates}")
    
    print(f"\nIMPORT ORDER RECOMMENDATION:")
    print(f"  1. Import new companies first")
    print(f"  2. Import new facilities second")
    print(f"  3. Import new contacts third")
    print(f"  4. Update existing companies")
    print(f"  5. Update existing facilities")
    print(f"  6. Update existing contacts")
    print(f"  7. Process associations (after all records exist)")
    
    return {
        'facilities_new': len(facilities_new),
        'facilities_existing': len(facilities_existing),
        'companies_new': len(companies_new),
        'companies_existing': len(companies_existing),
        'contacts_new': len(contacts_new),
        'contacts_existing': len(contacts_existing)
    }

def main():
    """Main processing pipeline for creating import files"""
    print("=" * 80)
    print("CREATING HUBSPOT IMPORT FILES")
    print("=" * 80)
    
    try:
        results = create_import_files()
        print(f"\nSUCCESS: Import files created successfully!")
        
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        raise

if __name__ == "__main__":
    main()
