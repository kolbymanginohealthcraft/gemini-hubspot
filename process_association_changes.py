#!/usr/bin/env python3
"""
Process association changes:
1. Analyze current associations in HubSpot data
2. Compare with new associations from Definitive Healthcare
3. Generate files for removing and adding associations
4. Split semicolon-separated associations into individual rows
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

def split_semicolon_associations(df, association_column, record_id_column, association_type):
    """Split semicolon-separated associations into individual rows"""
    split_associations = []
    
    for idx, row in df.iterrows():
        record_id = row[record_id_column]
        associations = str(row[association_column]).strip()
        
        if associations and associations != 'nan' and associations != '':
            # Split by semicolon and clean up
            association_ids = [aid.strip() for aid in associations.split(';') if aid.strip()]
            
            for association_id in association_ids:
                if association_id and association_id != 'nan':
                    split_associations.append({
                        'Record ID': record_id,
                        'Association ID': association_id,
                        'Association Type': association_type
                    })
    
    return pd.DataFrame(split_associations)

def analyze_current_associations():
    """Analyze current associations in HubSpot data"""
    log_step("Analyzing current HubSpot associations")
    
    # Load HubSpot data
    facilities_df = pd.read_csv("gemini/facilities.csv", low_memory=False)
    contacts_df = pd.read_csv("gemini/contacts.csv", low_memory=False)
    
    log_step("  Loaded HubSpot data", f"{len(facilities_df)} facilities, {len(contacts_df)} contacts")
    
    # Extract facility-company associations
    log_step("  Extracting facility-company associations")
    facility_company_current = split_semicolon_associations(
        facilities_df, 'Associated Company IDs', 'Record ID', 'Facility-Company'
    )
    
    # Extract contact-facility associations
    log_step("  Extracting contact-facility associations")
    contact_facility_current = split_semicolon_associations(
        contacts_df, 'Associated Facility IDs', 'Record ID', 'Contact-Facility'
    )
    
    # Extract contact-company associations
    log_step("  Extracting contact-company associations")
    contact_company_current = split_semicolon_associations(
        contacts_df, 'Associated Company IDs', 'Record ID', 'Contact-Company'
    )
    
    log_step("  Current associations", f"Facility-Company: {len(facility_company_current)}, Contact-Facility: {len(contact_facility_current)}, Contact-Company: {len(contact_company_current)}")
    
    return facility_company_current, contact_facility_current, contact_company_current

def load_new_associations():
    """Load new associations from Definitive Healthcare"""
    log_step("Loading new associations from Definitive Healthcare")
    
    # Load association files
    facility_company_new = pd.read_csv("associations/facility_company_associations.csv")
    contact_facility_new = pd.read_csv("associations/contact_facility_associations.csv")
    contact_company_new = pd.read_csv("associations/contact_company_associations.csv")
    
    # Filter to only include associations with Record IDs (existing records)
    facility_company_new = facility_company_new[
        (facility_company_new['Facility Record ID'] != '') & 
        (facility_company_new['Company Record ID'] != '') &
        (facility_company_new['Facility Record ID'].notna()) &
        (facility_company_new['Company Record ID'].notna())
    ].copy()
    
    contact_facility_new = contact_facility_new[
        (contact_facility_new['Contact Record ID'] != '') & 
        (contact_facility_new['Facility Record ID'] != '') &
        (contact_facility_new['Contact Record ID'].notna()) &
        (contact_facility_new['Facility Record ID'].notna())
    ].copy()
    
    contact_company_new = contact_company_new[
        (contact_company_new['Contact Record ID'] != '') & 
        (contact_company_new['Company Record ID'] != '') &
        (contact_company_new['Contact Record ID'].notna()) &
        (contact_company_new['Company Record ID'].notna())
    ].copy()
    
    # Format for comparison (convert to integers)
    facility_company_new_formatted = pd.DataFrame({
        'Record ID': facility_company_new['Facility Record ID'].astype(int),
        'Association ID': facility_company_new['Company Record ID'].astype(int),
        'Association Type': 'Facility-Company'
    })
    
    contact_facility_new_formatted = pd.DataFrame({
        'Record ID': contact_facility_new['Contact Record ID'].astype(int),
        'Association ID': contact_facility_new['Facility Record ID'].astype(int),
        'Association Type': 'Contact-Facility'
    })
    
    contact_company_new_formatted = pd.DataFrame({
        'Record ID': contact_company_new['Contact Record ID'].astype(int),
        'Association ID': contact_company_new['Company Record ID'].astype(int),
        'Association Type': 'Contact-Company'
    })
    
    log_step("  New associations", f"Facility-Company: {len(facility_company_new_formatted)}, Contact-Facility: {len(contact_facility_new_formatted)}, Contact-Company: {len(contact_company_new_formatted)}")
    
    return facility_company_new_formatted, contact_facility_new_formatted, contact_company_new_formatted

def compare_associations(current_df, new_df, association_type):
    """Compare current and new associations to find changes"""
    log_step(f"Comparing {association_type} associations")
    
    # Create comparison keys
    current_df['Key'] = current_df['Record ID'].astype(str) + '|' + current_df['Association ID'].astype(str)
    new_df['Key'] = new_df['Record ID'].astype(str) + '|' + new_df['Association ID'].astype(str)
    
    # Find associations to remove (in current but not in new)
    current_keys = set(current_df['Key'])
    new_keys = set(new_df['Key'])
    
    to_remove_keys = current_keys - new_keys
    to_add_keys = new_keys - current_keys
    
    # Create dataframes for remove and add
    to_remove = current_df[current_df['Key'].isin(to_remove_keys)].drop('Key', axis=1)
    to_add = new_df[new_df['Key'].isin(to_add_keys)].drop('Key', axis=1)
    
    log_step(f"  {association_type} changes", f"To remove: {len(to_remove)}, To add: {len(to_add)}")
    
    return to_remove, to_add

def main():
    """Main processing pipeline for association changes"""
    print("=" * 80)
    print("ASSOCIATION CHANGE ANALYSIS")
    print("=" * 80)
    
    # Create output directories
    os.makedirs("association_changes", exist_ok=True)
    os.makedirs("association_changes/remove", exist_ok=True)
    os.makedirs("association_changes/add", exist_ok=True)
    
    try:
        # Analyze current associations
        facility_company_current, contact_facility_current, contact_company_current = analyze_current_associations()
        
        # Load new associations
        facility_company_new, contact_facility_new, contact_company_new = load_new_associations()
        
        # Compare associations
        facility_company_remove, facility_company_add = compare_associations(
            facility_company_current, facility_company_new, "Facility-Company"
        )
        
        contact_facility_remove, contact_facility_add = compare_associations(
            contact_facility_current, contact_facility_new, "Contact-Facility"
        )
        
        contact_company_remove, contact_company_add = compare_associations(
            contact_company_current, contact_company_new, "Contact-Company"
        )
        
        # Save association change files
        log_step("Saving association change files")
        
        # Remove files
        if not facility_company_remove.empty:
            facility_company_remove.to_csv("association_changes/remove/facility_company_remove.csv", index=False)
            log_step("  Saved", "association_changes/remove/facility_company_remove.csv")
        
        if not contact_facility_remove.empty:
            contact_facility_remove.to_csv("association_changes/remove/contact_facility_remove.csv", index=False)
            log_step("  Saved", "association_changes/remove/contact_facility_remove.csv")
        
        if not contact_company_remove.empty:
            contact_company_remove.to_csv("association_changes/remove/contact_company_remove.csv", index=False)
            log_step("  Saved", "association_changes/remove/contact_company_remove.csv")
        
        # Add files
        if not facility_company_add.empty:
            facility_company_add.to_csv("association_changes/add/facility_company_add.csv", index=False)
            log_step("  Saved", "association_changes/add/facility_company_add.csv")
        
        if not contact_facility_add.empty:
            contact_facility_add.to_csv("association_changes/add/contact_facility_add.csv", index=False)
            log_step("  Saved", "association_changes/add/contact_facility_add.csv")
        
        if not contact_company_add.empty:
            contact_company_add.to_csv("association_changes/add/contact_company_add.csv", index=False)
            log_step("  Saved", "association_changes/add/contact_company_add.csv")
        
        # Generate summary
        print("\n" + "=" * 80)
        print("ASSOCIATION CHANGE SUMMARY")
        print("=" * 80)
        
        print(f"\nASSOCIATIONS TO REMOVE:")
        print(f"  Facility-Company: {len(facility_company_remove)}")
        print(f"  Contact-Facility: {len(contact_facility_remove)}")
        print(f"  Contact-Company: {len(contact_company_remove)}")
        print(f"  Total to remove: {len(facility_company_remove) + len(contact_facility_remove) + len(contact_company_remove)}")
        
        print(f"\nASSOCIATIONS TO ADD:")
        print(f"  Facility-Company: {len(facility_company_add)}")
        print(f"  Contact-Facility: {len(contact_facility_add)}")
        print(f"  Contact-Company: {len(contact_company_add)}")
        print(f"  Total to add: {len(facility_company_add) + len(contact_facility_remove) + len(contact_company_add)}")
        
        print(f"\nRECOMMENDED WORKFLOW:")
        print(f"  1. Import new records (companies, facilities, contacts)")
        print(f"  2. Update existing records")
        print(f"  3. Remove old associations using remove files")
        print(f"  4. Add new associations using add files")
        
        print(f"\nFiles ready for HubSpot association management!")
        
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        raise

if __name__ == "__main__":
    main()
