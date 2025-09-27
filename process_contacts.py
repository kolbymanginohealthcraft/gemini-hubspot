#!/usr/bin/env python3
"""
Complete contact processing pipeline:
1. Create formatted contacts from executives data (filtered by FIRM_TYPE)
2. Match Record IDs using DHC ID and Email lookups
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

def create_formatted_contacts():
    """Create formatted contacts file with unique GLOBAL_PERSON_ID values, filtered by FIRM_TYPE"""
    
    log_step("Loading executives data")
    executives_df = pd.read_csv('definitive/Long_Term_Care_Executives.csv', low_memory=False)
    log_step("  Loaded executives", f"{len(executives_df)} records")
    
    # Define allowed firm types
    allowed_firm_types = [
        'Assisted Living Facility',
        'Assisted Living Facility Corporation', 
        'Skilled Nursing Facility',
        'Skilled Nursing Facility Corporation'
    ]
    
    # Filter by firm type
    log_step("Filtering by FIRM_TYPE")
    print(f"    Available firm types: {executives_df['FIRM_TYPE'].value_counts().to_dict()}")
    
    filtered_df = executives_df[executives_df['FIRM_TYPE'].isin(allowed_firm_types)]
    log_step("  Filtered records", f"{len(filtered_df)} records after filtering")
    
    # Check for unique GLOBAL_PERSON_ID values in filtered data
    unique_count = filtered_df['GLOBAL_PERSON_ID'].nunique()
    total_count = len(filtered_df['GLOBAL_PERSON_ID'])
    log_step("  Unique analysis", f"{unique_count} unique, {total_count} total, {total_count - unique_count} duplicates")
    
    # Create formatted contacts with unique GLOBAL_PERSON_ID
    log_step("Creating formatted contacts")
    contacts_formatted = filtered_df.groupby('GLOBAL_PERSON_ID').first().reset_index()
    
    # Map to HubSpot format
    formatted_contacts = pd.DataFrame({
        'First Name': contacts_formatted['FIRST_NAME'],
        'Last Name': contacts_formatted['LAST_NAME'], 
        'DHC ID': contacts_formatted['GLOBAL_PERSON_ID'],
        'Job Title': contacts_formatted['TITLE'],
        'Email': contacts_formatted['EMAIL']
    })
    
    # Create output directory if it doesn't exist
    os.makedirs('formatted_data', exist_ok=True)
    
    # Save the formatted contacts
    formatted_contacts.to_csv('formatted_data/formatted_contacts.csv', index=False)
    log_step("  Saved contacts", f"{len(formatted_contacts)} unique contacts")
    
    # Show data quality stats
    log_step("  Data quality", f"First Name: {formatted_contacts['First Name'].notna().sum()}, Last Name: {formatted_contacts['Last Name'].notna().sum()}, Email: {formatted_contacts['Email'].notna().sum()}, Job Title: {formatted_contacts['Job Title'].notna().sum()}")
    
    return formatted_contacts

def match_contact_record_ids():
    """Match Record IDs for contacts using DHC ID and Email"""
    
    log_step("Loading contact data")
    
    # Load formatted contacts
    formatted_contacts = pd.read_csv("formatted_data/formatted_contacts.csv", low_memory=False)
    log_step("  Loaded formatted contacts", f"{len(formatted_contacts)} records")
    
    # Load HubSpot contacts
    hubspot_contacts = pd.read_csv("gemini/contacts.csv", low_memory=False)
    log_step("  Loaded HubSpot contacts", f"{len(hubspot_contacts)} records")
    
    # Initialize Record ID column
    formatted_contacts['Record ID'] = ''
    
    # Step 1: Match by DHC ID
    log_step("Matching by DHC ID")
    dhc_to_record_id = {}
    for idx, row in hubspot_contacts.iterrows():
        dhc_id = str(row['DHC ID']).strip()
        record_id = str(row['Record ID']).strip()
        if dhc_id and dhc_id != 'nan' and record_id and record_id != 'nan':
            try:
                dhc_to_record_id[int(float(dhc_id))] = record_id
            except (ValueError, TypeError):
                continue
    
    log_step("  Built DHC ID lookup", f"{len(dhc_to_record_id)} mappings")
    
    dhc_matches = 0
    for idx, row in formatted_contacts.iterrows():
        dhc_id = row['DHC ID']
        if pd.notna(dhc_id) and dhc_id in dhc_to_record_id:
            formatted_contacts.at[idx, 'Record ID'] = dhc_to_record_id[dhc_id]
            dhc_matches += 1
    
    log_step("  DHC ID matches", f"{dhc_matches} contacts matched")
    
    # Step 2: Match by Email for records without DHC ID match
    log_step("Matching by Email for unmatched records")
    email_to_record_id = {}
    for idx, row in hubspot_contacts.iterrows():
        email = str(row['Email']).strip().lower()
        record_id = str(row['Record ID']).strip()
        if email and email != 'nan' and email != 'none' and record_id and record_id != 'nan':
            email_to_record_id[email] = record_id
    
    log_step("  Built Email lookup", f"{len(email_to_record_id)} mappings")
    
    email_matches = 0
    for idx, row in formatted_contacts.iterrows():
        # Only try email match if no DHC ID match was found
        if not formatted_contacts.at[idx, 'Record ID']:
            email = str(row['Email']).strip().lower()
            if email and email != 'nan' and email != 'none' and email in email_to_record_id:
                formatted_contacts.at[idx, 'Record ID'] = email_to_record_id[email]
                email_matches += 1
    
    log_step("  Email matches", f"{email_matches} contacts matched")
    
    # Calculate final statistics
    total_matches = dhc_matches + email_matches
    new_records = len(formatted_contacts) - total_matches
    
    log_step("  Final results", f"Total matches: {total_matches}, New records: {new_records}")
    
    # Save updated contacts
    formatted_contacts.to_csv("formatted_data/formatted_contacts.csv", index=False)
    log_step("  Saved updated contacts", "formatted_data/formatted_contacts.csv")
    
    return formatted_contacts, total_matches, new_records

def main():
    """Main processing pipeline for contacts"""
    print("=" * 60)
    print("CONTACT PROCESSING PIPELINE")
    print("=" * 60)
    
    try:
        # Step 1: Create formatted contacts
        formatted_contacts = create_formatted_contacts()
        
        # Step 2: Match Record IDs
        updated_contacts, total_matches, new_records = match_contact_record_ids()
        
        # Final summary
        print("\n" + "=" * 60)
        print("CONTACT PROCESSING SUMMARY")
        print("=" * 60)
        print(f"Total contacts processed: {len(updated_contacts)}")
        print(f"Existing contacts (with Record ID): {total_matches}")
        print(f"New contacts (without Record ID): {new_records}")
        print(f"Match rate: {(total_matches/len(updated_contacts)*100):.1f}%")
        print("\nFiles ready for HubSpot import!")
        
        # Show sample of matched vs new records
        matched_records = updated_contacts[updated_contacts['Record ID'] != '']
        new_records_df = updated_contacts[updated_contacts['Record ID'] == '']
        
        if len(matched_records) > 0:
            print(f"\nSample of matched records:")
            print(matched_records[['First Name', 'Last Name', 'Email', 'Record ID']].head())
        
        if len(new_records_df) > 0:
            print(f"\nSample of new records:")
            print(new_records_df[['First Name', 'Last Name', 'Email', 'DHC ID']].head())
        
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        raise

if __name__ == "__main__":
    main()
