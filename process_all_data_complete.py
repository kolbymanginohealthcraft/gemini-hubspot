#!/usr/bin/env python3
"""
Complete data processing pipeline - combines all functionality:
1. Process facilities and companies (orgs) from MasterORG.csv
2. Process contacts from executives data (filtered by FIRM_TYPE)
3. Match Record IDs for all data types
4. Create separate import files for new vs existing records
5. Process association changes (remove/add)
6. Generate comprehensive summary
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

def format_phone_number(phone):
    """Format phone number as (123) 456-7890"""
    if pd.isna(phone) or phone == '' or str(phone).strip() == '':
        return ''
    
    # Remove all non-digit characters
    digits = ''.join(filter(str.isdigit, str(phone)))
    
    # Format as (123) 456-7890
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    elif len(digits) == 11 and digits[0] == '1':
        return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    else:
        return str(phone).strip()

def format_zip_code(zip_code):
    """Format zip code as 5-digit string"""
    if pd.isna(zip_code) or zip_code == '':
        return ''
    
    # Convert to string and remove non-digits
    zip_str = ''.join(filter(str.isdigit, str(zip_code)))
    
    # Take first 5 digits and pad with zeros if needed
    if len(zip_str) >= 5:
        return zip_str[:5]
    else:
        return zip_str.zfill(5)

def format_npi(npi):
    """Format NPI as integer (remove decimals)"""
    if pd.isna(npi) or npi == '':
        return ''
    
    try:
        # Convert to float first to handle decimal strings, then to int
        npi_int = int(float(str(npi)))
        return str(npi_int)
    except (ValueError, TypeError):
        return ''

def get_valid_us_states():
    """Get list of valid US states (50 states + DC)"""
    return {
        'AL', 'AK', 'AZ', 'AR', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA',
        'HI', 'ID', 'IL', 'IN', 'IA', 'KS', 'KY', 'LA', 'ME', 'MD',
        'MA', 'MI', 'MN', 'MS', 'MO', 'MT', 'NE', 'NV', 'NH', 'NJ',
        'NM', 'NY', 'NC', 'ND', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC',
        'SD', 'TN', 'TX', 'UT', 'VT', 'VA', 'WA', 'WV', 'WI', 'WY',
        'DC'
    }

def create_full_address(street, city, state, zip_code):
    """Create full address string"""
    def clean_field(field):
        if pd.isna(field) or field == '':
            return ''
        return str(field).strip()
    
    parts = []
    street_clean = clean_field(street)
    city_clean = clean_field(city)
    state_clean = clean_field(state)
    zip_clean = clean_field(zip_code)
    
    if street_clean:
        parts.append(street_clean)
    if city_clean:
        parts.append(city_clean)
    if state_clean:
        parts.append(state_clean)
    if zip_clean:
        parts.append(zip_clean)
    
    return ', '.join(parts)

def load_masterorg_data():
    """Load and inspect MasterORG.csv structure"""
    log_step("Loading MasterORG data")
    
    # Load master data with optimized settings
    master_df = pd.read_csv("definitive/MasterORG.csv", low_memory=False, engine='c')
    log_step("  Loaded MasterORG", f"{len(master_df)} records")
    
    # Show column structure
    log_step("  Column structure", f"{len(master_df.columns)} columns")
    print("    Available columns:")
    for i, col in enumerate(master_df.columns):
        print(f"      {i+1:2d}. {col}")
    
    return master_df

def process_facilities_from_masterorg(master_df):
    """Extract facilities from MasterORG.csv"""
    log_step("Processing facilities from MasterORG")
    
    # Filter for SNF and ALF facilities using Facility subtype
    facility_types = ['Skilled Nursing Facility', 'Assisted Living Facility']
    facilities = master_df[
        master_df['Facility subtype'].isin(facility_types) &
        (master_df['Facility status'] == 'Active')
    ].copy()
    
    # Apply SNF-specific filter: SNFs must have a Provider number
    snf_facilities = facilities[facilities['Facility subtype'] == 'Skilled Nursing Facility']
    alf_facilities = facilities[facilities['Facility subtype'] == 'Assisted Living Facility']
    
    # Filter SNFs to only include those with Provider number
    snf_facilities = snf_facilities[
        (snf_facilities['Provider number'].notna()) &
        (snf_facilities['Provider number'] != '')
    ]
    
    # Combine filtered SNFs with all ALFs
    facilities = pd.concat([snf_facilities, alf_facilities], ignore_index=True)
    
    # Filter to only include US states (50 states + DC)
    valid_states = get_valid_us_states()
    facilities = facilities[facilities['State'].isin(valid_states)]
    
    log_step("  Filtered facilities", f"{len(facilities)} SNF/ALF records")
    
    # Map fields to HubSpot format
    facilities_formatted = pd.DataFrame({
        'Name of Facility': facilities['Facility name'],
        'CCN': facilities['Provider number'].fillna(''),
        'Facility Type': facilities['Facility subtype'].map({
            'Skilled Nursing Facility': 'SNF',
            'Assisted Living Facility': 'ALF'
        }),
        'Street': facilities['AddressLine 1'],
        'City': facilities['City'],
        'State': facilities['State'],
        'Zip Code': facilities['Zip code'].apply(format_zip_code),
        'Phone Number': facilities['Organization phone'].apply(format_phone_number),
        'NPI': facilities['Facility primary NPI'].apply(format_npi),
        'Facility website': facilities['Facility website'].fillna(''),
        'Total Beds': facilities['Number of staffed beds'].fillna(''),
        'DHC ID': facilities['Facility definitive ID']
    })
    
    # Create full address columns
    facilities_formatted["Facility's Address"] = facilities_formatted.apply(
        lambda row: create_full_address(row['Street'], row['City'], row['State'], row['Zip Code']), 
        axis=1
    )
    
    # Add pipeline columns
    facilities_formatted['Facility pipeline'] = 'Gemini Onboarding'
    facilities_formatted['Facility pipeline stage'] = 'Lead'
    
    # Add Unique Facility Address
    facilities_formatted['Unique Facility Address'] = facilities_formatted["Facility's Address"]
    
    log_step("  Formatted facilities", f"{len(facilities_formatted)} records")
    
    return facilities_formatted

def process_companies_from_masterorg(master_df):
    """Extract companies from MasterORG.csv"""
    log_step("Processing companies from MasterORG")
    
    # First, get all Network IDs from SNF/ALF facilities
    facility_types = ['Skilled Nursing Facility', 'Assisted Living Facility']
    facilities = master_df[
        master_df['Facility subtype'].isin(facility_types) &
        (master_df['Facility status'] == 'Active')
    ]
    
    # Get unique Network IDs (excluding NaN)
    network_ids = set(facilities['Network ID'].dropna().astype(int))
    log_step("  Found network IDs", f"{len(network_ids)} unique IDs")
    
    # Find companies (records whose Facility definitive ID is a Network ID for facilities)
    companies = master_df[
        master_df['Facility definitive ID'].astype(int).isin(network_ids)
    ].copy()
    
    # Filter to only include US states (50 states + DC)
    valid_states = get_valid_us_states()
    companies = companies[companies['State'].isin(valid_states)]
    
    log_step("  Filtered companies", f"{len(companies)} records")
    
    # Map fields to HubSpot format
    companies_formatted = pd.DataFrame({
        'Company name': companies['Facility name'],
        'DHC ID': companies['Facility definitive ID'],
        'Street Address': companies['AddressLine 1'].fillna('') + ' ' + companies['AddressLine 2'].fillna(''),
        'City': companies['City'],
        'State/Region': companies['State'],
        'Postal Code': companies['Zip code'].apply(format_zip_code),
        'Phone Number': companies['Organization phone'].apply(format_phone_number),
        'Website URL': companies['Facility website'].fillna(''),
        'Country/Region': 'United States'
    })
    
    # Create HQ Unique Address
    companies_formatted['HQ Unique Address'] = companies_formatted.apply(
        lambda row: create_full_address(row['Street Address'], row['City'], row['State/Region'], row['Postal Code']), 
        axis=1
    )
    
    # Clean up Street Address (remove extra spaces)
    companies_formatted['Street Address'] = companies_formatted['Street Address'].str.strip()
    
    log_step("  Formatted companies", f"{len(companies_formatted)} records")
    
    return companies_formatted

def create_formatted_contacts():
    """Create formatted contacts file with unique GLOBAL_PERSON_ID values, filtered by FIRM_TYPE"""
    
    log_step("Loading executives data")
    executives_df = pd.read_csv('definitive/Long_Term_Care_Executives.csv', low_memory=False, engine='c')
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
    
    log_step("  Formatted contacts", f"{len(formatted_contacts)} unique contacts")
    
    # Show data quality stats
    log_step("  Data quality", f"First Name: {formatted_contacts['First Name'].notna().sum()}, Last Name: {formatted_contacts['Last Name'].notna().sum()}, Email: {formatted_contacts['Email'].notna().sum()}, Job Title: {formatted_contacts['Job Title'].notna().sum()}")
    
    return formatted_contacts

def match_record_ids(facilities_df, companies_df, contacts_df):
    """Match Record IDs from HubSpot data for all data types"""
    log_step("Matching Record IDs")
    
    # Load HubSpot data with optimized settings
    hubspot_facilities = pd.read_csv("gemini/facilities.csv", low_memory=False, engine='c')
    hubspot_companies = pd.read_csv("gemini/companies.csv", low_memory=False, engine='c')
    hubspot_contacts = pd.read_csv("gemini/contacts.csv", low_memory=False, engine='c')
    
    log_step("  Loaded HubSpot data", f"{len(hubspot_facilities)} facilities, {len(hubspot_companies)} companies, {len(hubspot_contacts)} contacts")
    
    # Initialize Record ID columns
    facilities_df['Record ID'] = ''
    companies_df['Record ID'] = ''
    contacts_df['Record ID'] = ''
    
    # Match facility Record IDs by DHC ID
    dhc_to_record_id = {}
    for idx, row in hubspot_facilities.iterrows():
        dhc_id = str(row['DHC ID']).strip()
        record_id = str(row['Record ID']).strip()
        if dhc_id and dhc_id != 'nan' and record_id and record_id != 'nan':
            try:
                dhc_to_record_id[int(float(dhc_id))] = record_id
            except (ValueError, TypeError):
                continue
    
    facility_matches = 0
    for idx, row in facilities_df.iterrows():
        dhc_id = row['DHC ID']
        if pd.notna(dhc_id) and dhc_id in dhc_to_record_id:
            facilities_df.at[idx, 'Record ID'] = dhc_to_record_id[dhc_id]
            facility_matches += 1
    
    # Match company Record IDs by DHC ID
    dhc_to_record_id = {}
    for idx, row in hubspot_companies.iterrows():
        dhc_id = str(row['DHC ID']).strip()
        record_id = str(row['Record ID']).strip()
        if dhc_id and dhc_id != 'nan' and record_id and record_id != 'nan':
            try:
                dhc_to_record_id[int(float(dhc_id))] = record_id
            except (ValueError, TypeError):
                continue
    
    company_matches = 0
    for idx, row in companies_df.iterrows():
        dhc_id = int(row['DHC ID'])
        if dhc_id in dhc_to_record_id:
            companies_df.at[idx, 'Record ID'] = dhc_to_record_id[dhc_id]
            company_matches += 1
    
    # Match contact Record IDs using vectorized operations
    log_step("  Matching contacts by DHC ID")
    
    # Prepare HubSpot data for DHC ID matching
    hubspot_contacts_dhc = hubspot_contacts[
        (hubspot_contacts['DHC ID'].notna()) & 
        (hubspot_contacts['DHC ID'] != '') & 
        (hubspot_contacts['Record ID'].notna()) & 
        (hubspot_contacts['Record ID'] != '')
    ].copy()
    
    # Convert DHC IDs to numeric for matching
    hubspot_contacts_dhc['DHC ID'] = pd.to_numeric(hubspot_contacts_dhc['DHC ID'], errors='coerce')
    contacts_df['DHC ID'] = pd.to_numeric(contacts_df['DHC ID'], errors='coerce')
    
    # Merge on DHC ID
    contacts_with_dhc = contacts_df.merge(
        hubspot_contacts_dhc[['DHC ID', 'Record ID']], 
        on='DHC ID', 
        how='left', 
        suffixes=('', '_hubspot')
    )
    
    # Update Record ID where DHC ID matched
    dhc_mask = contacts_with_dhc['Record ID_hubspot'].notna()
    contacts_df.loc[dhc_mask, 'Record ID'] = contacts_with_dhc.loc[dhc_mask, 'Record ID_hubspot']
    contact_dhc_matches = dhc_mask.sum()
    
    # Match remaining contacts by Email using vectorized operations
    log_step("  Matching contacts by Email")
    
    # Prepare email data for matching
    hubspot_contacts_email = hubspot_contacts[
        (hubspot_contacts['Email'].notna()) & 
        (hubspot_contacts['Email'] != '') & 
        (hubspot_contacts['Record ID'].notna()) & 
        (hubspot_contacts['Record ID'] != '')
    ].copy()
    
    # Normalize emails
    hubspot_contacts_email['Email_normalized'] = hubspot_contacts_email['Email'].str.strip().str.lower()
    contacts_df['Email_normalized'] = contacts_df['Email'].str.strip().str.lower()
    
    # Find contacts without Record ID
    unmatched_mask = (contacts_df['Record ID'].isna()) | (contacts_df['Record ID'] == '')
    
    # Merge on email for unmatched contacts
    if unmatched_mask.any():
        contacts_unmatched = contacts_df[unmatched_mask].copy()
        email_merge = contacts_unmatched.merge(
            hubspot_contacts_email[['Email_normalized', 'Record ID']], 
            on='Email_normalized', 
            how='left', 
            suffixes=('', '_hubspot')
        )
        
        # Update Record ID where email matched
        email_mask = email_merge['Record ID_hubspot'].notna()
        if email_mask.any():
            contacts_df.loc[contacts_df.index[unmatched_mask][email_mask], 'Record ID'] = email_merge.loc[email_mask, 'Record ID_hubspot']
    
    contact_email_matches = ((contacts_df['Record ID'].notna()) & (contacts_df['Record ID'] != '')).sum() - contact_dhc_matches
    total_contact_matches = contact_dhc_matches + contact_email_matches
    
    log_step("  Matched Record IDs", f"{facility_matches} facilities, {company_matches} companies, {total_contact_matches} contacts")
    
    return facilities_df, companies_df, contacts_df, facility_matches, company_matches, total_contact_matches

def detect_changes_in_existing_records(existing_df, hubspot_df, object_type):
    """Detect which existing records actually need updates by comparing with HubSpot data"""
    if existing_df.empty:
        return existing_df
    
    log_step(f"  Detecting changes for {object_type}")
    
    # Define key fields for comparison based on object type
    if object_type == 'facilities':
        key_fields = ['Name of Facility', 'Street', 'City', 'State', 'Zip Code', 'Phone Number', 'NPI', 'Facility website', 'Total Beds', 'CCN']
    elif object_type == 'companies':
        key_fields = ['Company name', 'Street Address', 'City', 'State/Region', 'Postal Code', 'Phone Number', 'Website URL']
    elif object_type == 'contacts':
        key_fields = ['First Name', 'Last Name', 'Job Title', 'Email']
    else:
        return existing_df
    
    # Filter to only include fields that exist in both dataframes
    available_fields = [field for field in key_fields if field in existing_df.columns and field in hubspot_df.columns]
    
    if not available_fields:
        log_step(f"    {object_type} changes detected", f"0 out of {len(existing_df)} records need updates (no matching fields)")
        return existing_df.iloc[0:0].copy()
    
    # Ensure Record ID columns have the same data type for merging
    # Convert to numeric first to handle decimal formatting, then to string
    existing_df['Record ID'] = pd.to_numeric(existing_df['Record ID'], errors='coerce').astype('Int64').astype(str)
    hubspot_df['Record ID'] = pd.to_numeric(hubspot_df['Record ID'], errors='coerce').astype('Int64').astype(str)
    
    merged_df = existing_df.merge(
        hubspot_df[['Record ID'] + available_fields], 
        on='Record ID', 
        how='left', 
        suffixes=('_new', '_hubspot')
    )
    
    # Create comparison mask for each field
    change_mask = pd.Series(False, index=merged_df.index)
    
    for field in available_fields:
        new_col = f"{field}_new"
        hubspot_col = f"{field}_hubspot"
        
        # Normalize values for comparison
        new_vals = merged_df[new_col].fillna('').astype(str).str.strip()
        hubspot_vals = merged_df[hubspot_col].fillna('').astype(str).str.strip()
        
        # Handle special cases
        new_vals = new_vals.replace(['nan', 'None'], '')
        hubspot_vals = hubspot_vals.replace(['nan', 'None'], '')
        
        # Normalize numeric fields (remove trailing .0 from decimals)
        if field in ['Zip Code', 'Total Beds', 'NPI', 'CCN']:
            new_vals = new_vals.str.replace(r'\.0$', '', regex=True)
            hubspot_vals = hubspot_vals.str.replace(r'\.0$', '', regex=True)
        
        # Check for differences
        field_changes = new_vals != hubspot_vals
        change_mask |= field_changes
    
    # Filter to only records with changes
    if change_mask.any():
        # Get the records that need changes from the merged dataframe
        changed_records = merged_df[change_mask].copy()
        
        # Debug: Check if 60 West is incorrectly in changed_records
        if object_type == 'facilities':
            sixty_west_in_changed = changed_records[changed_records['CCN_new'] == '075442']
            if len(sixty_west_in_changed) > 0:
                log_step(f"    WARNING: 60 West incorrectly flagged for changes", "This indicates a bug in change detection")
        
        # Extract only the original columns (those with _new suffix or without suffix)
        original_columns = []
        for col in existing_df.columns:
            if col in changed_records.columns:
                original_columns.append(col)
            elif f"{col}_new" in changed_records.columns:
                original_columns.append(f"{col}_new")
        
        # Create result dataframe with original structure
        result_df = changed_records[original_columns].copy()
        
        # Rename columns back to original names (remove _new suffix)
        column_mapping = {col: col.replace('_new', '') for col in result_df.columns if col.endswith('_new')}
        result_df = result_df.rename(columns=column_mapping)
        
        log_step(f"    {object_type} changes detected", f"{len(result_df)} out of {len(existing_df)} records need updates")
        return result_df
    else:
        log_step(f"    {object_type} changes detected", f"0 out of {len(existing_df)} records need updates")
        return existing_df.iloc[0:0].copy()

def create_import_files(facilities_df, companies_df, contacts_df):
    """Create separate import files for new records and updates"""
    log_step("Creating import files")
    
    # Load HubSpot data for comparison
    hubspot_facilities = pd.read_csv("gemini/facilities.csv", low_memory=False)
    hubspot_companies = pd.read_csv("gemini/companies.csv", low_memory=False)
    hubspot_contacts = pd.read_csv("gemini/contacts.csv", low_memory=False)
    
    # Split facilities into new vs existing
    log_step("  Splitting facilities")
    facilities_new = facilities_df[facilities_df['Record ID'].isna() | (facilities_df['Record ID'] == '')].copy()
    facilities_existing = facilities_df[facilities_df['Record ID'].notna() & (facilities_df['Record ID'] != '')].copy()
    
    # Remove Record ID column from new records
    if 'Record ID' in facilities_new.columns:
        facilities_new = facilities_new.drop('Record ID', axis=1)
    
    # Detect which existing facilities actually need updates
    facilities_existing = detect_changes_in_existing_records(facilities_existing, hubspot_facilities, 'facilities')
    
    log_step("    Facilities split", f"New: {len(facilities_new)}, Existing: {len(facilities_existing)}")
    
    # Split companies into new vs existing
    log_step("  Splitting companies")
    companies_new = companies_df[companies_df['Record ID'].isna() | (companies_df['Record ID'] == '')].copy()
    companies_existing = companies_df[companies_df['Record ID'].notna() & (companies_df['Record ID'] != '')].copy()
    
    # Remove Record ID column from new records
    if 'Record ID' in companies_new.columns:
        companies_new = companies_new.drop('Record ID', axis=1)
    
    # Detect which existing companies actually need updates
    companies_existing = detect_changes_in_existing_records(companies_existing, hubspot_companies, 'companies')
    
    log_step("    Companies split", f"New: {len(companies_new)}, Existing: {len(companies_existing)}")
    
    # Split contacts into new vs existing
    log_step("  Splitting contacts")
    contacts_new = contacts_df[contacts_df['Record ID'].isna() | (contacts_df['Record ID'] == '')].copy()
    contacts_existing = contacts_df[contacts_df['Record ID'].notna() & (contacts_df['Record ID'] != '')].copy()
    
    # Remove Record ID column from new records
    if 'Record ID' in contacts_new.columns:
        contacts_new = contacts_new.drop('Record ID', axis=1)
    
    # Detect which existing contacts actually need updates
    contacts_existing = detect_changes_in_existing_records(contacts_existing, hubspot_contacts, 'contacts')
    
    log_step("    Contacts split", f"New: {len(contacts_new)}, Existing: {len(contacts_existing)}")
    
    # Save new record files (Step 1)
    log_step("  Saving Step 1: New record files")
    if not facilities_new.empty:
        # Ensure Record ID is integer (for new records that have Record IDs)
        if 'Record ID' in facilities_new.columns:
            facilities_new['Record ID'] = pd.to_numeric(facilities_new['Record ID'], errors='coerce').astype('Int64')
        
        # Clean data: Remove duplicates and invalid records
        log_step("    Cleaning new facilities data")
        original_count = len(facilities_new)
        
        # Remove records with missing CCN
        facilities_new = facilities_new[facilities_new['CCN'].notna() & (facilities_new['CCN'] != '')]
        
        # Remove duplicates based on CCN (keep first occurrence)
        facilities_new = facilities_new.drop_duplicates(subset=['CCN'], keep='first')
        
        cleaned_count = len(facilities_new)
        log_step(f"    Data cleaning", f"Removed {original_count - cleaned_count} duplicate/invalid records")
        
        facilities_new.to_csv("hubspot_import/step1_new_records/facilities_new.csv", index=False)
        log_step("    Saved", "hubspot_import/step1_new_records/facilities_new.csv")
    
    if not companies_new.empty:
        # Ensure Record ID is integer (for new records that have Record IDs)
        if 'Record ID' in companies_new.columns:
            companies_new['Record ID'] = pd.to_numeric(companies_new['Record ID'], errors='coerce').astype('Int64')
        
        # Clean data: Remove duplicates and invalid records
        log_step("    Cleaning new companies data")
        original_count = len(companies_new)
        
        # Remove records with missing DHC ID
        companies_new = companies_new[companies_new['DHC ID'].notna() & (companies_new['DHC ID'] != '')]
        
        # Remove duplicates based on DHC ID (keep first occurrence)
        companies_new = companies_new.drop_duplicates(subset=['DHC ID'], keep='first')
        
        cleaned_count = len(companies_new)
        log_step(f"    Data cleaning", f"Removed {original_count - cleaned_count} duplicate/invalid records")
        
        companies_new.to_csv("hubspot_import/step1_new_records/companies_new.csv", index=False)
        log_step("    Saved", "hubspot_import/step1_new_records/companies_new.csv")
    
    if not contacts_new.empty:
        # Ensure Record ID is integer (for new records that have Record IDs)
        if 'Record ID' in contacts_new.columns:
            contacts_new['Record ID'] = pd.to_numeric(contacts_new['Record ID'], errors='coerce').astype('Int64')
        
        # Clean data: Remove duplicates and invalid records
        log_step("    Cleaning new contacts data")
        original_count = len(contacts_new)
        
        # Remove records with missing DHC ID
        contacts_new = contacts_new[contacts_new['DHC ID'].notna() & (contacts_new['DHC ID'] != '')]
        
        # Remove duplicates based on DHC ID (keep first occurrence)
        contacts_new = contacts_new.drop_duplicates(subset=['DHC ID'], keep='first')
        
        cleaned_count = len(contacts_new)
        log_step(f"    Data cleaning", f"Removed {original_count - cleaned_count} duplicate/invalid records")
        
        contacts_new.to_csv("hubspot_import/step1_new_records/contacts_new.csv", index=False)
        log_step("    Saved", "hubspot_import/step1_new_records/contacts_new.csv")
    
    # Save update files (Step 2)
    log_step("  Saving Step 2: Update files")
    if not facilities_existing.empty:
        # Ensure Record ID is integer
        facilities_existing['Record ID'] = pd.to_numeric(facilities_existing['Record ID'], errors='coerce').astype('Int64')
        
        # Remove pipeline columns from update files (these are only for new records)
        facilities_existing_clean = facilities_existing.drop(columns=['Facility pipeline', 'Facility pipeline stage'], errors='ignore')
        
        # Clean data: Remove duplicates and invalid records
        log_step("    Cleaning facilities data")
        original_count = len(facilities_existing_clean)
        
        # Remove records with missing CCN
        facilities_existing_clean = facilities_existing_clean[facilities_existing_clean['CCN'].notna() & (facilities_existing_clean['CCN'] != '')]
        
        # Remove duplicates based on CCN (keep first occurrence)
        facilities_existing_clean = facilities_existing_clean.drop_duplicates(subset=['CCN'], keep='first')
        
        # Remove duplicates based on Record ID (keep first occurrence)
        facilities_existing_clean = facilities_existing_clean.drop_duplicates(subset=['Record ID'], keep='first')
        
        cleaned_count = len(facilities_existing_clean)
        log_step(f"    Data cleaning", f"Removed {original_count - cleaned_count} duplicate/invalid records")
        
        facilities_existing_clean.to_csv("hubspot_import/step2_updates/facilities_updates.csv", index=False)
        log_step("    Saved", "hubspot_import/step2_updates/facilities_updates.csv")
    
    if not companies_existing.empty:
        # Ensure Record ID is integer
        companies_existing['Record ID'] = pd.to_numeric(companies_existing['Record ID'], errors='coerce').astype('Int64')
        
        # Clean data: Remove duplicates and invalid records
        log_step("    Cleaning companies data")
        original_count = len(companies_existing)
        
        # Remove records with missing DHC ID
        companies_existing = companies_existing[companies_existing['DHC ID'].notna() & (companies_existing['DHC ID'] != '')]
        
        # Remove duplicates based on DHC ID (keep first occurrence)
        companies_existing = companies_existing.drop_duplicates(subset=['DHC ID'], keep='first')
        
        # Remove duplicates based on Record ID (keep first occurrence)
        companies_existing = companies_existing.drop_duplicates(subset=['Record ID'], keep='first')
        
        cleaned_count = len(companies_existing)
        log_step(f"    Data cleaning", f"Removed {original_count - cleaned_count} duplicate/invalid records")
        
        companies_existing.to_csv("hubspot_import/step2_updates/companies_updates.csv", index=False)
        log_step("    Saved", "hubspot_import/step2_updates/companies_updates.csv")
    
    if not contacts_existing.empty:
        # Ensure Record ID is integer
        contacts_existing['Record ID'] = pd.to_numeric(contacts_existing['Record ID'], errors='coerce').astype('Int64')
        
        # Clean data: Remove duplicates and invalid records
        log_step("    Cleaning contacts data")
        original_count = len(contacts_existing)
        
        # Remove records with missing DHC ID
        contacts_existing = contacts_existing[contacts_existing['DHC ID'].notna() & (contacts_existing['DHC ID'] != '')]
        
        # Remove duplicates based on DHC ID (keep first occurrence)
        contacts_existing = contacts_existing.drop_duplicates(subset=['DHC ID'], keep='first')
        
        # Remove duplicates based on Record ID (keep first occurrence)
        contacts_existing = contacts_existing.drop_duplicates(subset=['Record ID'], keep='first')
        
        cleaned_count = len(contacts_existing)
        log_step(f"    Data cleaning", f"Removed {original_count - cleaned_count} duplicate/invalid records")
        
        contacts_existing.to_csv("hubspot_import/step2_updates/contacts_updates.csv", index=False)
        log_step("    Saved", "hubspot_import/step2_updates/contacts_updates.csv")
    
    return {
        'facilities_new': len(facilities_new),
        'facilities_existing': len(facilities_existing),
        'companies_new': len(companies_new),
        'companies_existing': len(companies_existing),
        'contacts_new': len(contacts_new),
        'contacts_existing': len(contacts_existing)
    }

def process_associations():
    """Process facility-company associations from MasterORG data"""
    log_step("Processing associations")
    
    # Load MasterORG data
    master_df = pd.read_csv("definitive/MasterORG.csv", low_memory=False)
    
    # Filter for SNF and ALF facilities
    facility_types = ['Skilled Nursing Facility', 'Assisted Living Facility']
    facilities = master_df[
        master_df['Facility subtype'].isin(facility_types) &
        (master_df['Facility status'] == 'Active')
    ].copy()
    
    # Apply SNF-specific filter
    snf_facilities = facilities[facilities['Facility subtype'] == 'Skilled Nursing Facility']
    alf_facilities = facilities[facilities['Facility subtype'] == 'Assisted Living Facility']
    
    snf_facilities = snf_facilities[
        (snf_facilities['Provider number'].notna()) &
        (snf_facilities['Provider number'] != '')
    ]
    
    facilities = pd.concat([snf_facilities, alf_facilities], ignore_index=True)
    
    # Get companies
    network_ids = set(facilities['Network ID'].dropna().astype(int))
    companies = master_df[master_df['Facility definitive ID'].astype(int).isin(network_ids)].copy()
    
    # Create facility-company associations using merge
    facility_company_associations = facilities.merge(
        companies[['Facility definitive ID', 'Facility name']], 
        left_on='Network ID', 
        right_on='Facility definitive ID', 
        how='inner',
        suffixes=('_facility', '_company')
    )
    
    # Format the associations
    associations_df = pd.DataFrame({
        'Facility DHC ID': facility_company_associations['Facility definitive ID_facility'],
        'Facility Name': facility_company_associations['Facility name_facility'],
        'Company DHC ID': facility_company_associations['Facility definitive ID_company'],
        'Company Name': facility_company_associations['Facility name_company'],
        'Network Name': facility_company_associations['Network'].fillna(''),
        'Association Type': 'Facility-Company'
    })
    
    log_step("  Created facility-company associations", f"{len(associations_df)} associations")
    
    return associations_df

def process_contact_associations():
    """Process contact-facility and contact-company associations"""
    log_step("Processing contact associations")
    
    # Load executives data
    executives_df = pd.read_csv('definitive/Long_Term_Care_Executives.csv', low_memory=False, engine='c')
    
    # Filter by firm type
    allowed_firm_types = [
        'Assisted Living Facility',
        'Assisted Living Facility Corporation', 
        'Skilled Nursing Facility',
        'Skilled Nursing Facility Corporation'
    ]
    
    filtered_df = executives_df[executives_df['FIRM_TYPE'].isin(allowed_firm_types)]
    
    # Filter contacts to only include those associated with facilities in valid US states
    valid_states = get_valid_us_states()
    
    # Load MasterORG to get facility states
    master_df = pd.read_csv("definitive/MasterORG.csv", low_memory=False)
    
    # Get facilities in valid US states
    us_facilities = master_df[master_df['State'].isin(valid_states)]
    us_facility_ids = set(us_facilities['Facility definitive ID'].astype(int))
    
    # Filter contacts to only include those associated with US facilities
    filtered_df = filtered_df[filtered_df['HOSPITAL_ID'].astype(int).isin(us_facility_ids)]
    
    # Create contact-facility associations
    contact_facility_associations = pd.DataFrame({
        'Contact DHC ID': filtered_df['GLOBAL_PERSON_ID'],
        'Contact Name': filtered_df['FIRST_NAME'] + ' ' + filtered_df['LAST_NAME'],
        'Facility DHC ID': filtered_df['HOSPITAL_ID'],
        'Facility Name': filtered_df['HOSPITAL_NAME'],
        'Contact Title': filtered_df['TITLE'],
        'Association Type': 'Contact-Facility'
    })
    
    log_step("  Created contact-facility associations", f"{len(contact_facility_associations)} associations")
    
    # Create contact-company associations via facility networks
    master_df = pd.read_csv("definitive/MasterORG.csv", low_memory=False)
    
    # Get facility-company mapping
    facility_types = ['Skilled Nursing Facility', 'Assisted Living Facility']
    facilities = master_df[
        master_df['Facility subtype'].isin(facility_types) &
        (master_df['Facility status'] == 'Active')
    ].copy()
    
    # Apply SNF-specific filter
    snf_facilities = facilities[facilities['Facility subtype'] == 'Skilled Nursing Facility']
    alf_facilities = facilities[facilities['Facility subtype'] == 'Assisted Living Facility']
    
    snf_facilities = snf_facilities[
        (snf_facilities['Provider number'].notna()) &
        (snf_facilities['Provider number'] != '')
    ]
    
    facilities = pd.concat([snf_facilities, alf_facilities], ignore_index=True)
    
    # Get companies
    network_ids = set(facilities['Network ID'].dropna().astype(int))
    companies = master_df[master_df['Facility definitive ID'].astype(int).isin(network_ids)].copy()
    
    # Create facility-company mapping
    facility_company_map = facilities[['Facility definitive ID', 'Network ID']].merge(
        companies[['Facility definitive ID', 'Facility name']], 
        left_on='Network ID', 
        right_on='Facility definitive ID', 
        how='inner'
    )
    
    # Join contacts with facility-company mapping
    contact_company_associations = filtered_df.merge(
        facility_company_map,
        left_on='HOSPITAL_ID',
        right_on='Facility definitive ID_x',
        how='inner'
    )
    
    # Format the associations
    contact_company_associations_df = pd.DataFrame({
        'Contact DHC ID': contact_company_associations['GLOBAL_PERSON_ID'],
        'Contact Name': contact_company_associations['FIRST_NAME'] + ' ' + contact_company_associations['LAST_NAME'],
        'Company DHC ID': contact_company_associations['Facility definitive ID_y'],
        'Company Name': contact_company_associations['Facility name'],
        'Contact Title': contact_company_associations['TITLE'],
        'Association Type': 'Contact-Company'
    })
    
    log_step("  Created contact-company associations", f"{len(contact_company_associations_df)} associations")
    
    return contact_facility_associations, contact_company_associations_df

def match_associations_with_record_ids(facility_company_df, contact_facility_df, contact_company_df):
    """Match associations with HubSpot Record IDs"""
    log_step("Matching associations with Record IDs")
    
    # Load HubSpot data with optimized settings
    hubspot_facilities = pd.read_csv("gemini/facilities.csv", low_memory=False, engine='c')
    hubspot_companies = pd.read_csv("gemini/companies.csv", low_memory=False, engine='c')
    hubspot_contacts = pd.read_csv("gemini/contacts.csv", low_memory=False, engine='c')
    
    # Create lookup dictionaries using vectorized operations
    # Filter and clean HubSpot data
    facility_lookup = hubspot_facilities[
        (hubspot_facilities['DHC ID'].notna()) & 
        (hubspot_facilities['DHC ID'] != '') & 
        (hubspot_facilities['Record ID'].notna()) & 
        (hubspot_facilities['Record ID'] != '')
    ][['DHC ID', 'Record ID']].copy()
    
    company_lookup = hubspot_companies[
        (hubspot_companies['DHC ID'].notna()) & 
        (hubspot_companies['DHC ID'] != '') & 
        (hubspot_companies['Record ID'].notna()) & 
        (hubspot_companies['Record ID'] != '')
    ][['DHC ID', 'Record ID']].copy()
    
    contact_lookup = hubspot_contacts[
        (hubspot_contacts['DHC ID'].notna()) & 
        (hubspot_contacts['DHC ID'] != '') & 
        (hubspot_contacts['Record ID'].notna()) & 
        (hubspot_contacts['Record ID'] != '')
    ][['DHC ID', 'Record ID']].copy()
    
    # Convert DHC IDs to numeric
    facility_lookup['DHC ID'] = pd.to_numeric(facility_lookup['DHC ID'], errors='coerce')
    company_lookup['DHC ID'] = pd.to_numeric(company_lookup['DHC ID'], errors='coerce')
    contact_lookup['DHC ID'] = pd.to_numeric(contact_lookup['DHC ID'], errors='coerce')
    
    # Create lookup dictionaries
    facility_dhc_to_record = dict(zip(facility_lookup['DHC ID'], facility_lookup['Record ID']))
    company_dhc_to_record = dict(zip(company_lookup['DHC ID'], company_lookup['Record ID']))
    contact_dhc_to_record = dict(zip(contact_lookup['DHC ID'], contact_lookup['Record ID']))
    
    # Add Record IDs using vectorized operations
    facility_company_df['Facility Record ID'] = facility_company_df['Facility DHC ID'].map(facility_dhc_to_record).fillna('')
    facility_company_df['Company Record ID'] = facility_company_df['Company DHC ID'].map(company_dhc_to_record).fillna('')
    
    contact_facility_df['Contact Record ID'] = contact_facility_df['Contact DHC ID'].map(contact_dhc_to_record).fillna('')
    contact_facility_df['Facility Record ID'] = contact_facility_df['Facility DHC ID'].map(facility_dhc_to_record).fillna('')
    
    contact_company_df['Contact Record ID'] = contact_company_df['Contact DHC ID'].map(contact_dhc_to_record).fillna('')
    contact_company_df['Company Record ID'] = contact_company_df['Company DHC ID'].map(company_dhc_to_record).fillna('')
    
    # Count matches
    facility_company_matches = len(facility_company_df[
        (facility_company_df['Facility Record ID'] != '') & 
        (facility_company_df['Company Record ID'] != '')
    ])
    
    contact_facility_matches = len(contact_facility_df[
        (contact_facility_df['Contact Record ID'] != '') & 
        (contact_facility_df['Facility Record ID'] != '')
    ])
    
    contact_company_matches = len(contact_company_df[
        (contact_company_df['Contact Record ID'] != '') & 
        (contact_company_df['Company Record ID'] != '')
    ])
    
    log_step("  Matched associations", f"Facility-Company: {facility_company_matches}, Contact-Facility: {contact_facility_matches}, Contact-Company: {contact_company_matches}")
    
    return facility_company_df, contact_facility_df, contact_company_df

def process_association_changes(facility_company_df, contact_facility_df, contact_company_df):
    """Process associations into add/remove files"""
    log_step("Processing association changes")
    
    # Load current HubSpot associations
    try:
        hubspot_facilities = pd.read_csv("gemini/facilities.csv", low_memory=False)
        hubspot_contacts = pd.read_csv("gemini/contacts.csv", low_memory=False)
    except FileNotFoundError:
        log_step("  No existing HubSpot data found, creating add-only files")
        return save_association_files(facility_company_df, contact_facility_df, contact_company_df)
    
    # Process each association type
    association_changes = {}
    
    # Facility-Company associations
    if not facility_company_df.empty:
        facility_company_changes = process_facility_company_changes(facility_company_df, hubspot_facilities)
        association_changes['facility_company'] = facility_company_changes
    
    # Contact-Facility associations  
    if not contact_facility_df.empty:
        contact_facility_changes = process_contact_facility_changes(contact_facility_df, hubspot_contacts)
        association_changes['contact_facility'] = contact_facility_changes
    
    # Contact-Company associations
    if not contact_company_df.empty:
        contact_company_changes = process_contact_company_changes(contact_company_df, hubspot_contacts)
        association_changes['contact_company'] = contact_company_changes
    
    # Save the processed files
    save_processed_associations(association_changes)
    
    return {
        'facility_company': len(facility_company_df),
        'contact_facility': len(contact_facility_df),
        'contact_company': len(contact_company_df)
    }

def process_facility_company_changes(facility_company_df, hubspot_facilities):
    """Process facility-company association changes"""
    # Filter to only include associations with Record IDs (existing records)
    facility_company_existing = facility_company_df[
        (facility_company_df['Facility Record ID'] != '') &
        (facility_company_df['Company Record ID'] != '') &
        (facility_company_df['Facility Record ID'].notna()) &
        (facility_company_df['Company Record ID'].notna())
    ].copy()
    
    # Get all associations (including those without Record IDs yet)
    facility_company_all = facility_company_df.copy()
    
    if facility_company_all.empty:
        return {'add': pd.DataFrame(), 'remove': pd.DataFrame()}
    
    # Get current associations from HubSpot using vectorized operations
    hubspot_facilities_clean = hubspot_facilities[
        (hubspot_facilities['Associated Company IDs'].notna()) & 
        (hubspot_facilities['Associated Company IDs'] != '') &
        (hubspot_facilities['Record ID'].notna())
    ].copy()
    
    if not hubspot_facilities_clean.empty:
        # Convert to string and split semicolon-separated values
        hubspot_facilities_clean['Association ID'] = hubspot_facilities_clean['Associated Company IDs'].astype(str).str.split(';')
        current_df = hubspot_facilities_clean.explode('Association ID')
        
        # Clean and convert
        current_df = current_df[current_df['Association ID'].str.strip() != ''].copy()
        current_df['Association ID'] = pd.to_numeric(current_df['Association ID'].str.strip(), errors='coerce')
        current_df = current_df[current_df['Association ID'].notna()].copy()
        
        # Format for comparison
        current_df = current_df[['Record ID', 'Association ID']].copy()
        current_df['Association Type'] = 'Facility-Company'
        current_df['Record ID'] = current_df['Record ID'].astype(int)
        current_df['Association ID'] = current_df['Association ID'].astype(int)
    else:
        current_df = pd.DataFrame(columns=['Record ID', 'Association ID', 'Association Type'])
    
    # Create add and remove dataframes
    add_df = pd.DataFrame(columns=['Record ID', 'Association ID', 'Association Type'])
    remove_df = pd.DataFrame(columns=['Record ID', 'Association ID', 'Association Type'])
    
    # If we have existing associations with Record IDs, process them
    if not facility_company_existing.empty:
        # Format for comparison (convert to integers)
        facility_company_existing_formatted = pd.DataFrame({
            'Record ID': facility_company_existing['Facility Record ID'].astype(int),
            'Association ID': facility_company_existing['Company Record ID'].astype(int),
            'Association Type': 'Facility-Company'
        })
        
        # Find associations to remove and add
        if not current_df.empty:
            # Find associations to remove (in current but not in new)
            remove_df = current_df.merge(facility_company_existing_formatted, on=['Record ID', 'Association ID'], how='left', indicator=True)
            remove_df = remove_df[remove_df['_merge'] == 'left_only'][['Record ID', 'Association ID', 'Association Type_x']].rename(columns={'Association Type_x': 'Association Type'})
            
            # Find associations to add (in new but not in current)
            add_df = facility_company_existing_formatted.merge(current_df, on=['Record ID', 'Association ID'], how='left', indicator=True)
            add_df = add_df[add_df['_merge'] == 'left_only'][['Record ID', 'Association ID', 'Association Type_x']].rename(columns={'Association Type_x': 'Association Type'})
        else:
            add_df = facility_company_existing_formatted
    
    # If we have associations without Record IDs yet, create placeholder add files
    # These will be processed after the records are imported
    if facility_company_existing.empty and not facility_company_all.empty:
        # Create a placeholder add file with all associations
        add_df = pd.DataFrame({
            'Record ID': ['TBD'] * len(facility_company_all),
            'Association ID': ['TBD'] * len(facility_company_all),
            'Association Type': ['Facility-Company'] * len(facility_company_all)
        })
    
    return {'add': add_df, 'remove': remove_df}

def process_contact_facility_changes(contact_facility_df, hubspot_contacts):
    """Process contact-facility association changes"""
    # Filter to only include associations with Record IDs (existing records)
    contact_facility_existing = contact_facility_df[
        (contact_facility_df['Contact Record ID'] != '') &
        (contact_facility_df['Facility Record ID'] != '') &
        (contact_facility_df['Contact Record ID'].notna()) &
        (contact_facility_df['Facility Record ID'].notna())
    ].copy()
    
    # Get all associations (including those without Record IDs yet)
    contact_facility_all = contact_facility_df.copy()
    
    if contact_facility_all.empty:
        return {'add': pd.DataFrame(), 'remove': pd.DataFrame()}
    
    # Get current associations from HubSpot using vectorized operations
    hubspot_contacts_clean = hubspot_contacts[
        (hubspot_contacts['Associated Facility IDs'].notna()) & 
        (hubspot_contacts['Associated Facility IDs'] != '') &
        (hubspot_contacts['Record ID'].notna())
    ].copy()
    
    if not hubspot_contacts_clean.empty:
        # Convert to string and split semicolon-separated values
        hubspot_contacts_clean['Association ID'] = hubspot_contacts_clean['Associated Facility IDs'].astype(str).str.split(';')
        current_df = hubspot_contacts_clean.explode('Association ID')
        
        # Clean and convert
        current_df = current_df[current_df['Association ID'].str.strip() != ''].copy()
        current_df['Association ID'] = pd.to_numeric(current_df['Association ID'].str.strip(), errors='coerce')
        current_df = current_df[current_df['Association ID'].notna()].copy()
        
        # Format for comparison
        current_df = current_df[['Record ID', 'Association ID']].copy()
        current_df['Association Type'] = 'Contact-Facility'
        current_df['Record ID'] = current_df['Record ID'].astype(int)
        current_df['Association ID'] = current_df['Association ID'].astype(int)
    else:
        current_df = pd.DataFrame(columns=['Record ID', 'Association ID', 'Association Type'])
    
    # Create add and remove dataframes
    add_df = pd.DataFrame(columns=['Record ID', 'Association ID', 'Association Type'])
    remove_df = pd.DataFrame(columns=['Record ID', 'Association ID', 'Association Type'])
    
    # If we have existing associations with Record IDs, process them
    if not contact_facility_existing.empty:
        # Format for comparison (convert to integers)
        contact_facility_existing_formatted = pd.DataFrame({
            'Record ID': contact_facility_existing['Contact Record ID'].astype(int),
            'Association ID': contact_facility_existing['Facility Record ID'].astype(int),
            'Association Type': 'Contact-Facility'
        })
        
        # Find associations to remove and add
        if not current_df.empty:
            # Find associations to remove (in current but not in new)
            remove_df = current_df.merge(contact_facility_existing_formatted, on=['Record ID', 'Association ID'], how='left', indicator=True)
            remove_df = remove_df[remove_df['_merge'] == 'left_only'][['Record ID', 'Association ID', 'Association Type_x']].rename(columns={'Association Type_x': 'Association Type'})
            
            # Find associations to add (in new but not in current)
            add_df = contact_facility_existing_formatted.merge(current_df, on=['Record ID', 'Association ID'], how='left', indicator=True)
            add_df = add_df[add_df['_merge'] == 'left_only'][['Record ID', 'Association ID', 'Association Type_x']].rename(columns={'Association Type_x': 'Association Type'})
        else:
            add_df = contact_facility_existing_formatted
    
    # If we have associations without Record IDs yet, create placeholder add files
    # These will be processed after the records are imported
    if contact_facility_existing.empty and not contact_facility_all.empty:
        # Create a placeholder add file with all associations
        add_df = pd.DataFrame({
            'Record ID': ['TBD'] * len(contact_facility_all),
            'Association ID': ['TBD'] * len(contact_facility_all),
            'Association Type': ['Contact-Facility'] * len(contact_facility_all)
        })
    
    return {'add': add_df, 'remove': remove_df}

def process_contact_company_changes(contact_company_df, hubspot_contacts):
    """Process contact-company association changes"""
    # Filter to only include associations with Record IDs (existing records)
    contact_company_new = contact_company_df[
        (contact_company_df['Contact Record ID'] != '') &
        (contact_company_df['Company Record ID'] != '') &
        (contact_company_df['Contact Record ID'].notna()) &
        (contact_company_df['Company Record ID'].notna())
    ].copy()
    
    if contact_company_new.empty:
        return {'add': pd.DataFrame(), 'remove': pd.DataFrame()}
    
    # Format for comparison (convert to integers)
    contact_company_new_formatted = pd.DataFrame({
        'Record ID': contact_company_new['Contact Record ID'].astype(int),
        'Association ID': contact_company_new['Company Record ID'].astype(int),
        'Association Type': 'Contact-Company'
    })
    
    # Get current associations from HubSpot using vectorized operations
    hubspot_contacts_clean = hubspot_contacts[
        (hubspot_contacts['Associated Company IDs'].notna()) & 
        (hubspot_contacts['Associated Company IDs'] != '') &
        (hubspot_contacts['Record ID'].notna())
    ].copy()
    
    if not hubspot_contacts_clean.empty:
        # Convert to string and split semicolon-separated values
        hubspot_contacts_clean['Association ID'] = hubspot_contacts_clean['Associated Company IDs'].astype(str).str.split(';')
        current_df = hubspot_contacts_clean.explode('Association ID')
        
        # Clean and convert
        current_df = current_df[current_df['Association ID'].str.strip() != ''].copy()
        current_df['Association ID'] = pd.to_numeric(current_df['Association ID'].str.strip(), errors='coerce')
        current_df = current_df[current_df['Association ID'].notna()].copy()
        
        # Format for comparison
        current_df = current_df[['Record ID', 'Association ID']].copy()
        current_df['Association Type'] = 'Contact-Company'
        current_df['Record ID'] = current_df['Record ID'].astype(int)
        current_df['Association ID'] = current_df['Association ID'].astype(int)
    else:
        current_df = pd.DataFrame(columns=['Record ID', 'Association ID', 'Association Type'])
    
    # Find associations to remove and add
    if not current_df.empty:
        # Find associations to remove (in current but not in new)
        remove_df = current_df.merge(contact_company_new_formatted, on=['Record ID', 'Association ID'], how='left', indicator=True)
        remove_df = remove_df[remove_df['_merge'] == 'left_only'][['Record ID', 'Association ID', 'Association Type_x']].rename(columns={'Association Type_x': 'Association Type'})
    else:
        remove_df = pd.DataFrame(columns=['Record ID', 'Association ID', 'Association Type'])
    
    # Find associations to add (in new but not in current)
    if not current_df.empty:
        add_df = contact_company_new_formatted.merge(current_df, on=['Record ID', 'Association ID'], how='left', indicator=True)
        add_df = add_df[add_df['_merge'] == 'left_only'][['Record ID', 'Association ID', 'Association Type_x']].rename(columns={'Association Type_x': 'Association Type'})
    else:
        add_df = contact_company_new_formatted
    
    return {'add': add_df, 'remove': remove_df}

def save_processed_associations(association_changes):
    """Save processed association files to add/remove folders"""
    log_step("Saving processed association files")
    
    for assoc_type, changes in association_changes.items():
        add_df = changes['add']
        remove_df = changes['remove']
        
        # Save add files
        if not add_df.empty:
            add_df.to_csv(f"hubspot_import/step3_associations/add/{assoc_type}_add.csv", index=False)
            log_step(f"  Saved {assoc_type} add", f"{len(add_df)} associations")
        
        # Save remove files
        if not remove_df.empty:
            remove_df.to_csv(f"hubspot_import/step3_associations/remove/{assoc_type}_remove.csv", index=False)
            log_step(f"  Saved {assoc_type} remove", f"{len(remove_df)} associations")
    
    log_step("  Saved files", "hubspot_import/step3_associations/add/ and remove/")

def save_association_files(facility_company_df, contact_facility_df, contact_company_df):
    """Save association files (fallback when no existing data)"""
    log_step("Saving Step 3: Association files")
    
    # Save association files
    facility_company_df.to_csv("hubspot_import/step3_associations/facility_company_associations.csv", index=False)
    contact_facility_df.to_csv("hubspot_import/step3_associations/contact_facility_associations.csv", index=False)
    contact_company_df.to_csv("hubspot_import/step3_associations/contact_company_associations.csv", index=False)
    
    log_step("  Saved files", "hubspot_import/step3_associations/*.csv")
    
    return {
        'facility_company': len(facility_company_df),
        'contact_facility': len(contact_facility_df),
        'contact_company': len(contact_company_df)
    }

def main():
    """Main processing pipeline for all data types"""
    print("=" * 80)
    print("COMPLETE DATA PROCESSING PIPELINE")
    print("=" * 80)
    
    # Create organized output directories
    os.makedirs("hubspot_import", exist_ok=True)
    os.makedirs("hubspot_import/step1_new_records", exist_ok=True)
    os.makedirs("hubspot_import/step2_updates", exist_ok=True)
    os.makedirs("hubspot_import/step3_associations", exist_ok=True)
    os.makedirs("hubspot_import/step3_associations/remove", exist_ok=True)
    os.makedirs("hubspot_import/step3_associations/add", exist_ok=True)
    os.makedirs("hubspot_import/raw_data", exist_ok=True)
    
    try:
        # Load MasterORG data
        master_df = load_masterorg_data()
        
        # Process facilities from MasterORG
        facilities_df = process_facilities_from_masterorg(master_df)
        
        # Process companies from MasterORG
        companies_df = process_companies_from_masterorg(master_df)
        
        # Process contacts from executives data
        contacts_df = create_formatted_contacts()
        
        # Match Record IDs for all data types
        facilities_df, companies_df, contacts_df, facility_matches, company_matches, contact_matches = match_record_ids(
            facilities_df, companies_df, contacts_df
        )
        
        # Save raw formatted data
        log_step("Saving raw formatted data")
        facilities_df.to_csv("hubspot_import/raw_data/formatted_facilities.csv", index=False)
        companies_df.to_csv("hubspot_import/raw_data/formatted_companies.csv", index=False)
        contacts_df.to_csv("hubspot_import/raw_data/formatted_contacts.csv", index=False)
        log_step("  Saved files", "hubspot_import/raw_data/formatted_*.csv")
        
        # Create import files
        import_results = create_import_files(facilities_df, companies_df, contacts_df)
        
        # Clear memory by deleting large dataframes
        del facilities_df, companies_df, contacts_df
        
        # Process associations
        facility_company_df = process_associations()
        contact_facility_df, contact_company_df = process_contact_associations()
        
        # Match associations with Record IDs
        facility_company_df, contact_facility_df, contact_company_df = match_associations_with_record_ids(
            facility_company_df, contact_facility_df, contact_company_df
        )
        
        # Save association files
        association_counts = process_association_changes(
            facility_company_df, contact_facility_df, contact_company_df
        )
        facility_company_count = association_counts['facility_company']
        contact_facility_count = association_counts['contact_facility']
        contact_company_count = association_counts['contact_company']
        
        # Clear memory
        del facility_company_df, contact_facility_df, contact_company_df
        
        # Generate comprehensive summary
        print("\n" + "=" * 80)
        print("COMPLETE DATA PROCESSING SUMMARY")
        print("=" * 80)
        
        # Facilities summary
        facility_new = import_results['facilities_new']
        facility_existing = import_results['facilities_existing']
        print(f"FACILITIES:")
        print(f"  Total: {facility_new + facility_existing}")
        print(f"  Existing (to update): {facility_existing}")
        print(f"  New (to create): {facility_new}")
        
        # Companies summary
        company_new = import_results['companies_new']
        company_existing = import_results['companies_existing']
        print(f"\nCOMPANIES:")
        print(f"  Total: {company_new + company_existing}")
        print(f"  Existing (to update): {company_existing}")
        print(f"  New (to create): {company_new}")
        
        # Contacts summary
        contact_new = import_results['contacts_new']
        contact_existing = import_results['contacts_existing']
        print(f"\nCONTACTS:")
        print(f"  Total: {contact_new + contact_existing}")
        print(f"  Existing (to update): {contact_existing}")
        print(f"  New (to create): {contact_new}")
        
        # Associations summary
        print(f"\nASSOCIATIONS:")
        print(f"  Facility-Company: {facility_company_count}")
        print(f"  Contact-Facility: {contact_facility_count}")
        print(f"  Contact-Company: {contact_company_count}")
        print(f"  Total associations: {facility_company_count + contact_facility_count + contact_company_count}")
        
        # Grand totals!
        total_records = facility_new + facility_existing + company_new + company_existing + contact_new + contact_existing
        total_existing = facility_existing + company_existing + contact_existing
        total_new = facility_new + company_new + contact_new
        
        print(f"\nGRAND TOTALS:")
        print(f"  Total Records: {total_records}")
        print(f"  Existing (to update): {total_existing}")
        print(f"  New (to create): {total_new}")
        print(f"  Match Rate: {(total_existing/total_records*100):.1f}%")
        
        print(f"\n" + "=" * 80)
        print("HUBSPOT IMPORT WORKFLOW")
        print("=" * 80)
        print(f"All files organized in: hubspot_import/")
        print(f"")
        print(f"STEP 1: Import New Records")
        print(f"   Files: hubspot_import/step1_new_records/")
        print(f"   Order: Companies -> Facilities -> Contacts")
        print(f"   Records: {company_new} companies, {facility_new} facilities, {contact_new} contacts")
        print(f"")
        print(f"STEP 2: Update Existing Records")
        print(f"   Files: hubspot_import/step2_updates/")
        print(f"   Order: Companies -> Facilities -> Contacts")
        print(f"   Records: {company_existing} companies, {facility_existing} facilities, {contact_existing} contacts")
        print(f"")
        print(f"STEP 3: Handle Associations")
        print(f"   Files: hubspot_import/step3_associations/")
        print(f"   Process: Remove old associations -> Add new associations")
        print(f"   Associations: {facility_company_count + contact_facility_count + contact_company_count} total")
        print(f"")
        print(f"NEXT STEPS:")
        print(f"   1. Import Step 1 files to HubSpot")
        print(f"   2. Export updated HubSpot data")
        print(f"   3. Re-run this script to track progress")
        print(f"   4. Import Step 2 files to HubSpot")
        print(f"   5. Import Step 3 associations to HubSpot")
        
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        raise

if __name__ == "__main__":
    main()
