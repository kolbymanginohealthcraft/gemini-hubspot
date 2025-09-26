#!/usr/bin/env python3
"""
Process Definitive Healthcare data using only MasterORG.csv
This script extracts both facilities and companies from the master file
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
        return f"({digits[:3]}) {digits[4:7]}-{digits[7:]}"
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
    
    # Load master data
    master_df = pd.read_csv("definitive/MasterORG.csv", low_memory=False)
    log_step("  Loaded MasterORG", f"{len(master_df)} records")
    
    # Show column structure
    log_step("  Column structure", f"{len(master_df.columns)} columns")
    print("    Available columns:")
    for i, col in enumerate(master_df.columns):
        print(f"      {i+1:2d}. {col}")
    
    # Show sample data
    log_step("  Sample data (first 3 rows)")
    print(master_df.head(3).to_string())
    
    # Show data types
    log_step("  Data types")
    print(master_df.dtypes.to_string())
    
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
        'NPI': facilities['Facility primary NPI'].fillna(''),
        'Facility website': '',  # Not available in MasterORG
        'Total Beds': facilities['Number of staffed beds'].fillna(''),
        'DHC ID': facilities['Facility definitive ID'],
        'Network Name': facilities['Network'].fillna(''),
        'Company Status': facilities['Facility status'],
        'Ownership': facilities['Ownership'].fillna(''),
        'Region': facilities['Region'],
        'County': facilities['County'],
        'Record Source': 'Definitive Healthcare',
        'Is Gemini Prospect': '',
        'Is Tricura Prospect': ''
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
        'Country/Region': 'United States',
        'Lifecycle Stage': 'Lead'
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

def match_record_ids(facilities_df, companies_df):
    """Match Record IDs from HubSpot data"""
    log_step("Matching Record IDs")
    
    # Load HubSpot data
    hubspot_facilities = pd.read_csv("gemini/facilities.csv", low_memory=False)
    hubspot_companies = pd.read_csv("gemini/companies.csv", low_memory=False)
    
    log_step("  Loaded HubSpot data", f"{len(hubspot_facilities)} facilities, {len(hubspot_companies)} companies")
    
    # Match facility Record IDs by CCN
    facilities_df['Record ID'] = ''
    ccn_to_record_id = {}
    for idx, row in hubspot_facilities.iterrows():
        ccn = str(row['CCN']).strip()
        record_id = str(row['Record ID']).strip()
        if ccn and ccn != 'nan' and record_id and record_id != 'nan':
            ccn_to_record_id[ccn] = record_id
    
    facility_matches = 0
    for idx, row in facilities_df.iterrows():
        ccn = str(row['CCN']).strip()
        if ccn and ccn != 'nan' and ccn in ccn_to_record_id:
            facilities_df.at[idx, 'Record ID'] = ccn_to_record_id[ccn]
            facility_matches += 1
    
    # Match company Record IDs by DHC ID
    companies_df['Record ID'] = ''
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
    
    log_step("  Matched Record IDs", f"{facility_matches} facilities, {company_matches} companies")
    
    return facilities_df, companies_df

def main():
    """Main processing pipeline using only MasterORG.csv"""
    print("=" * 60)
    print("MASTERORG-ONLY DATA PROCESSING PIPELINE")
    print("=" * 60)
    
    # Create output directories
    os.makedirs("masterorg_output", exist_ok=True)
    os.makedirs("hubspot_updates", exist_ok=True)
    
    try:
        # Load MasterORG data
        master_df = load_masterorg_data()
        
        # Process facilities from MasterORG
        facilities_df = process_facilities_from_masterorg(master_df)
        
        # Process companies from MasterORG
        companies_df = process_companies_from_masterorg(master_df)
        
        # Match Record IDs
        facilities_df, companies_df = match_record_ids(facilities_df, companies_df)
        
        # Save formatted data
        if not facilities_df.empty and not companies_df.empty:
            log_step("Saving formatted data")
            facilities_df.to_csv("masterorg_output/formatted_facilities.csv", index=False)
            companies_df.to_csv("masterorg_output/formatted_companies.csv", index=False)
            log_step("  Saved files", "masterorg_output/formatted_facilities.csv, masterorg_output/formatted_companies.csv")
            
            # Generate summary
            print("\n" + "=" * 60)
            print("PROCESSING SUMMARY")
            print("=" * 60)
            print(f"Total Facilities: {len(facilities_df)}")
            print(f"Total Companies: {len(companies_df)}")
            print("\nFiles ready for HubSpot import!")
        else:
            log_step("Processing incomplete", "No facilities or companies found")
        
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        raise

if __name__ == "__main__":
    main()
