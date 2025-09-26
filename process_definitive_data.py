#!/usr/bin/env python3
"""
Complete pipeline to process Definitive Healthcare data for HubSpot import
This script handles the entire process from raw data to formatted import files
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

def process_snf_data():
    """Process SNF data from SNF_Overview.csv"""
    log_step("Processing SNF data")
    
    # Load SNF data
    snf_df = pd.read_csv("definitive/SNF_Overview.csv", low_memory=False)
    log_step("  Loaded SNF data", f"{len(snf_df)} records")
    
    # Filter SNF facilities (exclude corporations, active only, with provider number)
    snf_facilities = snf_df[
        (snf_df['FIRM_TYPE'] != 'Skilled Nursing Facility Corporation') &
        (snf_df['COMPANY_STATUS'] == 'Active') &
        (snf_df['PROVIDER_NUMBER'].notna()) &
        (snf_df['PROVIDER_NUMBER'] != '')
    ].copy()
    
    log_step("  Filtered SNF facilities", f"{len(snf_facilities)} records")
    
    # Map fields to HubSpot format
    snf_formatted = pd.DataFrame({
        'Name of Facility': snf_facilities['HOSPITAL_NAME'],
        'CCN': snf_facilities['PROVIDER_NUMBER'],
        'Facility Type': 'SNF',
        'Street': snf_facilities['HQ_ADDRESS'],
        'City': snf_facilities['HQ_CITY'],
        'State': snf_facilities['HQ_STATE'],
        'Zip Code': snf_facilities['HQ_ZIP_CODE'].apply(format_zip_code),
        'Phone Number': snf_facilities['HQ_PHONE'].apply(format_phone_number),
        'NPI': snf_facilities['NPI_NUMBER'],
        'Facility website': snf_facilities['WEBSITE'],
        'Total Beds': snf_facilities['NUMBER_BEDS'],
        'DHC ID': snf_facilities['HOSPITAL_ID'],
        'Network Name': snf_facilities['NETWORK_NAME'],
        'Company Status': snf_facilities['COMPANY_STATUS'],
        'Ownership': snf_facilities['HOSPITAL_OWNERSHIP'],
        'Region': snf_facilities['HQ_REGION'],
        'County': snf_facilities['HQ_COUNTY'],
        'Record Source': 'Definitive Healthcare',
        'Is Gemini Prospect': '',
        'Is Tricura Prospect': ''
    })
    
    # Create full address columns
    snf_formatted["Facility's Address"] = snf_formatted.apply(
        lambda row: create_full_address(row['Street'], row['City'], row['State'], row['Zip Code']), 
        axis=1
    )
    
    # Add pipeline columns
    snf_formatted['Facility pipeline'] = 'Gemini Onboarding'
    snf_formatted['Facility pipeline stage'] = 'Lead'
    
    # Add Unique Facility Address
    snf_formatted['Unique Facility Address'] = snf_formatted["Facility's Address"]
    
    log_step("  Formatted SNF facilities", f"{len(snf_formatted)} records")
    
    return snf_formatted

def process_alf_data():
    """Process ALF data from ALF_Overview.csv"""
    log_step("Processing ALF data")
    
    # Load ALF data
    alf_df = pd.read_csv("definitive/ALF_Overview.csv", low_memory=False)
    log_step("  Loaded ALF data", f"{len(alf_df)} records")
    
    # Filter ALF facilities (exclude corporations, active only)
    alf_facilities = alf_df[
        (alf_df['FIRM_TYPE'] != 'Assisted Living Facility Corporation') &
        (alf_df['COMPANY_STATUS'] == 'Active')
    ].copy()
    
    log_step("  Filtered ALF facilities", f"{len(alf_facilities)} records")
    
    # Map fields to HubSpot format
    alf_formatted = pd.DataFrame({
        'Name of Facility': alf_facilities['HOSPITAL_NAME'],
        'CCN': '',  # ALFs typically don't have CCNs
        'Facility Type': 'ALF',
        'Street': alf_facilities['HQ_ADDRESS'],
        'City': alf_facilities['HQ_CITY'],
        'State': alf_facilities['HQ_STATE'],
        'Zip Code': alf_facilities['HQ_ZIP_CODE'].apply(format_zip_code),
        'Phone Number': alf_facilities['HQ_PHONE'].apply(format_phone_number),
        'NPI': '',  # ALFs typically don't have NPIs
        'Facility website': alf_facilities['WEBSITE'],
        'Total Beds': alf_facilities['NUMBER_BEDS'],
        'DHC ID': alf_facilities['HOSPITAL_ID'],
        'Network Name': alf_facilities['NETWORK_NAME'],
        'Company Status': alf_facilities['COMPANY_STATUS'],
        'Ownership': '',  # ALF file doesn't have ownership column
        'Region': alf_facilities['HQ_REGION'],
        'County': alf_facilities['HQ_COUNTY'],
        'Record Source': 'Definitive Healthcare',
        'Is Gemini Prospect': '',
        'Is Tricura Prospect': ''
    })
    
    # Create full address columns
    alf_formatted["Facility's Address"] = alf_formatted.apply(
        lambda row: create_full_address(row['Street'], row['City'], row['State'], row['Zip Code']), 
        axis=1
    )
    
    # Add pipeline columns
    alf_formatted['Facility pipeline'] = 'Gemini Onboarding'
    alf_formatted['Facility pipeline stage'] = 'Lead'
    
    # Add Unique Facility Address
    alf_formatted['Unique Facility Address'] = alf_formatted["Facility's Address"]
    
    log_step("  Formatted ALF facilities", f"{len(alf_formatted)} records")
    
    return alf_formatted

def process_corporations():
    """Process corporation data from MasterORG.csv"""
    log_step("Processing corporation data")
    
    # Load master facilities data
    master_df = pd.read_csv("definitive/MasterORG.csv", low_memory=False)
    log_step("  Loaded master facilities", f"{len(master_df)} records")
    
    # Load SNF and ALF data to get NETWORK_IDs
    snf_df = pd.read_csv("definitive/SNF_Overview.csv", low_memory=False)
    alf_df = pd.read_csv("definitive/ALF_Overview.csv", low_memory=False)
    
    # Get all NETWORK_IDs from facilities
    snf_network_ids = set(snf_df['NETWORK_ID'].dropna().astype(int))
    alf_network_ids = set(alf_df['NETWORK_ID'].dropna().astype(int))
    all_network_ids = snf_network_ids.union(alf_network_ids)
    
    log_step("  Found network IDs", f"{len(all_network_ids)} unique IDs")
    
    # Filter corporations (facilities whose ID is a NETWORK_ID for other facilities)
    corporations = master_df[
        master_df['Facility definitive ID'].astype(int).isin(all_network_ids)
    ].copy()
    
    log_step("  Filtered corporations", f"{len(corporations)} records")
    
    # Map fields to HubSpot format
    corporations_formatted = pd.DataFrame({
        'Company name': corporations['Facility name'],
        'DHC ID': corporations['Facility definitive ID'],
        'Street Address': corporations['AddressLine 1'].fillna('') + ' ' + corporations['AddressLine 2'].fillna(''),
        'City': corporations['City'],
        'State/Region': corporations['State'],
        'Postal Code': corporations['Zip code'].apply(format_zip_code),
        'Phone Number': corporations['Organization phone'].apply(format_phone_number),
        'Country/Region': 'United States',
        'Lifecycle Stage': 'Lead'
    })
    
    # Create HQ Unique Address
    corporations_formatted['HQ Unique Address'] = corporations_formatted.apply(
        lambda row: create_full_address(row['Street Address'], row['City'], row['State/Region'], row['Postal Code']), 
        axis=1
    )
    
    # Clean up Street Address (remove extra spaces)
    corporations_formatted['Street Address'] = corporations_formatted['Street Address'].str.strip()
    
    log_step("  Formatted corporations", f"{len(corporations_formatted)} records")
    
    return corporations_formatted

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
    """Main processing pipeline"""
    print("=" * 60)
    print("DEFINITIVE HEALTHCARE DATA PROCESSING PIPELINE")
    print("=" * 60)
    
    # Create output directories
    os.makedirs("formatted_data", exist_ok=True)
    os.makedirs("hubspot_updates", exist_ok=True)
    
    try:
        # Process SNF data
        snf_facilities = process_snf_data()
        
        # Process ALF data
        alf_facilities = process_alf_data()
        
        # Combine facilities
        log_step("Combining facilities")
        all_facilities = pd.concat([snf_facilities, alf_facilities], ignore_index=True)
        log_step("  Combined facilities", f"{len(all_facilities)} total records")
        
        # Process corporations
        corporations = process_corporations()
        
        # Match Record IDs
        all_facilities, corporations = match_record_ids(all_facilities, corporations)
        
        # Save formatted data
        log_step("Saving formatted data")
        all_facilities.to_csv("formatted_data/formatted_facilities.csv", index=False)
        corporations.to_csv("formatted_data/formatted_companies.csv", index=False)
        
        log_step("  Saved files", "formatted_facilities.csv, formatted_companies.csv")
        
        # Generate summary
        print("\n" + "=" * 60)
        print("PROCESSING SUMMARY")
        print("=" * 60)
        print(f"Total Facilities: {len(all_facilities)}")
        print(f"  - With Record ID (updates): {len(all_facilities[all_facilities['Record ID'] != ''])}")
        print(f"  - Without Record ID (new): {len(all_facilities[all_facilities['Record ID'] == ''])}")
        print(f"Total Companies: {len(corporations)}")
        print(f"  - With Record ID (updates): {len(corporations[corporations['Record ID'] != ''])}")
        print(f"  - Without Record ID (new): {len(corporations[corporations['Record ID'] == ''])}")
        print("\nFiles ready for HubSpot import!")
        
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        raise

if __name__ == "__main__":
    main()
