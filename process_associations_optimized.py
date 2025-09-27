#!/usr/bin/env python3
"""
Process association data efficiently using pandas joins:
1. Facility-Company associations (Facility definitive ID = Network ID)
2. Contact-Facility associations (GLOBAL_PERSON_ID → HOSPITAL_ID)
3. Contact-Company associations (via facility's network)
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

def process_facility_company_associations():
    """Extract facility-company associations using pandas joins"""
    log_step("Processing facility-company associations")
    
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
    
    # Get companies (records whose Facility definitive ID is a Network ID for facilities)
    network_ids = set(facilities['Network ID'].dropna().astype(int))
    companies = master_df[master_df['Facility definitive ID'].astype(int).isin(network_ids)].copy()
    
    # Create facility-company associations using merge
    # Join facilities with companies where facility's Network ID = company's Facility definitive ID
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

def process_contact_facility_associations():
    """Extract contact-facility associations using pandas joins"""
    log_step("Processing contact-facility associations")
    
    # Load executives data
    executives_df = pd.read_csv('definitive/Long_Term_Care_Executives.csv', low_memory=False)
    
    # Filter by firm type
    allowed_firm_types = [
        'Assisted Living Facility',
        'Assisted Living Facility Corporation', 
        'Skilled Nursing Facility',
        'Skilled Nursing Facility Corporation'
    ]
    
    filtered_df = executives_df[executives_df['FIRM_TYPE'].isin(allowed_firm_types)]
    
    # Create contact-facility associations
    associations_df = pd.DataFrame({
        'Contact DHC ID': filtered_df['GLOBAL_PERSON_ID'],
        'Contact Name': filtered_df['FIRST_NAME'] + ' ' + filtered_df['LAST_NAME'],
        'Facility DHC ID': filtered_df['HOSPITAL_ID'],
        'Facility Name': filtered_df['HOSPITAL_NAME'],
        'Contact Title': filtered_df['TITLE'],
        'Association Type': 'Contact-Facility'
    })
    
    log_step("  Created contact-facility associations", f"{len(associations_df)} associations")
    
    return associations_df

def process_contact_company_associations():
    """Extract contact-company associations via facility networks"""
    log_step("Processing contact-company associations")
    
    # Load executives data
    executives_df = pd.read_csv('definitive/Long_Term_Care_Executives.csv', low_memory=False)
    
    # Filter by firm type
    allowed_firm_types = [
        'Assisted Living Facility',
        'Assisted Living Facility Corporation', 
        'Skilled Nursing Facility',
        'Skilled Nursing Facility Corporation'
    ]
    
    filtered_df = executives_df[executives_df['FIRM_TYPE'].isin(allowed_firm_types)]
    
    # Load MasterORG data
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
    associations_df = pd.DataFrame({
        'Contact DHC ID': contact_company_associations['GLOBAL_PERSON_ID'],
        'Contact Name': contact_company_associations['FIRST_NAME'] + ' ' + contact_company_associations['LAST_NAME'],
        'Company DHC ID': contact_company_associations['Facility definitive ID_y'],
        'Company Name': contact_company_associations['Facility name'],
        'Contact Title': contact_company_associations['TITLE'],
        'Association Type': 'Contact-Company'
    })
    
    log_step("  Created contact-company associations", f"{len(associations_df)} associations")
    
    return associations_df

def match_associations_with_record_ids(facility_company_df, contact_facility_df, contact_company_df):
    """Match associations with HubSpot Record IDs using efficient lookups"""
    log_step("Matching associations with Record IDs")
    
    # Load HubSpot data
    hubspot_facilities = pd.read_csv("gemini/facilities.csv", low_memory=False)
    hubspot_companies = pd.read_csv("gemini/companies.csv", low_memory=False)
    hubspot_contacts = pd.read_csv("gemini/contacts.csv", low_memory=False)
    
    # Create lookup dictionaries efficiently (handle NaN values)
    facility_dhc_to_record = {}
    for idx, row in hubspot_facilities.iterrows():
        dhc_id = str(row['DHC ID']).strip()
        record_id = str(row['Record ID']).strip()
        if dhc_id and dhc_id != 'nan' and record_id and record_id != 'nan':
            try:
                facility_dhc_to_record[int(float(dhc_id))] = record_id
            except (ValueError, TypeError):
                continue
    
    company_dhc_to_record = {}
    for idx, row in hubspot_companies.iterrows():
        dhc_id = str(row['DHC ID']).strip()
        record_id = str(row['Record ID']).strip()
        if dhc_id and dhc_id != 'nan' and record_id and record_id != 'nan':
            try:
                company_dhc_to_record[int(float(dhc_id))] = record_id
            except (ValueError, TypeError):
                continue
    
    contact_dhc_to_record = {}
    for idx, row in hubspot_contacts.iterrows():
        dhc_id = str(row['DHC ID']).strip()
        record_id = str(row['Record ID']).strip()
        if dhc_id and dhc_id != 'nan' and record_id and record_id != 'nan':
            try:
                contact_dhc_to_record[int(float(dhc_id))] = record_id
            except (ValueError, TypeError):
                continue
    
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

def main():
    """Main processing pipeline for associations"""
    print("=" * 80)
    print("ASSOCIATION PROCESSING PIPELINE (OPTIMIZED)")
    print("=" * 80)
    
    # Create output directories
    os.makedirs("formatted_data", exist_ok=True)
    os.makedirs("associations", exist_ok=True)
    
    try:
        # Process facility-company associations
        facility_company_df = process_facility_company_associations()
        
        # Process contact-facility associations
        contact_facility_df = process_contact_facility_associations()
        
        # Process contact-company associations
        contact_company_df = process_contact_company_associations()
        
        # Match with Record IDs
        facility_company_df, contact_facility_df, contact_company_df = match_associations_with_record_ids(
            facility_company_df, contact_facility_df, contact_company_df
        )
        
        # Save association files
        log_step("Saving association files")
        facility_company_df.to_csv("associations/facility_company_associations.csv", index=False)
        contact_facility_df.to_csv("associations/contact_facility_associations.csv", index=False)
        contact_company_df.to_csv("associations/contact_company_associations.csv", index=False)
        
        log_step("  Saved files", "associations/facility_company_associations.csv, associations/contact_facility_associations.csv, associations/contact_company_associations.csv")
        
        # Generate summary
        print("\n" + "=" * 80)
        print("ASSOCIATION PROCESSING SUMMARY")
        print("=" * 80)
        print(f"Facility-Company Associations: {len(facility_company_df)}")
        print(f"Contact-Facility Associations: {len(contact_facility_df)}")
        print(f"Contact-Company Associations: {len(contact_company_df)}")
        print(f"Total Associations: {len(facility_company_df) + len(contact_facility_df) + len(contact_company_df)}")
        
        print(f"\nFiles ready for HubSpot association import!")
        print(f"Association files are separate from your formatted data files.")
        
    except Exception as e:
        print(f"\nERROR: {str(e)}")
        raise

if __name__ == "__main__":
    main()
