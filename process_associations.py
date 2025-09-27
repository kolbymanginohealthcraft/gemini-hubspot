#!/usr/bin/env python3
"""
Process association data:
1. Facility-Company associations
2. Contact-Facility associations  
3. Contact-Company associations
4. Output separate association files for HubSpot import
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
    """Extract facility-company associations from MasterORG data"""
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
    
    # Create facility-company associations
    facility_company_associations = []
    
    for idx, facility in facilities.iterrows():
        facility_dhc_id = facility['Facility definitive ID']
        network_id = facility['Network ID']
        network_name = facility['Network']
        
        if pd.notna(network_id) and network_id != '':
            # Find the company record with this Network ID
            company_record = master_df[master_df['Facility definitive ID'] == network_id]
            
            if not company_record.empty:
                company_dhc_id = company_record.iloc[0]['Facility definitive ID']
                company_name = company_record.iloc[0]['Facility name']
                
                facility_company_associations.append({
                    'Facility DHC ID': facility_dhc_id,
                    'Facility Name': facility['Facility name'],
                    'Company DHC ID': company_dhc_id,
                    'Company Name': company_name,
                    'Network Name': network_name if pd.notna(network_name) else '',
                    'Association Type': 'Facility-Company'
                })
    
    associations_df = pd.DataFrame(facility_company_associations)
    log_step("  Created facility-company associations", f"{len(associations_df)} associations")
    
    return associations_df

def process_contact_facility_associations():
    """Extract contact-facility associations from executives data"""
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
    contact_facility_associations = []
    
    for idx, executive in filtered_df.iterrows():
        contact_dhc_id = executive['GLOBAL_PERSON_ID']
        facility_dhc_id = executive['HOSPITAL_ID']
        contact_name = f"{executive['FIRST_NAME']} {executive['LAST_NAME']}"
        facility_name = executive['HOSPITAL_NAME']
        title = executive['TITLE']
        
        contact_facility_associations.append({
            'Contact DHC ID': contact_dhc_id,
            'Contact Name': contact_name,
            'Facility DHC ID': facility_dhc_id,
            'Facility Name': facility_name,
            'Contact Title': title,
            'Association Type': 'Contact-Facility'
        })
    
    associations_df = pd.DataFrame(contact_facility_associations)
    log_step("  Created contact-facility associations", f"{len(associations_df)} associations")
    
    return associations_df

def process_contact_company_associations():
    """Extract contact-company associations from executives data"""
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
    
    # Load MasterORG to get company information
    master_df = pd.read_csv("definitive/MasterORG.csv", low_memory=False)
    
    # Create contact-company associations
    contact_company_associations = []
    
    for idx, executive in filtered_df.iterrows():
        contact_dhc_id = executive['GLOBAL_PERSON_ID']
        facility_dhc_id = executive['HOSPITAL_ID']
        contact_name = f"{executive['FIRST_NAME']} {executive['LAST_NAME']}"
        title = executive['TITLE']
        
        # Find the facility to get its network/company
        facility_record = master_df[master_df['Facility definitive ID'] == facility_dhc_id]
        
        if not facility_record.empty:
            facility = facility_record.iloc[0]
            network_id = facility['Network ID']
            network_name = facility['Network']
            
            if pd.notna(network_id) and network_id != '':
                # Find the company record
                company_record = master_df[master_df['Facility definitive ID'] == network_id]
                
                if not company_record.empty:
                    company_dhc_id = company_record.iloc[0]['Facility definitive ID']
                    company_name = company_record.iloc[0]['Facility name']
                    
                    contact_company_associations.append({
                        'Contact DHC ID': contact_dhc_id,
                        'Contact Name': contact_name,
                        'Company DHC ID': company_dhc_id,
                        'Company Name': company_name,
                        'Network Name': network_name if pd.notna(network_name) else '',
                        'Contact Title': title,
                        'Association Type': 'Contact-Company'
                    })
    
    associations_df = pd.DataFrame(contact_company_associations)
    log_step("  Created contact-company associations", f"{len(associations_df)} associations")
    
    return associations_df

def match_associations_with_record_ids(facility_company_df, contact_facility_df, contact_company_df):
    """Match associations with HubSpot Record IDs"""
    log_step("Matching associations with Record IDs")
    
    # Load HubSpot data
    hubspot_facilities = pd.read_csv("gemini/facilities.csv", low_memory=False)
    hubspot_companies = pd.read_csv("gemini/companies.csv", low_memory=False)
    hubspot_contacts = pd.read_csv("gemini/contacts.csv", low_memory=False)
    
    # Create lookup dictionaries
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
    
    # Add Record IDs to associations
    facility_company_df['Facility Record ID'] = ''
    facility_company_df['Company Record ID'] = ''
    contact_facility_df['Contact Record ID'] = ''
    contact_facility_df['Facility Record ID'] = ''
    contact_company_df['Contact Record ID'] = ''
    contact_company_df['Company Record ID'] = ''
    
    # Match facility-company associations
    facility_company_matches = 0
    for idx, row in facility_company_df.iterrows():
        facility_dhc = row['Facility DHC ID']
        company_dhc = row['Company DHC ID']
        
        if facility_dhc in facility_dhc_to_record:
            facility_company_df.at[idx, 'Facility Record ID'] = facility_dhc_to_record[facility_dhc]
        if company_dhc in company_dhc_to_record:
            facility_company_df.at[idx, 'Company Record ID'] = company_dhc_to_record[company_dhc]
        
        if facility_dhc in facility_dhc_to_record and company_dhc in company_dhc_to_record:
            facility_company_matches += 1
    
    # Match contact-facility associations
    contact_facility_matches = 0
    for idx, row in contact_facility_df.iterrows():
        contact_dhc = row['Contact DHC ID']
        facility_dhc = row['Facility DHC ID']
        
        if contact_dhc in contact_dhc_to_record:
            contact_facility_df.at[idx, 'Contact Record ID'] = contact_dhc_to_record[contact_dhc]
        if facility_dhc in facility_dhc_to_record:
            contact_facility_df.at[idx, 'Facility Record ID'] = facility_dhc_to_record[facility_dhc]
        
        if contact_dhc in contact_dhc_to_record and facility_dhc in facility_dhc_to_record:
            contact_facility_matches += 1
    
    # Match contact-company associations
    contact_company_matches = 0
    for idx, row in contact_company_df.iterrows():
        contact_dhc = row['Contact DHC ID']
        company_dhc = row['Company DHC ID']
        
        if contact_dhc in contact_dhc_to_record:
            contact_company_df.at[idx, 'Contact Record ID'] = contact_dhc_to_record[contact_dhc]
        if company_dhc in company_dhc_to_record:
            contact_company_df.at[idx, 'Company Record ID'] = company_dhc_to_record[company_dhc]
        
        if contact_dhc in contact_dhc_to_record and company_dhc in company_dhc_to_record:
            contact_company_matches += 1
    
    log_step("  Matched associations", f"Facility-Company: {facility_company_matches}, Contact-Facility: {contact_facility_matches}, Contact-Company: {contact_company_matches}")
    
    return facility_company_df, contact_facility_df, contact_company_df

def main():
    """Main processing pipeline for associations"""
    print("=" * 80)
    print("ASSOCIATION PROCESSING PIPELINE")
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
