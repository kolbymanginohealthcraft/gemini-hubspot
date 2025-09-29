# HubSpot Import Workflow

This directory contains all files organized for your HubSpot import process.

## Directory Structure

```
hubspot_import/
├── step1_new_records/          # STEP 1: Import new records
│   ├── companies_new.csv
│   ├── facilities_new.csv
│   └── contacts_new.csv
├── step2_updates/              # STEP 2: Update existing records
│   ├── companies_updates.csv
│   ├── facilities_updates.csv
│   └── contacts_updates.csv
├── step3_associations/         # STEP 3: Handle associations
│   ├── facility_company_associations.csv
│   ├── contact_facility_associations.csv
│   └── contact_company_associations.csv
└── raw_data/                   # Raw formatted data (reference)
    ├── formatted_facilities.csv
    ├── formatted_companies.csv
    └── formatted_contacts.csv
```

## Import Workflow

### STEP 1: Import New Records
**Order:** Companies → Facilities → Contacts

1. **Companies** (`step1_new_records/companies_new.csv`)
   - Import as "Create new records"
   - Map fields to HubSpot company properties

2. **Facilities** (`step1_new_records/facilities_new.csv`)
   - Import as "Create new records"
   - Map fields to HubSpot facility properties

3. **Contacts** (`step1_new_records/contacts_new.csv`)
   - Import as "Create new records"
   - Map fields to HubSpot contact properties

### STEP 2: Update Existing Records
**Order:** Companies → Facilities → Contacts

1. **Companies** (`step2_updates/companies_updates.csv`)
   - Import as "Update existing records"
   - Match on Record ID field

2. **Facilities** (`step2_updates/facilities_updates.csv`)
   - Import as "Update existing records"
   - Match on Record ID field

3. **Contacts** (`step2_updates/contacts_updates.csv`)
   - Import as "Update existing records"
   - Match on Record ID field

### STEP 3: Handle Associations
**Process:** Remove old associations → Add new associations

1. **Remove existing associations** (if needed)
   - Use HubSpot API or manual process
   - Clear facility-company associations
   - Clear contact-facility associations
   - Clear contact-company associations

2. **Add new associations**
   - Import association files to HubSpot
   - Use HubSpot association import feature

## File Formats

### New Records
- **No Record ID column** (not needed for creation)
- Ready for "Create new records" import

### Updates
- **Includes Record ID column** (required for matching)
- Ready for "Update existing records" import

### Associations
- **Record ID**: Source record ID
- **Association ID**: Target record ID
- **Association Type**: Type of association

## Progress Tracking

After each step:
1. Export updated HubSpot data
2. Re-run `process_all_data_complete.py`
3. Check progress in the summary output
4. Continue to next step

## Notes

- **Import order matters**: Companies before facilities, facilities before contacts
- **Associations require existing records**: Complete Steps 1 & 2 before Step 3
- **Record IDs are integers**: No decimal formatting
- **File sizes**: Large files may need chunked imports
