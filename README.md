# HubSpot Data Updater

This tool updates your HubSpot data with information from Definitive Healthcare to enhance your healthcare facility database.

## Overview

The HubSpot Data Updater processes your existing HubSpot data (companies, contacts, facilities) and enriches it with comprehensive data from Definitive Healthcare, including:

- **Assisted Living Facilities (ALF)** data
- **Skilled Nursing Facilities (SNF)** data  
- **Executive contact information** from healthcare facilities

## Files Structure

### Input Data (Required)
```
gemini/
├── all-companies.csv      # Your existing HubSpot company records
├── all-contacts.csv       # Your existing HubSpot contact records
└── all-facilities.csv     # Your existing HubSpot facility records

definitive/
├── ALF_Overview.csv       # Definitive Healthcare ALF data
├── SNF_Overview.csv       # Definitive Healthcare SNF data
└── Long_Term_Care_Executives.csv  # Executive contact data
```

### Output Data (Generated)
```
hubspot_updates/
├── updated_companies.csv           # Companies updated with DHC data
├── new_contacts_from_executives.csv # New contacts from executives
├── new_facilities_from_definitive.csv # New facilities from DHC
└── data_update_summary.txt         # Summary report
```

## Installation

1. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Ensure your data files are in the correct folders:**
   - Place your HubSpot exports in the `gemini/` folder
   - Place your Definitive Healthcare exports in the `definitive/` folder

## Usage

### Quick Start
```bash
python run_data_update.py
```

### Advanced Usage
```python
from hubspot_data_updater import HubSpotDataUpdater

# Create updater instance
updater = HubSpotDataUpdater()

# Load data
updater.load_data()

# Generate updated files
updater.generate_hubspot_import_files()
```

## What the Tool Does

### 1. Company Data Enhancement
- **Matches** your existing HubSpot companies with Definitive Healthcare facilities
- **Adds new fields** with Definitive Healthcare data:
  - `DHC_HOSPITAL_ID`: Unique Definitive Healthcare identifier
  - `DHC_FACILITY_TYPE`: ALF or SNF classification
  - `DHC_NUMBER_BEDS`: Bed count information
  - `DHC_WEBSITE`: Official facility website
  - `DHC_PROFILE_LINK`: Link to Definitive Healthcare profile
  - `DHC_NPI_NUMBER`: National Provider Identifier
  - `DHC_COMPANY_STATUS`: Active/Inactive status
  - Address and contact information

### 2. New Contact Creation
- **Creates new contact records** from Definitive Healthcare executive data
- **Includes comprehensive information:**
  - Name, title, department
  - Email and phone numbers
  - Company association
  - Position level (C-Level, Manager, etc.)
  - Standardized titles

### 3. New Facility Addition
- **Adds new facility records** from Definitive Healthcare data
- **Includes detailed facility information:**
  - Facility name and type
  - Complete address and contact details
  - Bed count and capacity
  - Network affiliations
  - NPI and provider numbers (for SNFs)

## Data Matching Strategy

The tool uses intelligent matching to connect your existing HubSpot data with Definitive Healthcare records:

1. **Company Name Normalization:**
   - Removes common suffixes (Inc, LLC, Corp, etc.)
   - Standardizes healthcare terminology
   - Handles variations in naming conventions

2. **Match Types:**
   - **Exact Match**: Normalized names are identical
   - **Partial Match**: One name contains the other
   - **Best Match Selection**: Prioritizes exact matches over partial

3. **Data Quality:**
   - Skips records with missing essential data
   - Validates email formats and phone numbers
   - Maintains data integrity throughout the process

## HubSpot Import Process

### Step 1: Review Generated Files
- Check the `data_update_summary.txt` for processing statistics
- Review sample records in each CSV file
- Verify data quality and completeness

### Step 2: Import to HubSpot

#### For Updated Companies:
1. Go to HubSpot → Contacts → Companies
2. Click "Import" → "Import from CSV"
3. Upload `updated_companies.csv`
4. Map the new DHC_* fields to custom properties
5. Choose "Update existing records" for matching companies

#### For New Contacts:
1. Go to HubSpot → Contacts → Contacts
2. Click "Import" → "Import from CSV"
3. Upload `new_contacts_from_executives.csv`
4. Map fields to appropriate contact properties
5. Choose "Create new records"

#### For New Facilities:
1. Go to HubSpot → Contacts → Companies (or custom object)
2. Click "Import" → "Import from CSV"
3. Upload `new_facilities_from_definitive.csv`
4. Map fields to appropriate properties
5. Choose "Create new records"

### Step 3: Data Verification
- Verify that existing companies were updated correctly
- Check that new contacts are properly associated with companies
- Ensure new facilities are created with complete information
- Review any import errors or warnings

## Customization

### Adding New Fields
To add additional fields from Definitive Healthcare data:

1. **Modify the field mapping** in `update_companies_with_definitive_data()`
2. **Add new columns** to the `new_columns` list
3. **Update the assignment logic** to populate the new fields

### Changing Match Criteria
To adjust how companies are matched:

1. **Modify `normalize_company_name()`** to change name normalization
2. **Update `find_company_matches()`** to change matching logic
3. **Adjust similarity thresholds** for partial matches

### Filtering Data
To process only specific subsets of data:

1. **Add filtering logic** in the data loading methods
2. **Use pandas filtering** to select specific records
3. **Modify the processing loops** to skip unwanted records

## Troubleshooting

### Common Issues

**"File too large" errors:**
- The tool is designed to handle large files efficiently
- If you encounter memory issues, consider processing data in chunks

**"No matches found":**
- Check that company names in HubSpot match facility names in Definitive Healthcare
- Review the normalization process in the logs
- Consider manual review of unmatched records

**Import errors in HubSpot:**
- Verify that all required fields are present
- Check for special characters in data
- Ensure field mappings are correct

### Logging
The tool provides detailed logging information:
- Data loading statistics
- Matching results
- Processing progress
- Error messages and warnings

## Support

For issues or questions:
1. Check the generated `data_update_summary.txt` for processing details
2. Review the console output for error messages
3. Verify that all input files are properly formatted
4. Ensure you have the required Python dependencies installed

## Data Privacy and Security

- All processing is done locally on your machine
- No data is sent to external servers
- Original files are not modified
- Generated files contain only the data you specify

## Performance Notes

- Processing time depends on data size
- Large files (millions of records) may take several minutes
- Memory usage scales with data size
- Consider running during off-peak hours for large datasets
