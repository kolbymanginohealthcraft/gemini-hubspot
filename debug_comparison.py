import pandas as pd

# Load a sample of our formatted data
facilities = pd.read_csv('hubspot_import/raw_data/formatted_facilities.csv', nrows=5)
hubspot = pd.read_csv('gemini/facilities.csv', nrows=5)

print('OUR FORMATTED FACILITIES:')
print(facilities[['Name of Facility', 'Street', 'City', 'State', 'Phone Number', 'NPI']].head(2))

print('\nHUBSPOT FACILITIES:')
print(hubspot[['Name of Facility', 'Street', 'City', 'State', 'Phone Number', 'NPI']].head(2))

print('\nCOMPARISON:')
for i in range(2):
    print(f'Record {i}:')
    for field in ['Name of Facility', 'Street', 'City', 'State', 'Phone Number', 'NPI']:
        our_val = str(facilities.iloc[i][field]).strip() if pd.notna(facilities.iloc[i][field]) else ''
        hub_val = str(hubspot.iloc[i][field]).strip() if pd.notna(hubspot.iloc[i][field]) else ''
        if our_val != hub_val:
            print(f'  {field}: "{our_val}" vs "{hub_val}"')
