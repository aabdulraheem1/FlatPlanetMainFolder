#!/usr/bin/env python
import os
import sys
import django

# Add the SPR directory to Python path
sys.path.append(r'C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR\SPR')

# Setup Django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import SMART_Forecast_Model, scenarios
import pandas as pd

def analyze_dewb135_forecast():
    scenario = scenarios.objects.get(version='Jul 25 SPR Inv')
    
    # Get all DEWB135-1 forecast records
    forecast_records = SMART_Forecast_Model.objects.filter(
        version=scenario,
        Product='DEWB135-1'
    ).values('Product', 'Location', 'Period_AU', 'Forecast_Region', 'Customer_code', 'Qty')
    
    df = pd.DataFrame(list(forecast_records))
    print(f'Raw forecast records for DEWB135-1: {len(df)}')
    
    # Apply the same transformation as in the management command
    def transform_location(location):
        if location:
            if "_" in location:
                return location.split("_", 1)[1][:4]
            elif "-" in location:
                return location.split("-", 1)[1][:4]
        return location
    
    df['Location'] = df['Location'].apply(transform_location)
    
    # Group as in the management command
    unique_combos = df.groupby(['Product', 'Location', 'Period_AU', 'Forecast_Region', 'Customer_code'])['Qty'].sum().reset_index()
    print(f'After grouping: {len(unique_combos)}')
    print(f'Total quantity before: {df["Qty"].sum()}')
    print(f'Total quantity after: {unique_combos["Qty"].sum()}')
    
    print(f'\nUnique locations: {sorted(df["Location"].unique())}')
    print(f'Unique forecast regions: {sorted(df["Forecast_Region"].unique())}')
    print(f'Unique customer codes: {sorted(df["Customer_code"].unique())}')
    
    # Show sample combinations
    print('\nFirst 10 unique combinations:')
    for i, row in unique_combos.head(10).iterrows():
        print(f'{row["Period_AU"]} | {row["Location"]} | {row["Forecast_Region"]} | {row["Customer_code"]} | Qty: {row["Qty"]}')
    
    return unique_combos

if __name__ == "__main__":
    analyze_dewb135_forecast()
