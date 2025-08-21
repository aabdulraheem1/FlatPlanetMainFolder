"""
Excel Site Allocation Converter

This script converts the complex 8-column site allocation Excel format to the simple 3-column format
that the upload_site_allocation function expects.

Input Format (8 columns):
Product | Date | Site1_Name | Site1_Percentage | Site2_Name | Site2_Percentage | Site3_Name | Site3_Percentage

Output Format (3 columns):
Product | Site | AllocationPercentage

Usage:
    python convert_site_allocation_excel.py input_file.xlsx output_file.xlsx
"""

import pandas as pd
import sys
import os

def convert_site_allocation_excel(input_file, output_file):
    """
    Convert complex 8-column site allocation Excel to simple 3-column format
    """
    print(f"Reading Excel file: {input_file}")
    
    # Read the input Excel file
    try:
        df = pd.read_excel(input_file)
        print(f"Successfully loaded {len(df)} rows from input file")
    except Exception as e:
        print(f"Error reading Excel file: {e}")
        return False
    
    # Validate input columns
    expected_columns = ['Product', 'Date', 'Site1_Name', 'Site1_Percentage', 
                       'Site2_Name', 'Site2_Percentage', 'Site3_Name', 'Site3_Percentage']
    
    missing_columns = [col for col in expected_columns if col not in df.columns]
    if missing_columns:
        print(f"Error: Missing columns in input file: {missing_columns}")
        print(f"Found columns: {list(df.columns)}")
        return False
    
    print("Input file has correct column structure")
    
    # Create output data list
    output_data = []
    processed_rows = 0
    skipped_rows = 0
    
    print("Converting data...")
    
    for index, row in df.iterrows():
        product = row['Product']
        
        # Skip if product is empty
        if pd.isna(product) or product == '':
            skipped_rows += 1
            continue
        
        # Process Site 1
        site1_name = row['Site1_Name']
        site1_percentage = row['Site1_Percentage']
        
        if not pd.isna(site1_name) and not pd.isna(site1_percentage) and site1_percentage > 0:
            output_data.append({
                'Product': product,
                'Site': site1_name,
                'AllocationPercentage': site1_percentage
            })
        
        # Process Site 2
        site2_name = row['Site2_Name']
        site2_percentage = row['Site2_Percentage']
        
        if not pd.isna(site2_name) and not pd.isna(site2_percentage) and site2_percentage > 0:
            output_data.append({
                'Product': product,
                'Site': site2_name,
                'AllocationPercentage': site2_percentage
            })
        
        # Process Site 3
        site3_name = row['Site3_Name']
        site3_percentage = row['Site3_Percentage']
        
        if not pd.isna(site3_name) and not pd.isna(site3_percentage) and site3_percentage > 0:
            output_data.append({
                'Product': product,
                'Site': site3_name,
                'AllocationPercentage': site3_percentage
            })
        
        processed_rows += 1
        
        # Progress indicator
        if processed_rows % 100 == 0:
            print(f"Processed {processed_rows} rows...")
    
    # Create output DataFrame
    output_df = pd.DataFrame(output_data)
    
    print(f"\nConversion Summary:")
    print(f"Input rows processed: {processed_rows}")
    print(f"Input rows skipped: {skipped_rows}")
    print(f"Output rows generated: {len(output_df)}")
    
    if len(output_df) == 0:
        print("Warning: No data to export!")
        return False
    
    # Validate output data
    print("\nValidating output data...")
    
    # Check for valid revenue sites (as per your system)
    allowed_sites = ['XUZ1', 'MER1', 'WOD1', 'COI2']
    invalid_sites = output_df[~output_df['Site'].isin(allowed_sites)]['Site'].unique()
    
    if len(invalid_sites) > 0:
        print(f"Warning: Found sites that may not be valid revenue sites: {list(invalid_sites)}")
        print(f"Valid revenue sites are: {allowed_sites}")
        
        # Show breakdown by site
        site_counts = output_df['Site'].value_counts()
        print("\nSite breakdown:")
        for site, count in site_counts.items():
            status = "✓ Valid" if site in allowed_sites else "⚠ Check"
            print(f"  {site}: {count} records {status}")
    
    # Check percentage totals per product
    print("\nChecking percentage totals per product...")
    percentage_totals = output_df.groupby('Product')['AllocationPercentage'].sum()
    
    # Find products that don't sum to 100%
    non_100_percent = percentage_totals[abs(percentage_totals - 100) > 0.01]
    
    if len(non_100_percent) > 0:
        print(f"Warning: {len(non_100_percent)} products don't sum to exactly 100%:")
        for product, total in non_100_percent.head(10).items():
            print(f"  {product}: {total}%")
        if len(non_100_percent) > 10:
            print(f"  ... and {len(non_100_percent) - 10} more")
    else:
        print("✓ All products sum to 100%")
    
    # Save output file
    try:
        output_df.to_excel(output_file, index=False)
        print(f"\n✓ Successfully saved converted file: {output_file}")
        
        # Show sample of output data
        print(f"\nSample of converted data:")
        print(output_df.head(10).to_string(index=False))
        
        return True
        
    except Exception as e:
        print(f"Error saving output file: {e}")
        return False

def main():
    if len(sys.argv) != 3:
        print("Usage: python convert_site_allocation_excel.py <input_file.xlsx> <output_file.xlsx>")
        print("\nExample:")
        print("  python convert_site_allocation_excel.py 'new SKU transfer.xlsx' 'site_allocation_converted.xlsx'")
        return
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    
    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Error: Input file '{input_file}' does not exist")
        return
    
    # Check if output file already exists
    if os.path.exists(output_file):
        response = input(f"Output file '{output_file}' already exists. Overwrite? (y/N): ")
        if response.lower() != 'y':
            print("Conversion cancelled")
            return
    
    print("=" * 60)
    print("Site Allocation Excel Converter")
    print("=" * 60)
    
    success = convert_site_allocation_excel(input_file, output_file)
    
    if success:
        print("\n✓ Conversion completed successfully!")
        print(f"You can now upload '{output_file}' to your Site Allocation function.")
    else:
        print("\n✗ Conversion failed. Please check the errors above.")

if __name__ == "__main__":
    main()
