# Production Allocation Excel Template Structure

## Excel File Format

The Excel template for Production Allocation has the following structure:

### Column Headers (Row 1):
| Column | Header | Description |
|--------|--------|-------------|
| A | Product | Product code/ID that exists in the system |
| B | Month | 3-letter month abbreviation (Jan, Feb, Mar, etc.) |
| C | Year | 4-digit year (2025, 2026, etc.) |
| D | Total_Qty | Total quantity to be allocated across sites |
| E | Site1_Name | First site name (must exist in system) |
| F | Site1_Percentage | Percentage allocated to Site1 (0-100) |
| G | Site2_Name | Second site name (optional, leave blank if not used) |
| H | Site2_Percentage | Percentage allocated to Site2 (0-100) |
| I | Site3_Name | Third site name (optional, leave blank if not used) |
| J | Site3_Percentage | Percentage allocated to Site3 (0-100) |

### Example Data (Row 3 onwards):
| Product | Month | Year | Total_Qty | Site1_Name | Site1_Percentage | Site2_Name | Site2_Percentage | Site3_Name | Site3_Percentage |
|---------|-------|------|-----------|-----------|----------------|-----------|----------------|-----------|----------------|
| 2037-203-01B | Aug | 2025 | 1000 | Brisbane Foundry | 60 | Sydney Foundry | 40 | | 0 |
| 2037-203-01B | Sep | 2025 | 1500 | Brisbane Foundry | 100 | | 0 | | 0 |
| PROD-123-XYZ | Oct | 2025 | 2000 | Melbourne Foundry | 30 | Brisbane Foundry | 50 | Sydney Foundry | 20 |

## Validation Rules:

1. **Product**: Must match existing products in the MasterDataProductModel
2. **Month**: Must be 3-letter format (Jan, Feb, Mar, Apr, May, Jun, Jul, Aug, Sep, Oct, Nov, Dec)
3. **Year**: Must be 4-digit integer
4. **Total_Qty**: Must be a positive number
5. **Site Names**: Must match existing sites in MasterDataPlantModel
6. **Percentages**: Must sum to exactly 100% per product/month/year combination
7. **Optional Sites**: Site2 and Site3 can be left blank if not needed
8. **No Duplicates**: Each Product/Month/Year combination should appear only once

## Processing Logic:

1. **File Upload**: User uploads Excel file through the modal
2. **Validation**: System validates all data against business rules
3. **Delete Existing**: For each valid row, delete existing CalculatedProductionModel records for that product/month/year
4. **Create New Records**: Create new allocation records based on the percentages
5. **Calculate Quantities**: `allocated_qty = total_qty × (percentage / 100)`
6. **Calculate Tonnes**: `tonnes = allocated_qty × (product.DressMass / 1000)`

## Error Handling:

- **Warnings**: Non-critical issues (invalid products, percentage mismatches) are logged but don't stop processing
- **Skipped Rows**: Invalid rows are skipped with detailed error messages
- **Success Report**: Shows number of records processed, warnings, and summary
- **Rollback**: Each product/month combination is processed atomically

## Usage Workflow:

1. Click "Excel Upload" tab in Production Allocation modal
2. Click "Template" button to download the Excel template
3. Fill in your allocation data following the structure above
4. Upload the completed file
5. Review any warnings or errors in the upload results
6. System automatically applies the allocations to CalculatedProductionModel

This Excel upload method allows for bulk allocation updates instead of manual entry through the UI.
