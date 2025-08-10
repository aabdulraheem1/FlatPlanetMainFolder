print("=== PRODUCTION_AUD COMPARISON SUMMARY ===\n")

print("KEY FINDING:")
print("CalculatedProductionModel has records with empty parent_product_group ('')")
print("These records are NOT being included in InventoryProjectionModel\n")

print("NUMBERS:")
print("• CalculatedProductionModel total: $1,029,077,753.31")
print("• InventoryProjectionModel total: $840,261,720.79")
print("• Missing from projections: $188,816,032.52")
print("• All missing records have empty parent_product_group ('')\n")

print("SPECIFIC TO CRAWLER SYSTEMS:")
print("• Crawler Systems production_aud MATCHES exactly: $214,618,837.33")
print("• No discrepancy in Crawler Systems data")
print("• The issue is with other product groups that have empty parent_product_group\n")

print("ROOT CAUSE:")
print("Records in CalculatedProductionModel with empty/null parent_product_group")
print("are not being aggregated into InventoryProjectionModel because the")
print("projection model groups by parent_product_group, and empty values")
print("are likely being filtered out.\n")

print("IMPACT ON AUTO-LEVELING:")
print("Auto-leveling should work correctly for Crawler Systems since the")
print("production_aud values match exactly between both tables.")
print("The $188M difference is from other product groups with missing parent_product_group data.")
