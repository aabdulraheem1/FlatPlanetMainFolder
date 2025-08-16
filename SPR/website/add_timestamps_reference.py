"""
Script to add timestamp fields to ALL models with version/scenario fields
Run this after the database columns have been added
"""

# Models that need timestamp fields added
models_to_update = [
    # (line_number_range, model_name, class_definition_line)
    ('MasterDataOrderBook', 139, 142),
    ('MasterDataCapacityModel', 147, 165), 
    ('MasterDataHistoryOfProductionModel', 190, 195),
    ('MasterDataIncotTermTypesModel', 200, 206),
    ('MasterdataIncoTermsModel', 211, 226),
    ('MasterDataPlan', 231, 372),
    ('MasterDataScheduleModel', 378, 385),
    ('AggregatedForecast', 390, 401),
    ('MasterDataInventory', 406, 418),
    ('MasterDataCastToDespatchModel', 434, 437),
    ('CalcualtedReplenishmentModel', 442, 450),
    ('CalculatedProductionModel', 455, 537),
    ('MasterDataEpicorSupplierMasterDataModel', 542, 570),
    ('MasterDataManuallyAssignProductionRequirement', 575, 580),
    ('ProductSiteCostModel', 585, 595),
    ('FixedPlantConversionModifiersModel', 600, 665),
    ('MasterDataSafetyStocks', 670, 690),
]

# This is a reference for manual updates
# Each model needs these two lines added before the def __str__ method:
timestamp_fields = '''
    # Timestamp fields for change tracking
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
'''

print("Reference for adding timestamp fields to all remaining models:")
print("Add these lines before the def __str__ method in each model:")
print(timestamp_fields)
print("\nModels that need updating:")
for model_name, start_line, end_line in models_to_update:
    print(f"- {model_name} (around line {start_line}-{end_line})")
