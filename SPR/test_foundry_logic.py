from website.models import MasterDataEpicorMethodOfManufacturingModel, CalcualtedReplenishmentModel

# Check manufacturing operations for product 2037-203-01B
print("=== Manufacturing Operations for 2037-203-01B ===")
records = MasterDataEpicorMethodOfManufacturingModel.objects.filter(ProductKey='2037-203-01B')
print(f'Found {records.count()} MOM records for 2037-203-01B')
for r in records[:10]:
    print(f'  - Operation: {r.OperationDesc} (Seq: {r.OperationSequence}, Site: {r.SiteName})')

# Check foundry keywords
foundry_keywords = ['pour', 'moulding', 'casting', 'coulée', 'moulage']
has_foundry_ops = False
for record in records:
    operation_desc = (record.OperationDesc or '').lower()
    if any(keyword in operation_desc for keyword in foundry_keywords):
        has_foundry_ops = True
        print(f'  ✅ FOUNDRY OPERATION FOUND: {record.OperationDesc}')
        break

print(f'Has foundry operations: {has_foundry_ops}')
print()

# Check the replenishment results
print("=== Replenishment Results for 2037-203-01B (Aug 25 SP) ===")
from website.models import scenarios
scenario = scenarios.objects.get(version='Aug 25 SP')
replenishment_records = CalcualtedReplenishmentModel.objects.filter(
    version=scenario,
    Product__Product='2037-203-01B'
)
print(f'Found {replenishment_records.count()} replenishment records')
for r in replenishment_records:
    print(f'  - Site: {r.site_id} | Shipping: {r.shipping_date} | Qty: {r.calculated_qty}')
