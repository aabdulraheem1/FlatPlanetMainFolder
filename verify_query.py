#!/usr/bin/env python
import os
import django
import sys

sys.path.append('c:/Users/aali/OneDrive - bradken.com/Data/Training/SPR/SPR')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
django.setup()

from website.models import scenarios, CalcualtedReplenishmentModel

# Check the exact scenario and product data
print("🔍 Checking exact database values...")

scenario = scenarios.objects.get(version='Aug 25 SP')
print(f"✅ Scenario ID in database: '{scenario.version}'")

records = CalcualtedReplenishmentModel.objects.filter(
    version=scenario,
    Product_id='1979-102-01C'
)

print(f"✅ Found {records.count()} records for product '1979-102-01C'")

for record in records:
    print(f"   - version_id: '{record.version.version}'")
    print(f"   - Product_id: '{record.Product_id}'")
    print(f"   - Site_id: '{record.Site_id}'")
    print(f"   - ShippingDate: {record.ShippingDate}")
    print(f"   - ReplenishmentQty: {record.ReplenishmentQty}")
    print("   ---")

# Check if there might be any case sensitivity or whitespace issues
all_scenarios = scenarios.objects.all()
print(f"\n📋 All available scenarios:")
for s in all_scenarios:
    print(f"   - '{s.version}' (Length: {len(s.version)})")

# Check for any variations of the product code
all_products = CalcualtedReplenishmentModel.objects.filter(
    Product_id__icontains='1979-102-01'
).values_list('Product_id', flat=True).distinct()

print(f"\n🔍 Similar product codes in database:")
for product in all_products:
    print(f"   - '{product}'")
