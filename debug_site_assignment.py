#!/usr/bin/env python
import os
import sys
import django
from pathlib import Path

# Add the SPR directory to Python path
spr_dir = Path(__file__).parent / "SPR"
sys.path.insert(0, str(spr_dir))

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
django.setup()

from website.models import (
    CalcualtedReplenishmentModel, scenarios, SMART_Forecast_Model,
    MasterDataManuallyAssignProductionRequirement, MasterDataHistoryOfProductionModel,
    SiteAllocationModel
)

# Get scenario
scenario = scenarios.objects.get(version="Aug 25 SPR")

print("=== BK57592A Site Assignment Investigation ===")

# Check 1: Manual Assignment (highest priority)
print("\n1. MANUAL ASSIGNMENT CHECK:")
manual_assignments = MasterDataManuallyAssignProductionRequirement.objects.filter(
    version=scenario,
    Product="BK57592A"
)
print(f"Found {manual_assignments.count()} manual assignments")
for manual in manual_assignments:
    print(f"  Manual Assignment: {manual.Product} -> {manual.Site}")

# Check 2: Order Book (existing orders - from replenishment)
print("\n2. REPLENISHMENT/ORDER BOOK CHECK:")
replenishments = CalcualtedReplenishmentModel.objects.filter(
    version=scenario,
    Product="BK57592A"
)
print(f"Found {replenishments.count()} replenishment records")
for rep in replenishments[:5]:
    print(f"  Replenishment: Site={rep.Site}, Shipping={rep.ShippingDate}, Qty={rep.Qty}")

# Check 3: Production History
print("\n3. PRODUCTION HISTORY CHECK:")
production_history = MasterDataHistoryOfProductionModel.objects.filter(
    version=scenario,
    Product="BK57592A"
)
print(f"Found {production_history.count()} production history records")
for hist in production_history[:5]:
    print(f"  History: Foundry={hist.Foundry}, Product={hist.Product}, Month={hist.ProductionMonth}")

# Check 4: Site Allocation (default percentages)
print("\n4. SITE ALLOCATION CHECK:")
site_allocations = SiteAllocationModel.objects.filter(
    version=scenario,
    Product="BK57592A"
)
print(f"Found {site_allocations.count()} site allocation records")
for alloc in site_allocations:
    print(f"  Allocation: Product={alloc.Product}, Site={alloc.Site}, Percentage={alloc.Percentage}")

# Check 5: SMART Forecast data (to see original demand location)
print("\n5. SMART FORECAST CHECK:")
smart_forecasts = SMART_Forecast_Model.objects.filter(
    version=scenario,
    Product="BK57592A"
)
print(f"Found {smart_forecasts.count()} SMART forecast records")
for forecast in smart_forecasts[:5]:
    print(f"  Forecast: Product={forecast.Product}, Location={forecast.Location}, Customer={forecast.Customer_code}, Qty={forecast.Qty}")

# Check if DTC1 is a foundry
foundry_sites = {'XUZ1', 'MTJ1', 'COI2', 'MER1', 'WOD1', 'WUN1', 'CHI1'}
print(f"\n6. SITE TYPE CHECK:")
print(f"  DTC1 is a foundry: {('DTC1' in foundry_sites)}")
print(f"  Known foundries: {foundry_sites}")

# Check what type of product BK57592A is (might need MOM operations)
print(f"\n7. PRODUCT TYPE ANALYSIS:")
print(f"  Product: BK57592A")
print("  This appears to be a SAG mill product based on production records")
print("  SAG mill products might not require foundry operations")
