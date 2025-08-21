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

from website.models import MasterdataIncoTermsModel, MasterDataIncotTermTypesModel, MasterDataFreightModel

print("=== INCOTERMS MODEL ===")
print(f"Total records: {MasterdataIncoTermsModel.objects.count()}")
for obj in MasterdataIncoTermsModel.objects.all()[:10]:
    print(f"{obj.id}: {obj}")
    print(f"  Fields: {[field.name for field in obj._meta.fields]}")
    break

print("\n=== INCOTERM TYPES MODEL ===")
print(f"Total records: {MasterDataIncotTermTypesModel.objects.count()}")
for obj in MasterDataIncotTermTypesModel.objects.all()[:10]:
    print(f"{obj.id}: {obj}")
    print(f"  Fields: {[field.name for field in obj._meta.fields]}")
    break

print("\n=== FREIGHT MODEL ===")
print(f"Total records: {MasterDataFreightModel.objects.count()}")
for obj in MasterDataFreightModel.objects.all()[:10]:
    print(f"{obj.id}: {obj}")
    print(f"  Fields: {[field.name for field in obj._meta.fields]}")
    break

# Look for relationships to SMART_Forecast_Model
from website.models import SMART_Forecast_Model

print("\n=== SMART FORECAST INCOTERMS RELATIONSHIP ===")
# Check if SMART_Forecast_Model has any incoterm-related fields
smart_fields = [field.name for field in SMART_Forecast_Model._meta.fields]
incoterm_fields = [field for field in smart_fields if 'incoterm' in field.lower() or 'freight' in field.lower()]
print(f"SMART Forecast incoterm-related fields: {incoterm_fields}")

# Sample a few SMART forecast records to see their incoterm data
print("\nSample SMART forecast records:")
for obj in SMART_Forecast_Model.objects.all()[:5]:
    print(f"ID {obj.id}: Site={getattr(obj, 'Site', 'N/A')}, Product={getattr(obj, 'Product', 'N/A')}")
    for field in incoterm_fields:
        if hasattr(obj, field):
            value = getattr(obj, field)
            print(f"  {field}: {value}")
    print()
