#!/usr/bin/env python
"""
Check database schema consistency for CalculatedProductionModel
"""
import os
import sys
import django

# Add the project path to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import CalculatedProductionModel, scenarios
from django.db import connection

def check_database_schema():
    print("🔍 Checking database schema consistency...")
    
    # Check the actual database schema
    cursor = connection.cursor()
    cursor.execute("""
        SELECT COLUMN_NAME 
        FROM INFORMATION_SCHEMA.COLUMNS 
        WHERE TABLE_NAME = 'website_calculatedproductionmodel' 
        AND COLUMN_NAME LIKE '%aud%'
        ORDER BY COLUMN_NAME
    """)
    
    db_columns = [col[0] for col in cursor.fetchall()]
    print(f"📊 Database columns with 'aud': {db_columns}")
    
    # Check model field names
    model_fields = [field.name for field in CalculatedProductionModel._meta.fields if 'aud' in field.name]
    print(f"🏗️  Model fields with 'aud': {model_fields}")
    
    # Check if there's a mismatch
    if 'production_aud' in db_columns and 'cogs_aud' in model_fields:
        print("⚠️  MISMATCH: Database has 'production_aud' but model expects 'cogs_aud'")
        return 'production_aud'
    elif 'cogs_aud' in db_columns and 'cogs_aud' in model_fields:
        print("✅ MATCH: Both database and model use 'cogs_aud'")
        return 'cogs_aud'
    else:
        print(f"❌ UNKNOWN STATE: DB columns: {db_columns}, Model fields: {model_fields}")
        return None

def test_model_creation():
    print("\n🧪 Testing model creation...")
    
    # Get a test scenario
    scenario = scenarios.objects.first()
    if not scenario:
        print("❌ No scenarios found for testing")
        return False
    
    try:
        # Try to create a test record
        test_record = CalculatedProductionModel(
            version=scenario,
            cogs_aud=100.0,
            revenue_aud=200.0,
            cost_aud=50.0
        )
        print("✅ Model accepts cogs_aud field")
        return True
    except Exception as e:
        print(f"❌ Model error: {e}")
        return False

if __name__ == "__main__":
    field_name = check_database_schema()
    test_result = test_model_creation()
    
    if field_name and test_result:
        print(f"\n🎯 Field name to use: {field_name}")
    else:
        print(f"\n⚠️  Schema inconsistency detected - may need migration fix")
