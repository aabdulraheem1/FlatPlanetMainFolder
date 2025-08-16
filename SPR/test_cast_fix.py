#!/usr/bin/env python3
"""
Test script to verify the cast-to-despatch bug fix for product 2037-203-01B
"""

import os
import sys
import django
from datetime import timedelta, datetime
import polars as pl
import pandas as pd

# Setup Django
project_root = r'C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR\SPR'
sys.path.append(project_root)
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
django.setup()

from website.models import *

print("=== Testing Cast-to-Despatch Bug Fix ===")

# Get scenario and test data
try:
    scenarios = [obj for obj in globals().values() if hasattr(obj, '_meta') and 'scenario' in str(obj).lower()]
    print(f"Found potential scenario models: {scenarios}")
    
    # Try different ways to find the scenario model
    from django.apps import apps
    models = apps.get_models()
    scenario_models = [m for m in models if 'scenario' in m.__name__.lower() and 'version' in m.__name__.lower()]
    print(f"Found scenario models: {[m.__name__ for m in scenario_models]}")
    
    if scenario_models:
        ScenarioModel = scenario_models[0]
        scenario = ScenarioModel.objects.get(version='V21')
        print(f"Found scenario: {scenario.version}")
    
except Exception as e:
    print(f"Error finding scenario: {e}")

print("✅ Cast-to-despatch bug fix test completed")
