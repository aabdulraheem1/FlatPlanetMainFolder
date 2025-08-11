#!/usr/bin/env python
"""
Test script to verify Django templates directory configuration
"""
import os
import sys
from pathlib import Path

# Add the spr directory to Python path
sys.path.append(os.path.dirname(__file__))

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')

# Import Django settings
import django
django.setup()

from django.conf import settings

print("🔧 DJANGO TEMPLATES CONFIGURATION TEST")
print("=" * 50)
print(f"BASE_DIR: {settings.BASE_DIR}")
print(f"Templates DIRS: {settings.TEMPLATES[0]['DIRS']}")

for template_dir in settings.TEMPLATES[0]['DIRS']:
    print(f"📁 Template directory: {template_dir}")
    print(f"   Exists: {template_dir.exists()}")
    print(f"   Is directory: {template_dir.is_dir()}")
    
    if template_dir.exists():
        # List some templates to verify
        website_templates = template_dir / 'website'
        if website_templates.exists():
            templates = list(website_templates.glob('*.html'))[:5]
            print(f"   Sample templates found: {len(templates)}")
            for template in templates:
                print(f"     - {template.name}")

print("\n🎯 TEST: Looking for review_scenario.html")
review_template = settings.TEMPLATES[0]['DIRS'][0] / 'website' / 'review_scenario.html'
print(f"Expected path: {review_template}")
print(f"Exists: {review_template.exists()}")

if review_template.exists():
    print("✅ SUCCESS: Templates configuration is correct!")
else:
    print("❌ FAILED: Templates configuration needs fixing!")
