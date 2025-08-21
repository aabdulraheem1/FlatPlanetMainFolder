#!/usr/bin/env python
"""
Test the 1:1 replenishment fix for Z14EP
"""

import os
import sys
import django

# Add the SPR project root to the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
spr_root = current_dir
if spr_root not in sys.path:
    sys.path.insert(0, spr_root)

# Set up Django environment
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')

try:
    django.setup()
except Exception as e:
    print(f"Django setup error: {e}")
    parent_dir = os.path.dirname(spr_root)
    if parent_dir not in sys.path:
        sys.path.insert(0, parent_dir)
    django.setup()

from website.management.commands.populate_calculated_replenishment_v3_optimized import Command

def test_new_replenishment():
    """Test the 1:1 replenishment logic for Z14EP"""
    
    print("🧪 TESTING NEW 1:1 REPLENISHMENT LOGIC")
    print("=" * 50)
    
    # Run the command for just a few records
    command = Command()
    
    try:
        # Run with very small limit to test
        command.handle(version="Aug 25 SPR", limit=10)
        print("✅ Command completed successfully")
        
    except Exception as e:
        print(f"❌ Error during command execution: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_new_replenishment()
