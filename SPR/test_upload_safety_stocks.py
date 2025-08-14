#!/usr/bin/env python
import os
import sys

if __name__ == "__main__":
    # Add the project directory to Python path
    project_dir = os.path.dirname(os.path.abspath(__file__))
    sys.path.insert(0, project_dir)
    
    os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
    
    import django
    django.setup()
    
    # Now test the upload_safety_stocks functionality
    from website.models import scenarios, MasterDataSafetyStocks
    from website.views import run_management_command
    
    print("Testing upload_safety_stocks view functionality...")
    
    # Get scenario
    scenario = scenarios.objects.get(version='Aug 25 SP')
    print(f"Testing with scenario: {scenario}")
    
    # Clear existing records
    deleted_count = MasterDataSafetyStocks.objects.filter(version=scenario).delete()
    print(f"Cleared {deleted_count[0]} existing records")
    
    # Test the run_management_command function directly
    print("Testing run_management_command...")
    try:
        result = run_management_command('fetch_safety_stocks_data', 'Aug 25 SP')
        print(f"Return code: {result.returncode}")
        print(f"Success: {result.returncode == 0}")
        
        if result.returncode == 0:
            print("Command executed successfully!")
            if result.stdout:
                print(f"STDOUT: {result.stdout[:300]}...")
        else:
            print("Command failed!")
            if result.stderr:
                print(f"STDERR: {result.stderr[:300]}...")
    except Exception as e:
        print(f"Exception in run_management_command: {e}")
        import traceback
        traceback.print_exc()
    
    # Check final count
    final_count = MasterDataSafetyStocks.objects.filter(version=scenario).count()
    print(f"Final record count: {final_count}")
