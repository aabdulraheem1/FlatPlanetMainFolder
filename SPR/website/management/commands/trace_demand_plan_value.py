from django.core.management.base import BaseCommand
from website.models import scenarios
from website.views import get_cached_control_tower_data
from website.customized_function import calculate_control_tower_data, get_combined_demand_and_poured_data

class Command(BaseCommand):
    help = 'Trace the exact path of demand_plan.FY25.MTJ1 value'

    def handle(self, *args, **options):
        scenario_name = "Aug 25 SP"
        
        print("🔍 TRACING demand_plan.FY25.MTJ1 VALUE PATH")
        print(f"Scenario: {scenario_name}")
        print("=" * 60)
        
        # Step 1: Get scenario object
        try:
            scenario = scenarios.objects.get(version=scenario_name)
            print(f"✅ Step 1: Scenario object found - {scenario}")
        except scenarios.DoesNotExist:
            print(f"❌ Step 1: Scenario '{scenario_name}' not found")
            return
        
        # Step 2: Call get_cached_control_tower_data (the view function)
        print(f"\n🔎 Step 2: Calling get_cached_control_tower_data(scenario)")
        try:
            view_result = get_cached_control_tower_data(scenario)
            print(f"✅ Step 2: get_cached_control_tower_data returned:")
            print(f"    Type: {type(view_result)}")
            print(f"    Keys: {view_result.keys() if isinstance(view_result, dict) else 'Not a dict'}")
            
            if 'combined_demand_plan' in view_result:
                combined_plan = view_result['combined_demand_plan']
                print(f"    combined_demand_plan type: {type(combined_plan)}")
                
                if isinstance(combined_plan, dict) and 'FY25' in combined_plan:
                    fy25_data = combined_plan['FY25']
                    print(f"    FY25 type: {type(fy25_data)}")
                    print(f"    FY25 keys: {fy25_data.keys() if isinstance(fy25_data, dict) else 'Not a dict'}")
                    
                    if isinstance(fy25_data, dict) and 'MTJ1' in fy25_data:
                        mtj1_value = fy25_data['MTJ1']
                        print(f"    📊 FOUND: demand_plan.FY25.MTJ1 = {mtj1_value}")
                    else:
                        print(f"    ❌ MTJ1 not found in FY25 data")
                else:
                    print(f"    ❌ FY25 not found or combined_demand_plan not a dict")
            else:
                print(f"    ❌ combined_demand_plan not found in result")
                
        except Exception as e:
            print(f"❌ Step 2: Error in get_cached_control_tower_data: {e}")
            import traceback
            traceback.print_exc()
        
        # Step 3: Call calculate_control_tower_data directly (what should happen)
        print(f"\n🔎 Step 3: Calling calculate_control_tower_data('{scenario_name}') directly")
        try:
            direct_result = calculate_control_tower_data(scenario_name)
            print(f"✅ Step 3: calculate_control_tower_data returned:")
            print(f"    Type: {type(direct_result)}")
            print(f"    Keys: {direct_result.keys() if isinstance(direct_result, dict) else 'Not a dict'}")
            
            if 'combined_demand_plan' in direct_result:
                combined_plan = direct_result['combined_demand_plan']
                print(f"    combined_demand_plan type: {type(combined_plan)}")
                
                if isinstance(combined_plan, dict) and 'FY25' in combined_plan:
                    fy25_data = combined_plan['FY25']
                    print(f"    FY25 type: {type(fy25_data)}")
                    print(f"    FY25 keys: {fy25_data.keys() if isinstance(fy25_data, dict) else 'Not a dict'}")
                    
                    if isinstance(fy25_data, dict) and 'MTJ1' in fy25_data:
                        mtj1_value = fy25_data['MTJ1']
                        print(f"    📊 FOUND: demand_plan.FY25.MTJ1 = {mtj1_value}")
                    else:
                        print(f"    ❌ MTJ1 not found in FY25 data")
                else:
                    print(f"    ❌ FY25 not found or combined_demand_plan not a dict")
            else:
                print(f"    ❌ combined_demand_plan not found in result")
                
        except Exception as e:
            print(f"❌ Step 3: Error in calculate_control_tower_data: {e}")
            import traceback
            traceback.print_exc()
        
        # Step 4: Call get_combined_demand_and_poured_data directly (the core function)
        print(f"\n🔎 Step 4: Calling get_combined_demand_and_poured_data(scenario) directly")
        try:
            core_result, poured_data = get_combined_demand_and_poured_data(scenario)
            print(f"✅ Step 4: get_combined_demand_and_poured_data returned:")
            print(f"    core_result type: {type(core_result)}")
            print(f"    Keys: {core_result.keys() if isinstance(core_result, dict) else 'Not a dict'}")
            
            if isinstance(core_result, dict) and 'FY25' in core_result:
                fy25_data = core_result['FY25']
                print(f"    FY25 type: {type(fy25_data)}")
                print(f"    FY25 keys: {fy25_data.keys() if isinstance(fy25_data, dict) else 'Not a dict'}")
                
                if isinstance(fy25_data, dict) and 'MTJ1' in fy25_data:
                    mtj1_value = fy25_data['MTJ1']
                    print(f"    📊 FOUND: MTJ1 = {mtj1_value}")
                else:
                    print(f"    ❌ MTJ1 not found in FY25 data")
            else:
                print(f"    ❌ FY25 not found or core_result not a dict")
                
        except Exception as e:
            print(f"❌ Step 4: Error in get_combined_demand_and_poured_data: {e}")
            import traceback
            traceback.print_exc()
        
        print("\n" + "=" * 60)
        print("🏁 TRACE COMPLETE")
        print("The template variable {{ demand_plan.FY25.MTJ1 }} gets its value from:")
        print("1. control_tower view calls get_cached_control_tower_data()")
        print("2. get_cached_control_tower_data() calls calculate_control_tower_data()")  
        print("3. calculate_control_tower_data() calls get_combined_demand_and_poured_data()")
        print("4. The result becomes context['demand_plan'] in the template")
