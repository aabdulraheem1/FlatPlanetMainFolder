# Add this to your tests.py file:

# Check what the inventory search returns
from website.customized_function import search_inventory_planning_results

# Try the same search that your HTML page is using
results = search_inventory_planning_results(
    scenario_version='Jul 25 SPR Inv',
    product='T690EP',
    location='WAT1',
    site=''  # or None, depending on your function
)

print(f"\nSearch function results:")
if hasattr(results, '__len__'):
    print(f"Number of results returned by search: {len(results)}")
    if len(results) > 0:
        print(f"First result date: {results[0].get('date', 'N/A')}")
        print(f"Last result date: {results[-1].get('date', 'N/A')}")
else:
    print("Results is not a list/array")
    print(f"Results type: {type(results)}")