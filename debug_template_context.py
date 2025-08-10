#!/usr/bin/env python
import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import MasterDataInventory, scenarios
from django.template import Template, Context
from django.test import RequestFactory
from website.views import review_scenario

print('=== COMPREHENSIVE TEMPLATE CONTEXT TEST ===')

# Step 1: Test the data directly
print('\n1. TESTING DATA DIRECTLY:')
scenario = scenarios.objects.get(version='Jul 25 SPR')
inventory_snapshot = MasterDataInventory.objects.filter(version=scenario).first()
print(f'Scenario: {scenario.version}')
print(f'MasterDataInventory record: {inventory_snapshot}')
print(f'Date: {inventory_snapshot.date_of_snapshot if inventory_snapshot else "None"}')

# Step 2: Test what the view actually returns
print('\n2. TESTING VIEW FUNCTION DIRECTLY:')
factory = RequestFactory()
request = factory.get('/scenario/1/simple_inventory/')

# Mock the user (review_scenario might require login)
from django.contrib.auth.models import AnonymousUser
request.user = AnonymousUser()

try:
    response = review_scenario(request, '1')  # Using version ID 1
    print(f'View response status: {response.status_code}')
    
    # Extract context from response
    if hasattr(response, 'context_data'):
        context = response.context_data
        print(f'Context keys: {list(context.keys())}')
        
        if 'inventory_snapshot_date' in context:
            inv_date = context['inventory_snapshot_date']
            print(f'inventory_snapshot_date in context: {inv_date}')
            print(f'Type: {type(inv_date)}')
            print(f'Boolean: {bool(inv_date)}')
        else:
            print('❌ inventory_snapshot_date NOT in context')
    else:
        print('No context_data in response')
        
except Exception as e:
    print(f'❌ Error calling view: {e}')
    import traceback
    traceback.print_exc()

# Step 3: Test template rendering directly
print('\n3. TESTING TEMPLATE RENDERING:')
template_code = '''
{% if inventory_snapshot_date %}
TEMPLATE RESULT: As of: {{ inventory_snapshot_date|date:"d F Y" }}
{% else %}
TEMPLATE RESULT: No inventory snapshot date available
{% endif %}
DEBUG INFO:
- inventory_snapshot_date value: "{{ inventory_snapshot_date }}"
- inventory_snapshot_date exists: {{ inventory_snapshot_date|yesno:"YES,NO,UNKNOWN" }}
- inventory_snapshot_date type: {{ inventory_snapshot_date|default_if_none:"None" }}
'''

# Test with the actual data
template = Template(template_code)
context = Context({
    'inventory_snapshot_date': inventory_snapshot.date_of_snapshot if inventory_snapshot else None
})
result = template.render(context)
print('TEMPLATE TEST WITH REAL DATA:')
print(result)

# Test with None
print('\nTEMPLATE TEST WITH NONE:')
context_none = Context({'inventory_snapshot_date': None})
result_none = template.render(context_none)
print(result_none)

# Test with empty string
print('\nTEMPLATE TEST WITH EMPTY STRING:')
context_empty = Context({'inventory_snapshot_date': ''})
result_empty = template.render(context_empty)
print(result_empty)

print('\n=== CONCLUSION ===')
print('If the template is showing "No inventory snapshot date available",')
print('it means the Django view is passing None or empty value to the template.')
