#!/usr/bin/env python
"""
COMPREHENSIVE TEMPLATE CONTEXT TEST
This will test exactly what gets passed to the template and why it's showing the wrong value
"""

import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from django.test import RequestFactory
from django.contrib.auth.models import User
from website.views import review_scenario
from website.models import scenarios, MasterDataInventory

print("🔥🔥🔥 COMPREHENSIVE TEMPLATE CONTEXT TEST 🔥🔥🔥")
print("=" * 80)

# 1. Test the database directly
print("\n1. TESTING DATABASE DIRECTLY:")
try:
    scenario = scenarios.objects.get(version='Jul 25 SPR')
    print(f"   ✅ Scenario: {scenario.version}")
    
    inventory_snapshot = MasterDataInventory.objects.filter(version=scenario).first()
    if inventory_snapshot:
        print(f"   ✅ MasterDataInventory found: {inventory_snapshot.date_of_snapshot}")
    else:
        print(f"   ❌ No MasterDataInventory found")
except Exception as e:
    print(f"   ❌ Database error: {e}")

# 2. Test the Django view directly
print("\n2. TESTING DJANGO VIEW DIRECTLY:")
try:
    # Create a mock request
    factory = RequestFactory()
    request = factory.get('/scenario/1/simple_inventory/')
    
    # Create or get a user
    user, created = User.objects.get_or_create(username='testuser')
    request.user = user
    
    print("   🔥 Calling review_scenario view directly...")
    
    # Call the view function directly
    response = review_scenario(request, '1')
    
    print(f"   ✅ View executed successfully")
    print(f"   ✅ Response status: {response.status_code}")
    print(f"   ✅ Response type: {type(response)}")
    
except Exception as e:
    print(f"   ❌ View error: {e}")
    import traceback
    traceback.print_exc()

# 3. Test URL routing
print("\n3. TESTING URL ROUTING:")
from django.urls import reverse, resolve

try:
    # Test URL reverse
    url = reverse('review_scenario', args=['1'])
    print(f"   ✅ Reverse URL: {url}")
    
    # Test URL resolve
    resolver = resolve('/scenario/review/1')
    print(f"   ✅ URL resolves to: {resolver.func.__name__}")
    print(f"   ✅ View function: {resolver.func}")
    
except Exception as e:
    print(f"   ❌ URL error: {e}")

# 4. Test template context creation manually
print("\n4. TESTING TEMPLATE CONTEXT MANUALLY:")
try:
    scenario = scenarios.objects.get(version='Jul 25 SPR')
    inventory_snapshot = MasterDataInventory.objects.filter(version=scenario).first()
    inventory_snapshot_date = inventory_snapshot.date_of_snapshot if inventory_snapshot else None
    
    # Simulate template context
    context = {
        'inventory_snapshot_date': inventory_snapshot_date,
        'scenario': scenario,
    }
    
    print(f"   ✅ Context inventory_snapshot_date: {context['inventory_snapshot_date']}")
    print(f"   ✅ Type: {type(context['inventory_snapshot_date'])}")
    print(f"   ✅ Boolean: {bool(context['inventory_snapshot_date'])}")
    
    # Test Django template rendering
    from django.template import Template, Context
    template_code = """
    {% if inventory_snapshot_date %}
        Date exists: {{ inventory_snapshot_date|date:"d F Y" }}
    {% else %}
        No date available
    {% endif %}
    """
    
    template = Template(template_code)
    django_context = Context(context)
    result = template.render(django_context)
    
    print(f"   ✅ Template renders: {result.strip()}")
    
except Exception as e:
    print(f"   ❌ Context error: {e}")

print("\n" + "=" * 80)
print("🔥 TEST COMPLETE - CHECK RESULTS ABOVE 🔥")
