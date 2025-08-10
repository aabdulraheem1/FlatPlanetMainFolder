import os
import django
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import MasterDataInventory, scenarios
from django.template import Template, Context

# Get the scenario and inventory data
scenario = scenarios.objects.get(version='Jul 25 SPR')

# Simulate the view logic exactly
snapshot_date = None
inventory_snapshot_date = None
try:
    inventory_snapshot = MasterDataInventory.objects.filter(version=scenario).first()
    if inventory_snapshot:
        snapshot_date = inventory_snapshot.date_of_snapshot.strftime('%B %d, %Y')
        inventory_snapshot_date = inventory_snapshot.date_of_snapshot
except:
    snapshot_date = "Date not available"
    inventory_snapshot_date = None

# Test what the template would render
template_code = """
{% if inventory_snapshot_date %}
    inventorySnapshotDate = '{{ inventory_snapshot_date|date:"Y-m-d" }}';
{% else %}
    inventorySnapshotDate = null;
{% endif %}

Raw inventory_snapshot_date: {{ inventory_snapshot_date }}
Formatted with date filter: {{ inventory_snapshot_date|date:"Y-m-d" }}
"""

template = Template(template_code)
context = Context({'inventory_snapshot_date': inventory_snapshot_date})
rendered = template.render(context)

print("Template rendering test:")
print("=" * 50)
print(rendered)
print("=" * 50)
print(f"inventory_snapshot_date value: {inventory_snapshot_date}")
print(f"inventory_snapshot_date type: {type(inventory_snapshot_date)}")
