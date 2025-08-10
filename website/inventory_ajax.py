from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_http_methods
from website.models import scenarios, InventoryProjectionModel
import json

@csrf_exempt
@require_http_methods(["POST"])
def get_inventory_ajax(request, version):
    try:
        scenario = get_object_or_404(scenarios, version=version)
        filter_group = request.POST.get('filter', 'All Product Groups')
        
        # Direct query
        if filter_group == 'All Product Groups':
            records = InventoryProjectionModel.objects.filter(version=scenario).order_by('month')
        else:
            records = InventoryProjectionModel.objects.filter(version=scenario, parent_product_group=filter_group).order_by('month')
        
        # Build data
        labels = []
        production = []
        opening_inventory = []
        table_data = []
        
        for r in records:
            month_label = r.month.strftime('%b %y')
            if month_label not in labels:
                labels.append(month_label)
                production.append(float(r.production_aud or 0))
                opening_inventory.append(float(r.opening_inventory_aud or 0))
            
            table_data.append({
                'month': month_label,
                'parent_product_group': r.parent_product_group,
                'production_aud': float(r.production_aud or 0),
                'opening_inventory_aud': float(r.opening_inventory_aud or 0),
                'closing_inventory_aud': float(r.closing_inventory_aud or 0)
            })
        
        return JsonResponse({
            'chart_data': {
                'labels': labels,
                'production': production,
                'opening_inventory': opening_inventory
            },
            'table_data': table_data
        })
        
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)
