"""
AJAX views for performance optimization
"""
from django.http import JsonResponse
from django.shortcuts import get_object_or_404
from django.contrib.auth.decorators import login_required
from .models import scenarios
from .customized_function import build_detailed_monthly_table
import json

@login_required
def get_detailed_monthly_table(request, version):
    """AJAX endpoint to get detailed monthly table for a specific FY and site"""
    if request.method != 'GET':
        return JsonResponse({'error': 'Only GET requests allowed'}, status=405)
    
    fy = request.GET.get('fy')
    site = request.GET.get('site')
    plan_type = request.GET.get('plan_type', 'demand')  # Default to 'demand' if not specified
    
    if not fy or not site:
        return JsonResponse({'error': 'FY and site parameters required'}, status=400)
    
    try:
        scenario = get_object_or_404(scenarios, version=version)
        table_html = build_detailed_monthly_table(fy, site, scenario, plan_type)
        
        return JsonResponse({
            'success': True,
            'html': str(table_html),
            'fy': fy,
            'site': site,
            'plan_type': plan_type
        })
    except Exception as e:
        return JsonResponse({
            'success': False,
            'error': str(e)
        }, status=500)
