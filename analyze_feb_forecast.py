#!/usr/bin/env python
import os
import sys
import django

# Add the project root to the Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

# Configure Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')
django.setup()

from website.models import SMART_Forecast_Model, scenarios

def analyze_feb_forecast():
    scenario = scenarios.objects.get(version='Jul 25 SPR Inv')

    # Check one specific date/location combination
    forecasts = SMART_Forecast_Model.objects.filter(
        version=scenario,
        Product='DEWB135-1',
        Period_AU='2026-02-01'
    ).values('Location', 'Forecast_Region', 'Customer_code', 'Qty')

    print('Feb 2026 DEWB135-1 Forecast Breakdown:')
    total_forecast = 0
    for f in forecasts:
        print(f'Location: {f["Location"]}, Region: {f["Forecast_Region"]}, Customer: {f["Customer_code"]}, Qty: {f["Qty"]}')
        total_forecast += f["Qty"] or 0

    print(f'Total Forecast for Feb 2026: {total_forecast}')
    print(f'Replenishment for Feb 2026: 595 (WOD1)')
    if total_forecast > 0:
        multiplier = 595/total_forecast
        print(f'Safety stock multiplier: {multiplier:.2f}x')
    else:
        print('Safety stock multiplier: N/A')

if __name__ == "__main__":
    analyze_feb_forecast()
