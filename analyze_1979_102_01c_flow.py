#!/usr/bin/env python3
"""
Analyze the complete flow for product '1979-102-01C' in scenario 'Aug 25 SPR'
From SMART_Forecast -> Replenishment -> Production
"""

import os
import sys
import django

# Setup Django environment
sys.path.append('C:\\Users\\aali\\OneDrive - bradken.com\\Data\\Training\\SPR\\SPR')
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'settings')
django.setup()

from website.models import (
    scenarios,
    SMART_Forecast_Model,
    CalcualtedReplenishmentModel,
    CalculatedProductionModel,
    MasterDataProductModel,
    MasterDataInventory,
    MasterDataSafetyStocks
)

def analyze_product_flow():
    product_code = '1979-102-01C'
    scenario_version = 'Aug 25 SPR'
    
    print(f"🔍 Analyzing flow for product: {product_code}")
    print(f"📅 Scenario: {scenario_version}")
    print("=" * 80)
    
    try:
        scenario = scenarios.objects.get(version=scenario_version)
        print(f"✅ Found scenario: {scenario.version}")
    except scenarios.DoesNotExist:
        print(f"❌ Scenario '{scenario_version}' not found")
        return
    
    # Check if product exists in master data
    try:
        product = MasterDataProductModel.objects.get(Product=product_code)
        print(f"✅ Product found: {product.Product}")
        print(f"   Dress Mass: {product.DressMass}")
        print(f"   Product Group: {product.ProductGroup}")
        print(f"   Parent Product Group: {product.ParentProductGroup}")
    except MasterDataProductModel.DoesNotExist:
        print(f"❌ Product '{product_code}' not found in master data")
        return
    
    print("\n" + "=" * 80)
    print("1️⃣  SMART FORECAST ANALYSIS")
    print("=" * 80)
    
    # Check SMART Forecast data
    forecast_records = SMART_Forecast_Model.objects.filter(
        version=scenario,
        Product=product_code
    )
    
    print(f"📊 Found {forecast_records.count()} forecast records")
    
    total_forecast_qty = 0
    total_forecast_tonnes = 0
    
    for record in forecast_records:
        print(f"   📍 Location: {record.Location}")
        print(f"   📅 Period: {record.Period_AU}")
        print(f"   📦 Qty: {record.Qty}")
        print(f"   ⚖️  Tonnes: {record.Tonnes}")
        print(f"   🏭 Forecast Region: {record.Forecast_Region}")
        print(f"   👤 Customer: {record.Customer_code}")
        print(f"   ---")
        
        total_forecast_qty += record.Qty or 0
        total_forecast_tonnes += record.Tonnes or 0
    
    print(f"📈 TOTAL FORECAST: {total_forecast_qty} qty, {total_forecast_tonnes} tonnes")
    
    print("\n" + "=" * 80)
    print("2️⃣  SAFETY STOCK ANALYSIS")
    print("=" * 80)
    
    # Check safety stock data
    safety_stocks = MasterDataSafetyStocks.objects.filter(
        version=scenario,
        PartNum=product_code
    )
    
    print(f"🛡️  Found {safety_stocks.count()} safety stock records")
    
    for ss in safety_stocks:
        combined_safety = (ss.SafetyQty or 0) + (ss.MinimumQty or 0)
        print(f"   🏭 Plant: {ss.Plant}")
        print(f"   🛡️  Safety Qty: {ss.SafetyQty}")
        print(f"   📦 Minimum Qty: {ss.MinimumQty}")
        print(f"   🔢 Combined Safety Stock: {combined_safety}")
        print(f"   ---")
    
    print("\n" + "=" * 80)
    print("3️⃣  INVENTORY ANALYSIS")
    print("=" * 80)
    
    # Check inventory data
    inventory_records = MasterDataInventory.objects.filter(
        version=scenario,
        product=product_code
    )
    
    print(f"📦 Found {inventory_records.count()} inventory records")
    
    total_onhand = 0
    total_intransit = 0
    total_wip = 0
    
    for inv in inventory_records:
        print(f"   🏭 Site: {inv.site.SiteName}")
        print(f"   📦 On Hand: {inv.onhandstock_qty}")
        print(f"   🚚 In Transit: {inv.intransitstock_qty}")
        print(f"   🏗️  WIP: {inv.wip_stock_qty}")
        
        total_onhand += inv.onhandstock_qty or 0
        total_intransit += inv.intransitstock_qty or 0
        total_wip += inv.wip_stock_qty or 0
        print(f"   ---")
    
    total_inventory = total_onhand + total_intransit + total_wip
    print(f"📊 TOTAL INVENTORY: {total_inventory} (OnHand: {total_onhand}, InTransit: {total_intransit}, WIP: {total_wip})")
    
    print("\n" + "=" * 80)
    print("4️⃣  REPLENISHMENT ANALYSIS")
    print("=" * 80)
    
    # Check replenishment records
    replenishment_records = CalcualtedReplenishmentModel.objects.filter(
        version=scenario,
        Product=product
    )
    
    print(f"🔄 Found {replenishment_records.count()} replenishment records")
    
    total_replenishment_qty = 0
    
    for rep in replenishment_records:
        print(f"   📍 Location: {rep.Location}")
        print(f"   🏭 Site: {rep.Site.SiteName if rep.Site else 'No Site'}")
        print(f"   📅 Shipping Date: {rep.ShippingDate}")
        print(f"   📦 Replenishment Qty: {rep.ReplenishmentQty}")
        print(f"   👤 Latest Customer: {rep.latest_customer_invoice}")
        print(f"   ---")
        
        total_replenishment_qty += rep.ReplenishmentQty or 0
    
    print(f"🔄 TOTAL REPLENISHMENT: {total_replenishment_qty} qty")
    
    print("\n" + "=" * 80)
    print("5️⃣  PRODUCTION ANALYSIS")
    print("=" * 80)
    
    # Check production records
    production_records = CalculatedProductionModel.objects.filter(
        version=scenario,
        product=product
    )
    
    print(f"🏭 Found {production_records.count()} production records")
    
    total_production_qty = 0
    total_production_tonnes = 0
    
    for prod in production_records:
        print(f"   🏭 Site: {prod.site.SiteName if prod.site else 'No Site'}")
        print(f"   📅 Pouring Date: {prod.pouring_date}")
        print(f"   📦 Production Qty: {prod.production_quantity}")
        print(f"   ⚖️  Tonnes: {prod.tonnes}")
        print(f"   🏷️  Product Group: {prod.product_group}")
        print(f"   🔧 Is Outsourced: {prod.is_outsourced}")
        print(f"   👤 Latest Customer: {prod.latest_customer_invoice}")
        print(f"   ---")
        
        total_production_qty += prod.production_quantity or 0
        total_production_tonnes += prod.tonnes or 0
    
    print(f"🏭 TOTAL PRODUCTION: {total_production_qty} qty, {total_production_tonnes} tonnes")
    
    print("\n" + "=" * 80)
    print("📊 FLOW SUMMARY")
    print("=" * 80)
    
    print(f"1️⃣  SMART Forecast: {total_forecast_qty} qty → {total_forecast_tonnes} tonnes")
    print(f"2️⃣  Total Inventory: {total_inventory} qty")
    print(f"3️⃣  Net Requirement (Forecast - Inventory): {total_forecast_qty - total_inventory} qty")
    print(f"4️⃣  Replenishment: {total_replenishment_qty} qty")
    print(f"5️⃣  Production: {total_production_qty} qty → {total_production_tonnes} tonnes")
    
    print(f"\n🔢 CALCULATION VALIDATION:")
    print(f"   Forecast to Replenishment ratio: {(total_replenishment_qty / total_forecast_qty * 100) if total_forecast_qty > 0 else 0:.1f}%")
    print(f"   Replenishment to Production ratio: {(total_production_qty / total_replenishment_qty * 100) if total_replenishment_qty > 0 else 0:.1f}%")
    print(f"   Forecast to Production ratio: {(total_production_qty / total_forecast_qty * 100) if total_forecast_qty > 0 else 0:.1f}%")

if __name__ == '__main__':
    analyze_product_flow()
