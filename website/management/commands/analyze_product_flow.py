from django.core.management.base import BaseCommand
from website.models import (
    SMART_Forecast_Model, CalcualtedReplenishmentModel, CalculatedProductionModel,
    MasterDataInventory, MasterDataOrderBook, MasterDataHistoryOfProductionModel,
    MasterDataEpicorSupplierMasterDataModel, MasterDataSafetyStocks,
    scenarios
)
import pandas as pd
from collections import defaultdict


class Command(BaseCommand):
    help = 'Analyze data flow for a specific product through forecast -> replenishment -> production models'

    def add_arguments(self, parser):
        parser.add_argument('version', type=str, help='Scenario version')
        parser.add_argument('product', type=str, help='Product code to analyze')

    def handle(self, *args, **options):
        version = options['version']
        product = options['product']
        
        try:
            scenario = scenarios.objects.get(version=version)
        except scenarios.DoesNotExist:
            self.stdout.write(self.style.ERROR(f"Scenario '{version}' not found"))
            return

        self.stdout.write(f"\n{'='*80}")
        self.stdout.write(f"ANALYZING PRODUCT: {product} IN SCENARIO: {version}")
        self.stdout.write(f"{'='*80}")

        # 1. Check SMART Forecast Data
        self.analyze_smart_forecast(scenario, product)
        
        # 2. Check Replenishment Data
        self.analyze_replenishment(scenario, product)
        
        # 3. Check Production Data
        self.analyze_production(scenario, product)
        
        # 4. Check Inventory Data
        self.analyze_inventory(scenario, product)
        
        # 5. Check Site Assignment Logic
        self.analyze_site_assignment(scenario, product)
        
        # 6. Check Safety Stock
        self.analyze_safety_stock(scenario, product)

    def analyze_smart_forecast(self, scenario, product):
        self.stdout.write(f"\n1. SMART FORECAST DATA")
        self.stdout.write("-" * 50)
        
        forecasts = SMART_Forecast_Model.objects.filter(
            version=scenario,
            Product=product
        ).values('Location', 'Period_AU', 'Forecast_Region', 'Customer_code', 'Data_Source', 'Qty')
        
        df = pd.DataFrame(list(forecasts))
        
        if df.empty:
            self.stdout.write("❌ No forecast records found")
            return
        
        total_qty = df['Qty'].sum()
        self.stdout.write(f"📊 Total forecast records: {len(df)}")
        self.stdout.write(f"📊 Total forecast quantity: {total_qty}")
        
        # Group by location
        location_summary = df.groupby('Location')['Qty'].agg(['count', 'sum']).reset_index()
        self.stdout.write(f"\n📍 Breakdown by Location:")
        for _, row in location_summary.iterrows():
            self.stdout.write(f"   {row['Location']}: {row['sum']} units ({row['count']} records)")
        
        # Group by data source
        source_summary = df.groupby('Data_Source')['Qty'].agg(['count', 'sum']).reset_index()
        self.stdout.write(f"\n📋 Breakdown by Data Source:")
        for _, row in source_summary.iterrows():
            self.stdout.write(f"   {row['Data_Source']}: {row['sum']} units ({row['count']} records)")
        
        # Show sample records
        self.stdout.write(f"\n📄 Sample records (first 5):")
        for _, row in df.head().iterrows():
            self.stdout.write(f"   {row['Location']} | {row['Period_AU']} | {row['Qty']} units | {row['Data_Source']}")

    def analyze_replenishment(self, scenario, product):
        self.stdout.write(f"\n2. REPLENISHMENT DATA")
        self.stdout.write("-" * 50)
        
        replenishments = CalcualtedReplenishmentModel.objects.filter(
            version=scenario.version,
            Product=product
        ).values('Product', 'Location', 'Site', 'ShippingDate', 'ReplenishmentQty')
        
        df = pd.DataFrame(list(replenishments))
        
        if df.empty:
            self.stdout.write("❌ No replenishment records found")
            return
        
        total_qty = df['ReplenishmentQty'].sum()
        self.stdout.write(f"📊 Total replenishment records: {len(df)}")
        self.stdout.write(f"📊 Total replenishment quantity: {total_qty}")
        
        # Group by location
        location_summary = df.groupby('Location')['ReplenishmentQty'].agg(['count', 'sum']).reset_index()
        self.stdout.write(f"\n📍 Breakdown by Location:")
        for _, row in location_summary.iterrows():
            self.stdout.write(f"   {row['Location']}: {row['sum']} units ({row['count']} records)")
        
        # Group by site
        site_summary = df.groupby('Site')['ReplenishmentQty'].agg(['count', 'sum']).reset_index()
        self.stdout.write(f"\n🏭 Breakdown by Production Site:")
        for _, row in site_summary.iterrows():
            self.stdout.write(f"   {row['Site']}: {row['sum']} units ({row['count']} records)")
        
        # Show monthly breakdown
        df['Month'] = pd.to_datetime(df['ShippingDate']).dt.to_period('M')
        monthly_summary = df.groupby('Month')['ReplenishmentQty'].sum().reset_index()
        self.stdout.write(f"\n📅 Monthly breakdown:")
        for _, row in monthly_summary.iterrows():
            self.stdout.write(f"   {row['Month']}: {row['ReplenishmentQty']} units")

    def analyze_production(self, scenario, product):
        self.stdout.write(f"\n3. PRODUCTION DATA")
        self.stdout.write("-" * 50)
        
        productions = CalculatedProductionModel.objects.filter(
            version=scenario,
            product_id=product
        ).values('site_id', 'pouring_date', 'production_quantity', 'cogs_aud')
        
        df = pd.DataFrame(list(productions))
        
        if df.empty:
            self.stdout.write("❌ No production records found")
            return
        
        total_qty = df['production_quantity'].sum()
        total_cost = df['cogs_aud'].sum()
        self.stdout.write(f"📊 Total production records: {len(df)}")
        self.stdout.write(f"📊 Total production quantity: {total_qty}")
        self.stdout.write(f"📊 Total production cost: ${total_cost:,.2f}")
        
        # Group by site
        site_summary = df.groupby('site_id').agg({
            'production_quantity': ['count', 'sum'],
            'cogs_aud': 'sum'
        }).reset_index()
        site_summary.columns = ['site_id', 'record_count', 'total_qty', 'total_cost']
        
        self.stdout.write(f"\n🏭 Breakdown by Production Site:")
        for _, row in site_summary.iterrows():
            self.stdout.write(f"   {row['site_id']}: {row['total_qty']} units (${row['total_cost']:,.2f}) - {row['record_count']} records")

    def analyze_inventory(self, scenario, product):
        self.stdout.write(f"\n4. INVENTORY DATA")
        self.stdout.write("-" * 50)
        
        inventory = MasterDataInventory.objects.filter(
            version=scenario,
            product=product
        ).values('site_id', 'onhandstock_qty', 'intransitstock_qty', 'wip_stock_qty', 'date_of_snapshot')
        
        df = pd.DataFrame(list(inventory))
        
        if df.empty:
            self.stdout.write("❌ No inventory records found")
            return
        
        # Aggregate by site
        site_summary = df.groupby('site_id').agg({
            'onhandstock_qty': 'sum',
            'intransitstock_qty': 'sum', 
            'wip_stock_qty': 'sum'
        }).reset_index()
        
        self.stdout.write(f"📦 Inventory by Location:")
        for _, row in site_summary.iterrows():
            total_stock = (row['onhandstock_qty'] or 0) + (row['intransitstock_qty'] or 0) + (row['wip_stock_qty'] or 0)
            self.stdout.write(f"   {row['site_id']}: {total_stock} units (On-hand: {row['onhandstock_qty'] or 0}, In-transit: {row['intransitstock_qty'] or 0}, WIP: {row['wip_stock_qty'] or 0})")

    def analyze_site_assignment(self, scenario, product):
        self.stdout.write(f"\n5. SITE ASSIGNMENT LOGIC")
        self.stdout.write("-" * 50)
        
        # Check order book assignment
        order_book = MasterDataOrderBook.objects.filter(
            version=scenario,
            productkey=product
        ).values('productkey', 'site')
        
        if order_book.exists():
            site = order_book.first()['site']
            self.stdout.write(f"📋 Order Book Assignment: {site}")
        else:
            self.stdout.write("📋 Order Book Assignment: None found")
        
        # Check production history assignment
        prod_history = MasterDataHistoryOfProductionModel.objects.filter(
            version=scenario,
            Product=product
        ).values('Product', 'Foundry').distinct()
        
        if prod_history.exists():
            foundries = [p['Foundry'] for p in prod_history]
            self.stdout.write(f"🏭 Production History Assignment: {', '.join(foundries)}")
        else:
            self.stdout.write("🏭 Production History Assignment: None found")
        
        # Check supplier assignment
        supplier = MasterDataEpicorSupplierMasterDataModel.objects.filter(
            version=scenario,
            PartNum=product
        ).values('PartNum', 'VendorID').distinct()
        
        if supplier.exists():
            vendors = [s['VendorID'] for s in supplier if s['VendorID'] is not None]
            if vendors:
                self.stdout.write(f"🏢 Supplier Assignment: {', '.join(vendors)}")
            else:
                self.stdout.write("🏢 Supplier Assignment: Found records but no valid VendorIDs")
        else:
            self.stdout.write("🏢 Supplier Assignment: None found")

    def analyze_safety_stock(self, scenario, product):
        self.stdout.write(f"\n6. SAFETY STOCK REQUIREMENTS")
        self.stdout.write("-" * 50)
        
        safety_stocks = MasterDataSafetyStocks.objects.filter(
            version=scenario,
            PartNum=product
        ).values('Plant', 'PartNum', 'MinimumQty', 'SafetyQty')
        
        df = pd.DataFrame(list(safety_stocks))
        
        if df.empty:
            self.stdout.write("📦 No safety stock requirements found")
            return
        
        self.stdout.write(f"📦 Safety Stock Requirements:")
        for _, row in df.iterrows():
            total_required = (row['MinimumQty'] or 0) + (row['SafetyQty'] or 0)
            self.stdout.write(f"   {row['Plant']}: {total_required} units (Min: {row['MinimumQty'] or 0}, Safety: {row['SafetyQty'] or 0})")
        
        total_safety_stock = df['MinimumQty'].fillna(0).sum() + df['SafetyQty'].fillna(0).sum()
        self.stdout.write(f"📦 Total Safety Stock Required: {total_safety_stock} units")
        
        self.stdout.write(f"\n{'='*80}")
        self.stdout.write("ANALYSIS COMPLETE")
        self.stdout.write(f"{'='*80}")
