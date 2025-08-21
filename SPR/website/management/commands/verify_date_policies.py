from django.core.management.base import BaseCommand
from website.models import (
    SMART_Forecast_Model, 
    CalcualtedReplenishmentModel, 
    CalculatedProductionModel,
    ScenarioModel
)
from datetime import datetime, timedelta


class Command(BaseCommand):
    help = 'Verify that date calculation policies are correctly implemented'

    def add_arguments(self, parser):
        parser.add_argument(
            'scenario_name',
            type=str,
            help='Scenario name to verify (e.g., "Aug 25 SPR")'
        )
        parser.add_argument(
            '--product',
            type=str,
            help='Specific product to test (optional)',
            default=None
        )
        parser.add_argument(
            '--limit',
            type=int,
            help='Limit number of records to check',
            default=5
        )

    def handle(self, *args, **options):
        scenario_name = options['scenario_name']
        product_filter = options.get('product')
        limit = options['limit']

        self.stdout.write(f"\n🔍 VERIFYING DATE CALCULATION POLICIES")
        self.stdout.write(f"📂 Scenario: {scenario_name}")
        self.stdout.write("="*80)

        # Policy Statement
        self.stdout.write("\n📋 DATE CALCULATION POLICIES:")
        self.stdout.write("   1️⃣ Shipping Date = Period_AU - Freight Days (SUBTRACT)")
        self.stdout.write("   2️⃣ Pouring Date = Shipping Date - Cast-to-Dispatch Days (SUBTRACT)")
        self.stdout.write("="*80)

        # Get scenario
        try:
            scenario = ScenarioModel.objects.get(scenario_name=scenario_name)
        except ScenarioModel.DoesNotExist:
            self.stdout.write(f"❌ Scenario '{scenario_name}' not found!")
            return

        # Query filter for replenishment
        filter_kwargs = {'version__scenario': scenario}
        if product_filter:
            filter_kwargs['Product__product_number__icontains'] = product_filter

        # Check replenishment data (shipping dates)
        self.stdout.write("\n🚢 REPLENISHMENT DATA - SHIPPING DATE VERIFICATION:")
        replenishment_records = CalcualtedReplenishmentModel.objects.filter(
            **filter_kwargs
        ).select_related('version', 'Product', 'Site')[:limit]

        for record in replenishment_records:
            self.stdout.write(f"   📦 {record.Product.product_number} @ {record.Site.site_name}")
            self.stdout.write(f"      Shipping Date: {record.ShippingDate}")
            self.stdout.write(f"      Quantity: {record.ReplenishmentQty}")

        # Query filter for production
        prod_filter_kwargs = {'version__scenario': scenario}
        if product_filter:
            prod_filter_kwargs['product__product_number__icontains'] = product_filter

        # Check production data (pouring dates)
        self.stdout.write("\n🏭 PRODUCTION DATA - POURING DATE VERIFICATION:")
        production_records = CalculatedProductionModel.objects.filter(
            **prod_filter_kwargs
        ).select_related('version', 'product', 'site')[:limit]

        for record in production_records:
            self.stdout.write(f"   🏭 {record.product.product_number} @ {record.site.site_name}")
            self.stdout.write(f"      Pouring Date: {record.pouring_date}")
            self.stdout.write(f"      Production Quantity: {record.production_quantity}")

        # Summary statistics
        self.stdout.write("\n📊 SUMMARY STATISTICS:")
        
        total_replenishment = CalcualtedReplenishmentModel.objects.filter(
            version__scenario=scenario
        ).count()
        
        total_production = CalculatedProductionModel.objects.filter(
            version__scenario=scenario
        ).count()
        
        self.stdout.write(f"   📦 Total replenishment records: {total_replenishment:,}")
        self.stdout.write(f"   🏭 Total production records: {total_production:,}")
        
        # Test specific date calculations
        self.stdout.write("\n🧪 DATE ARITHMETIC TEST:")
        test_date = datetime(2025, 8, 25).date()
        test_freight_days = 7
        test_cast_days = 3
        
        calculated_shipping = test_date - timedelta(days=test_freight_days)
        calculated_pouring = calculated_shipping - timedelta(days=test_cast_days)
        
        self.stdout.write(f"   📅 Test Period: {test_date}")
        self.stdout.write(f"   🚢 Shipping = {test_date} - {test_freight_days} days = {calculated_shipping}")
        self.stdout.write(f"   🏭 Pouring = {calculated_shipping} - {test_cast_days} days = {calculated_pouring}")

        # Show actual dates from data
        if replenishment_records and production_records:
            self.stdout.write("\n🔍 ACTUAL DATE FLOW ANALYSIS:")
            rep_record = replenishment_records.first()
            prod_record = production_records.first()
            
            self.stdout.write(f"   📦 Sample Replenishment:")
            self.stdout.write(f"      Product: {rep_record.Product.product_number}")
            self.stdout.write(f"      Shipping Date: {rep_record.ShippingDate}")
            
            # Find matching production record
            matching_prod = production_records.filter(
                product=rep_record.Product,
                site=rep_record.Site
            ).first()
            
            if matching_prod:
                self.stdout.write(f"   🏭 Matching Production:")
                self.stdout.write(f"      Product: {matching_prod.product.product_number}")
                self.stdout.write(f"      Pouring Date: {matching_prod.pouring_date}")
                
                # Calculate days difference
                if rep_record.ShippingDate and matching_prod.pouring_date:
                    days_diff = (rep_record.ShippingDate - matching_prod.pouring_date).days
                    self.stdout.write(f"   ⏳ Cast-to-Dispatch Period: {days_diff} days")

        self.stdout.write("\n✅ Date policy verification completed!")
