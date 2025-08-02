"""
Database Index Verification and Creation Script
Ensures all performance-critical indexes are created
"""

import time
from django.core.management.base import BaseCommand
from django.db import connection, transaction

class Command(BaseCommand):
    help = 'Verify and create performance indexes for calculate_model'

    def add_arguments(self, parser):
        parser.add_argument(
            '--create',
            action='store_true',
            help="Create missing indexes",
        )
        parser.add_argument(
            '--force',
            action='store_true',
            help="Drop and recreate all indexes",
        )

    def handle(self, *args, **options):
        create_indexes = options['create']
        force_recreate = options['force']
        
        print("="*60)
        print("🗄️  DATABASE INDEX VERIFICATION")
        print("="*60)
        
        # Define critical indexes for performance
        indexes = [
            {
                'name': 'idx_smart_forecast_version_data_source',
                'table': 'website_smart_forecast_model',
                'columns': '(version_id, Data_Source)',
                'reason': 'Filtering by version and data source in aggregated forecast'
            },
            {
                'name': 'idx_smart_forecast_version_product',
                'table': 'website_smart_forecast_model',
                'columns': '(version_id, Product)',
                'reason': 'Product lookup in replenishment calculation'
            },
            {
                'name': 'idx_inventory_version_product_site',
                'table': 'website_masterdatainventory',
                'columns': '(version_id, product, site_id)',
                'reason': 'Inventory lookup by product and site'
            },
            {
                'name': 'idx_replenishment_version_product',
                'table': 'website_calcualtedreplenishmentmodel',
                'columns': '(version_id, Product_id)',
                'reason': 'Replenishment data filtering in production calculation'
            },
            {
                'name': 'idx_replenishment_version_site',
                'table': 'website_calcualtedreplenishmentmodel',
                'columns': '(version_id, Site_id)',
                'reason': 'Site-based replenishment filtering'
            },
            {
                'name': 'idx_production_version_product',
                'table': 'website_calculatedproductionmodel',
                'columns': '(version_id, product_id)',
                'reason': 'Production data aggregation and reporting'
            },
            {
                'name': 'idx_product_cost_version_product',
                'table': 'website_productsitecostmodel',
                'columns': '(version_id, product_id)',
                'reason': 'Cost lookup optimization'
            },
            {
                'name': 'idx_cast_despatch_version',
                'table': 'website_masterdatacasttodespatchmodel',
                'columns': '(version_id)',
                'reason': 'Cast to despatch days lookup'
            }
        ]
        
        existing_indexes = self.get_existing_indexes()
        
        print(f"📊 Checking {len(indexes)} critical indexes...")
        print()
        
        missing_indexes = []
        existing_count = 0
        
        for idx in indexes:
            if idx['name'] in existing_indexes:
                print(f"✅ {idx['name']}")
                print(f"   📋 {idx['table']}{idx['columns']}")
                existing_count += 1
            else:
                print(f"❌ {idx['name']} - MISSING")
                print(f"   📋 {idx['table']}{idx['columns']}")
                print(f"   💡 {idx['reason']}")
                missing_indexes.append(idx)
            print()
        
        print("-" * 60)
        print(f"📊 Summary: {existing_count}/{len(indexes)} indexes exist")
        
        if missing_indexes:
            print(f"⚠️  {len(missing_indexes)} indexes are missing")
            
            if create_indexes or force_recreate:
                print("\n🔧 Creating missing indexes...")
                self.create_indexes(missing_indexes, force_recreate)
            else:
                print("\n💡 To create missing indexes, run:")
                print("   python manage.py verify_indexes --create")
                
                print("\n📄 SQL to create missing indexes:")
                for idx in missing_indexes:
                    print(f"CREATE INDEX {idx['name']} ON {idx['table']} {idx['columns']};")
        else:
            print("🎉 All critical indexes exist!")
            
        # Performance recommendations
        print("\n💡 ADDITIONAL PERFORMANCE TIPS:")
        print("-" * 40)
        print("1. Monitor query execution plans")
        print("2. Consider partitioning large tables by version")
        print("3. Regularly update table statistics")
        print("4. Monitor index usage and fragmentation")

    def get_existing_indexes(self):
        """Get list of existing indexes from database"""
        with connection.cursor() as cursor:
            # This query works for SQL Server
            cursor.execute("""
                SELECT DISTINCT i.name
                FROM sys.indexes i
                INNER JOIN sys.objects o ON i.object_id = o.object_id
                WHERE o.type = 'U' 
                AND i.type > 0
                AND i.name IS NOT NULL
            """)
            return set(row[0] for row in cursor.fetchall())

    def create_indexes(self, indexes, force_recreate=False):
        """Create the specified indexes"""
        
        with transaction.atomic():
            for idx in indexes:
                try:
                    with connection.cursor() as cursor:
                        if force_recreate:
                            # Drop index if it exists
                            try:
                                cursor.execute(f"DROP INDEX {idx['name']} ON {idx['table']}")
                                print(f"🗑️  Dropped existing index: {idx['name']}")
                            except:
                                pass  # Index might not exist
                        
                        # Create index
                        sql = f"CREATE INDEX {idx['name']} ON {idx['table']} {idx['columns']}"
                        cursor.execute(sql)
                        print(f"✅ Created index: {idx['name']}")
                        
                except Exception as e:
                    print(f"❌ Failed to create {idx['name']}: {str(e)}")
        
        print(f"\n🎉 Index creation completed!")

    def analyze_query_performance(self):
        """Analyze current query performance"""
        print("\n📊 QUERY PERFORMANCE ANALYSIS:")
        print("-" * 40)
        
        # Sample queries to test
        test_queries = [
            {
                'name': 'SMART Forecast by Version',
                'sql': "SELECT COUNT(*) FROM website_smart_forecast_model WHERE version_id = %s",
                'params': [1]
            },
            {
                'name': 'Inventory by Version and Product',
                'sql': "SELECT COUNT(*) FROM website_masterdatainventory WHERE version_id = %s AND product LIKE %s",
                'params': [1, 'BK%']
            }
        ]
        
        for query in test_queries:
            start_time = time.time()
            try:
                with connection.cursor() as cursor:
                    cursor.execute(query['sql'], query['params'])
                    result = cursor.fetchone()
                duration = time.time() - start_time
                print(f"   {query['name']}: {duration*1000:.1f}ms")
            except Exception as e:
                print(f"   {query['name']}: ERROR - {str(e)}")
