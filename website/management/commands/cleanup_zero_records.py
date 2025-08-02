"""
Zero Records Cleanup Management Command
Removes existing zero quantity records from SMART_Forecast_Model
"""

from django.core.management.base import BaseCommand
from django.db import connection, transaction
from website.models import SMART_Forecast_Model

class Command(BaseCommand):
    help = 'Clean up zero quantity records from SMART_Forecast_Model for better performance'

    def add_arguments(self, parser):
        parser.add_argument(
            '--scenario',
            type=str,
            help="Specific scenario version to clean (optional)",
        )
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help="Show what would be deleted without actually deleting",
        )
        parser.add_argument(
            '--confirm',
            action='store_true',
            help="Confirm deletion of zero records",
        )

    def handle(self, *args, **options):
        scenario = options.get('scenario')
        dry_run = options['dry_run']
        confirm = options['confirm']
        
        print("="*70)
        print("🧹 ZERO RECORDS CLEANUP")
        print("="*70)
        
        if scenario:
            print(f"📊 Scenario: {scenario}")
        else:
            print("📊 Scope: ALL scenarios")
        
        print(f"🔍 Mode: {'DRY RUN' if dry_run else 'LIVE CLEANUP'}")
        print("="*70)
        
        # Build query filter
        if scenario:
            queryset = SMART_Forecast_Model.objects.filter(
                version__version=scenario,
                Qty__lte=0
            ) | SMART_Forecast_Model.objects.filter(
                version__version=scenario,
                Qty__isnull=True
            )
        else:
            queryset = SMART_Forecast_Model.objects.filter(
                Qty__lte=0
            ) | SMART_Forecast_Model.objects.filter(
                Qty__isnull=True
            )
        
        # Get counts by data source
        print("📊 Zero Records Analysis:")
        print("-" * 40)
        
        total_zero_records = queryset.count()
        
        if total_zero_records == 0:
            print("✅ No zero quantity records found!")
            return
        
        # Count by data source
        data_source_counts = {}
        for record in queryset.values('Data_Source').distinct():
            data_source = record['Data_Source']
            count = queryset.filter(Data_Source=data_source).count()
            data_source_counts[data_source] = count
            print(f"   {data_source}: {count:,} zero records")
        
        print(f"\n🎯 Total zero records to clean: {total_zero_records:,}")
        
        if dry_run:
            print("\n💡 DRY RUN MODE - No records will be deleted")
            print("   Run with --confirm to actually delete the records")
            
            # Show sample zero records
            print("\n📋 Sample zero records:")
            sample_records = queryset.select_related('version')[:10]
            for record in sample_records:
                print(f"   ID: {record.id}, Version: {record.version.version}, "
                      f"Product: {record.Product}, Qty: {record.Qty}, "
                      f"Data Source: {record.Data_Source}")
            
            return
        
        if not confirm:
            print("\n⚠️  Use --confirm flag to proceed with deletion")
            print("   or --dry-run to see what would be deleted")
            return
        
        # Proceed with deletion
        print(f"\n🗑️  Deleting {total_zero_records:,} zero quantity records...")
        
        try:
            with transaction.atomic():
                deleted_count = queryset.delete()[0]
                print(f"✅ Successfully deleted {deleted_count:,} zero quantity records")
                
                # Show space savings estimate
                avg_record_size = 500  # bytes per record estimate
                space_saved_mb = (deleted_count * avg_record_size) / (1024 * 1024)
                print(f"💾 Estimated space saved: {space_saved_mb:.1f} MB")
                
                print(f"\n🚀 Performance Impact:")
                print(f"   ⚡ Query performance should improve")
                print(f"   🧠 Memory usage reduced during data processing")
                print(f"   📊 Polars conversions will be faster")
                
        except Exception as e:
            print(f"❌ Error during cleanup: {str(e)}")
            raise
        
        print(f"\n✅ Zero records cleanup completed!")
        print(f"💡 Future uploads will automatically filter zeros")

    def get_zero_record_statistics(self, scenario=None):
        """Get detailed statistics about zero records"""
        
        base_query = SMART_Forecast_Model.objects.all()
        if scenario:
            base_query = base_query.filter(version__version=scenario)
        
        total_records = base_query.count()
        zero_records = base_query.filter(Qty__lte=0).count() + base_query.filter(Qty__isnull=True).count()
        
        return {
            'total_records': total_records,
            'zero_records': zero_records,
            'zero_percentage': (zero_records / total_records * 100) if total_records > 0 else 0,
            'non_zero_records': total_records - zero_records
        }
