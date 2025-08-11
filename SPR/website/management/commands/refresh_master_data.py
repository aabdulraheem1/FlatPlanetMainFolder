"""
Example Data Refresh Management Command
Demonstrates how to refresh data from Epicor while preserving user modifications
"""
from django.core.management.base import BaseCommand
from django.utils import timezone
from website.models import MasterDataPlantModel, MasterDataProductModel, MasterDataSuppliersModel, MasterDataCustomersModel
from website.data_protection_utils import (
    safe_update_from_epicor, 
    get_user_created_records, 
    get_epicor_managed_records,
    create_data_refresh_summary
)


class Command(BaseCommand):
    help = 'Refresh master data from Epicor while preserving user modifications'
    
    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be updated without making changes',
        )
        parser.add_argument(
            '--model',
            type=str,
            choices=['plants', 'products', 'suppliers', 'customers', 'all'],
            default='all',
            help='Which model to refresh (default: all)',
        )
    
    def handle(self, *args, **options):
        dry_run = options['dry_run']
        model_choice = options['model']
        
        self.stdout.write(self.style.SUCCESS(f"Starting data refresh (dry_run={dry_run})..."))
        
        summary = {}
        
        if model_choice in ['plants', 'all']:
            summary['Plants'] = self.refresh_plants(dry_run)
            
        if model_choice in ['products', 'all']:
            summary['Products'] = self.refresh_products(dry_run)
            
        if model_choice in ['suppliers', 'all']:
            summary['Suppliers'] = self.refresh_suppliers(dry_run)
            
        if model_choice in ['customers', 'all']:
            summary['Customers'] = self.refresh_customers(dry_run)
        
        # Print summary
        summary_report = create_data_refresh_summary(summary)
        self.stdout.write(summary_report)
        
        if dry_run:
            self.stdout.write(self.style.WARNING("DRY RUN - No changes were made"))
        else:
            self.stdout.write(self.style.SUCCESS("Data refresh completed!"))
    
    def refresh_plants(self, dry_run=False):
        """Refresh plant data from Epicor"""
        self.stdout.write("Refreshing Plants data...")
        
        # Simulate Epicor data (replace with actual Epicor connection)
        epicor_plants = [
            {
                'SiteName': 'MTJ1',
                'Company': 'BRADKEN',
                'Country': 'Australia',
                'Location': 'Newcastle',
                'PlantRegion': 'Asia Pacific',
                'SiteType': 'Foundry'
            },
            {
                'SiteName': 'COI2', 
                'Company': 'BRADKEN',
                'Country': 'Brazil',
                'Location': 'Sao Paulo',
                'PlantRegion': 'South America',
                'SiteType': 'Foundry'
            }
        ]
        
        updated = []
        protected = []
        created = []
        deleted = []
        
        # Update existing plants
        for epicor_plant in epicor_plants:
            site_name = epicor_plant['SiteName']
            
            try:
                plant = MasterDataPlantModel.objects.get(SiteName=site_name)
                
                if not dry_run:
                    update_result = safe_update_from_epicor(plant, epicor_plant)
                    plant.save()
                    
                    if update_result['protected_fields']:
                        protected.append(f"{site_name} (protected: {', '.join(update_result['protected_fields'])})")
                    else:
                        updated.append(site_name)
                else:
                    # Dry run - just check what would be protected
                    from website.data_protection_utils import is_field_protected
                    protected_fields = [field for field in epicor_plant.keys() if is_field_protected(plant, field)]
                    if protected_fields:
                        protected.append(f"{site_name} (would protect: {', '.join(protected_fields)})")
                    else:
                        updated.append(f"{site_name} (would update)")
                        
            except MasterDataPlantModel.DoesNotExist:
                # Create new plant
                if not dry_run:
                    plant = MasterDataPlantModel.objects.create(**epicor_plant)
                    plant.last_imported_from_epicor = timezone.now()
                    plant.save()
                created.append(site_name)
        
        # Handle deletions (only delete Epicor-managed records)
        epicor_site_names = [p['SiteName'] for p in epicor_plants]
        to_delete = get_epicor_managed_records(MasterDataPlantModel).exclude(SiteName__in=epicor_site_names)
        
        for plant in to_delete:
            deleted.append(plant.SiteName)
            if not dry_run:
                plant.delete()
        
        return {
            'updated': updated,
            'protected': protected,
            'created': created,
            'deleted': deleted
        }
    
    def refresh_products(self, dry_run=False):
        """Refresh product data from Epicor"""
        self.stdout.write("Refreshing Products data...")
        
        # Simulate some product updates
        updated = []
        protected = []
        created = []
        deleted = []
        
        # This is where you'd connect to Epicor and get product data
        # For now, just return empty results
        return {
            'updated': updated,
            'protected': protected,
            'created': created,
            'deleted': deleted
        }
    
    def refresh_suppliers(self, dry_run=False):
        """Refresh supplier data from Epicor"""
        self.stdout.write("Refreshing Suppliers data...")
        
        updated = []
        protected = []
        created = []
        deleted = []
        
        # This is where you'd connect to Epicor and get supplier data
        return {
            'updated': updated,
            'protected': protected,
            'created': created,
            'deleted': deleted
        }
    
    def refresh_customers(self, dry_run=False):
        """Refresh customer data from Epicor"""
        self.stdout.write("Refreshing Customers data...")
        
        updated = []
        protected = []
        created = []
        deleted = []
        
        # This is where you'd connect to Epicor and get customer data
        return {
            'updated': updated,
            'protected': protected,
            'created': created,
            'deleted': deleted
        }
