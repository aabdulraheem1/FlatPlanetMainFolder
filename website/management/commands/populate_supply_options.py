from django.core.management.base import BaseCommand
from website.models import MasterDataSupplyOptionsModel, MasterDataProductModel, MasterDataPlantModel, MasterDataSuppliersModel
from sqlalchemy import create_engine, text

class Command(BaseCommand):
    help = "Fetch data from the server, calculate supply options, and populate the MasterDataSupplyOptionsModel."

    def handle(self, *args, **kwargs):
        # Connect to the database
        Server = 'bknew-sql02'
        Database = 'Bradken_Data_Warehouse'
        Driver = 'ODBC Driver 17 for SQL Server'
        Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
        engine = create_engine(Database_Con)

        # Delete all existing records in the table
        MasterDataSupplyOptionsModel.objects.all().delete()

        # Dictionary to store all new records
        supply_options_data = []

        # Use context manager for connection
        with engine.connect() as connection:
            # First Query: Fetch data from PowerBI.HeatProducts, PowerBI.Products, and PowerBI.Site
            query1 = text("""
                SELECT 
                    hp.CastQty,
                    hp.TapTime,
                    p.ProductKey AS ProductCode,
                    p.DressMass,
                    s.SiteName,
                    s.Location
                FROM PowerBI.HeatProducts hp
                INNER JOIN PowerBI.Products p ON hp.skProductId = p.skProductId
                INNER JOIN PowerBI.Site s ON hp.SkSiteId = s.skSiteId
            """)
            result1 = connection.execute(query1)

            for row in result1:
                if not row.ProductCode or not row.SiteName or not row.CastQty or not row.DressMass:
                    continue

                tonnes = row.CastQty * row.DressMass

                product_instance = MasterDataProductModel.objects.filter(Product=row.ProductCode).first()
                site_instance = MasterDataPlantModel.objects.filter(SiteName=row.SiteName).first()

                if product_instance and site_instance:
                    supply_options_data.append(MasterDataSupplyOptionsModel(
                        Product=product_instance,
                        Site=site_instance,
                        DateofSupply=row.TapTime,
                        InhouseOrOutsource="Inhouse",
                        Supplier=None,
                        SourceName=row.Location,
                        Qty=row.CastQty,
                        Tonnes=tonnes,
                    ))

            # Second Query: Fetch data from PowerBI.POReceipts, PowerBI.Products, PowerBI.Dates, and PowerBI.Supplier
            query2 = text("""
                SELECT 
                    pr.[Transaction Qty] AS TransactionQty,
                    d.DateValue,
                    p.ProductKey,
                    p.DressMass,
                    s.VendorID,
                    s.TradingName
                FROM [PowerBI].[PO Receipts] pr
                INNER JOIN PowerBI.Products p ON pr.skProductId = p.skProductId
                INNER JOIN PowerBI.Supplier s ON pr.skSupplierId = s.skSupplierId
                INNER JOIN PowerBI.Dates d ON pr.skReceiptDateId = d.skDateId  
            """)
            result2 = connection.execute(query2)

            for row in result2:
                if not row.ProductKey or not row.VendorID or not row.TransactionQty or not row.DressMass:
                    continue

                tonnes = row.TransactionQty * row.DressMass

                product_instance = MasterDataProductModel.objects.filter(Product=row.ProductKey).first()
                supplier_instance = MasterDataSuppliersModel.objects.filter(VendorID=row.VendorID).first()

                if product_instance and supplier_instance:
                    supply_options_data.append(MasterDataSupplyOptionsModel(
                        Product=product_instance,
                        Supplier=supplier_instance,
                        Site=None,
                        DateofSupply=row.DateValue,
                        InhouseOrOutsource="Outsource",
                        SourceName=row.TradingName,
                        Qty=row.TransactionQty,
                        Tonnes=tonnes,
                    ))

        # Bulk save all records to the database
        MasterDataSupplyOptionsModel.objects.bulk_create(supply_options_data)

        self.stdout.write(self.style.SUCCESS("Supply options data fetched and saved successfully."))