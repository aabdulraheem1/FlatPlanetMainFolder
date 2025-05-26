from django.db import models

# Create your models here.


class test(models.Model):
    test = models.CharField(max_length=100)

    def __str__(self):
        return self.test

class MasterDataPlantModel(models.Model):
    InhouseOrOutsource = models.CharField(max_length=250, default='Inhouse', null=True, blank=True)  # Default value set to 'Inhouse'
    SiteName = models.CharField(primary_key=True, max_length=250)
    TradingName = models.CharField(max_length=250, null=True, blank=True)
    Company = models.CharField(max_length=250,null=True, blank=True)
    Country = models.CharField(max_length=250,null=True, blank=True)
    Location = models.CharField(max_length=250,null=True, blank=True)
    PlantRegion = models.CharField(max_length=250,null=True, blank=True)
    SiteType = models.CharField(max_length=250,null=True, blank=True)
    About = models.TextField(null=True,max_length=3000, blank=True)

    def __str__(self):
        return self.SiteName or "Unknown Site"

class scenarios(models.Model):
    version = models.CharField(max_length=100, primary_key=True, null=False)
    scenario_description = models.TextField(default="type your description of this version here", null=False)
    created_by = models.CharField(max_length=100, null=False)
    creation_date = models.DateField(auto_now_add=True)
    open_to_update = models.BooleanField(default=False)
    visible_to_users = models.BooleanField(default=False)
    approval1 = models.BooleanField(default=False)
    approval2 = models.BooleanField(default=False)
    approval3 = models.BooleanField(default=False)

class SMART_Forecast_Model(models.Model):
    version = models.ForeignKey(scenarios,  on_delete=models.CASCADE)
    Data_Source = models.CharField(max_length=100,blank=True, null=True)
    Forecast_Region = models.CharField(max_length=100,blank=True, null=True)
    Product_Group = models.CharField(max_length=100,blank=True, null=True)
    Product = models.CharField(max_length=100,blank=True, null=True)
    ProductFamilyDescription = models.CharField(max_length=100,blank=True, null=True)
    Customer_code = models.CharField(max_length=100,blank=True, null=True)
    Location = models.CharField(max_length=100,blank=True, null=True)
    Forecasted_Weight_Curr = models.FloatField(default=0, null=True)
    PriceAUD = models.FloatField(default=0, null=True,blank=True)
    DP_Cycle = models.DateField( null=True,blank=True)
    Period_AU = models.DateField( null=True,blank=True)
    Qty = models.FloatField(default=0, null=True,blank=True)
    Tonnes = models.FloatField(default=0, null=True, blank=True) # New field to store pre-calculated Tonnes
   
    def __str__(self):
        return self.Product   
    
class Revenue_Forecast_Model(models.Model):
    version = models.ForeignKey(scenarios , on_delete=models.CASCADE)
    Data_Source = models.CharField(max_length=100,blank=True, null=True)
    Forecast_Region = models.CharField(max_length=100,blank=True, null=True)
    ParentProductGroupDescription = models.CharField(max_length=100,blank=True, null=True)
    ProductGroupDescription = models.CharField(max_length=100,blank=True, null=True)
    Period_AU = models.DateField( null=True)
    Revenue = models.FloatField(default=0, null=True)
   
    def __str__(self):
        return self.Product

class Product_Model(models.Model):
    Product = models.CharField(max_length=100)
    Product_Group = models.CharField(max_length=100, null=True)
    image = models.ImageField(null=True, blank=True, upload_to="images/")

    def __str__(self):
        return self.Product
        
class MasterDataOrderBook(models.Model):
    version = models.ForeignKey(scenarios , on_delete=models.CASCADE)
    site = models.CharField(max_length=100)
    productkey = models.CharField(max_length=100)

    def __str__(self):
        return f"{self.version.version} - {self.productkey}"

class MasterDataCapacityModel(models.Model):
    version = models.ForeignKey(scenarios , on_delete=models.CASCADE)  # Foreign key from scenarios
    subversion = models.CharField(max_length=250, null=True, blank=True)
    Foundry = models.ForeignKey(MasterDataPlantModel, on_delete=models.CASCADE)  # Foreign key from MasterDataPlantModel
    PouringDaysPerWeek = models.IntegerField(null=True, blank=True)
    ShiftsPerDay = models.IntegerField(null=True, blank=True)
    HoursPershift = models.IntegerField(null=True, blank=True)
    Maxnumberofheatsperday = models.IntegerField(null=True, blank=True)
    Minnumberofheatsperday = models.IntegerField(null=True, blank=True)
    Averagenumberofheatsperday = models.IntegerField(null=True, blank=True)
    Month = models.DateField(null=True, blank=True)
    Yiled = models.FloatField(null=True, blank=True)
    Waster = models.FloatField(null=True, blank=True)
    Dresspouringcapacity = models.FloatField(null=True, blank=True)
    Calendardays = models.IntegerField(null=True, blank=True)
    Plannedmaintenancedays = models.IntegerField(null=True, blank=True)
    Publicholidays = models.IntegerField(null=True, blank=True)
    Weekends = models.IntegerField(null=True, blank=True)
    Othernonpouringdays = models.IntegerField(null=True, blank=True)
    Unavailabiledays = models.IntegerField(null=True, blank=True)
    Availabledays = models.IntegerField(null=True, blank=True)
    Heatsperday = models.IntegerField(null=True, blank=True)
    CastMasstonsperheat = models.FloatField(null=True, blank=True)
    Casttonnesperday = models.FloatField(null=True, blank=True)
    Workcentre = models.CharField(max_length=250, default="Pouring")

    def __str__(self):
        return f"{self.Workcentre} - {self.version.version} - {self.Foundry.SiteName}"
    
class MasterDataCommentModel(models.Model):
    version = models.CharField(max_length=250)
    subversion = models.CharField(max_length=250)
    PriorAssumptionsDecisions = models.TextField(max_length=1000)
    NewAssumptions = models.TextField(max_length=1000)
    Opportunities = models.TextField(max_length=1000)
    Risk = models.TextField(max_length=1000)
    DecisionsActionsRequired = models.TextField(max_length=1000)
    SummaryKPI = models.TextField(max_length=1000)
    SummaryCapacity = models.TextField(max_length=1000)
    
    def __str__(self):
        return self.version
    
class MasterDataHistoryOfProductionModel(models.Model):
    version = models.ForeignKey(scenarios , on_delete=models.CASCADE)  # Changed to ForeignKey
    Product = models.CharField(max_length=250)
    Foundry = models.CharField(max_length=250)
    ProductionMonth = models.DateField()
    ProductionQty = models.IntegerField()

    def __str__(self):
        return f"{self.version.version} - {self.Product}"
    
class MasterDataIncotTermTypesModel(models.Model):
    version = models.ForeignKey(scenarios , on_delete=models.CASCADE)  # Foreign key from scenarios
    IncoTerm = models.CharField(max_length=250)  # Not globally unique
    IncoTermCaregory = models.CharField(max_length=250)

    class Meta:
        unique_together = ('version', 'IncoTerm')  # Ensure uniqueness per version

    def __str__(self):
        return f"{self.version.version} - {self.IncoTerm}"
    
class MasterdataIncoTermsModel(models.Model):
    version = models.ForeignKey(scenarios , max_length=250, on_delete=models.CASCADE)
    CustomerCode = models.CharField(max_length=250)
    Incoterm = models.ForeignKey(MasterDataIncotTermTypesModel, on_delete=models.CASCADE)  # Reference the primary key

    def __str__(self):
        return f"{self.version.version} - {self.CustomerCode} - {self.Incoterm.IncoTerm}"
    
class MasterDataLeadTimesModel(models.Model):
    version = models.CharField(max_length=250)
    Foundry = models.CharField(max_length=250)
    SalesClass = models.CharField(max_length=250)
    InventoryClass = models.CharField(max_length=250)
    LeatTimeDays = models.IntegerField()

    def __str__(self):
        return self.version
    
from django.db import models

class MasterDataPlan(models.Model):
    version = models.ForeignKey(scenarios , on_delete=models.CASCADE)
    Foundry = models.ForeignKey(MasterDataPlantModel, on_delete=models.CASCADE)
    Month = models.DateField(null=True, blank=True)
    Yield = models.FloatField(null=True, blank=True)
    WasterPercentage = models.FloatField(null=True, blank=True)
    PlannedMaintenanceDays = models.IntegerField(null=True, blank=True)
    PublicHolidays = models.IntegerField(null=True, blank=True)
    Weekends = models.IntegerField(null=True, blank=True)
    OtherNonPouringDays = models.IntegerField(null=True, blank=True)
    heatsperdays = models.FloatField(null=True, blank=True)
    TonsPerHeat = models.FloatField(null=True, blank=True)

    @property
    def CalendarDays(self):
        """Calculate the number of days in the given month."""
        if self.Month:
            from calendar import monthrange
            return monthrange(self.Month.year, self.Month.month)[1]
        return 0

    @property
    def AvailableDays(self):
        """Calculate available days."""
        if self.CalendarDays:
            return (
                self.CalendarDays
                - (self.PublicHolidays or 0)
                - (self.Weekends or 0)
                - (self.OtherNonPouringDays or 0)
                - (self.PlannedMaintenanceDays or 0)
            )
        return 0

    @property
    def PlanDressMass(self):
        """Calculate dress mass."""
        if self.AvailableDays and self.heatsperdays and self.TonsPerHeat and self.Yield:
            return (
                self.AvailableDays
                * self.heatsperdays
                * self.TonsPerHeat
                * self.Yield
                * (1 - (self.WasterPercentage or 0) / 100)
            )
        return 0

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['Foundry', 'Month', 'version'], name='unique_foundry_month_version')
        ]

    def __str__(self):
        return f"{self.Foundry.SiteName} - {self.Month} - {self.Version.version}"
    
class MasterDataProductModel(models.Model):
    Product = models.CharField(max_length=250, primary_key=True)
    ProductDescription = models.TextField(null=True,blank=True)
    SalesClass = models.CharField(max_length=250, null=True,blank=True)
    SalesClassDescription = models.TextField(null=True,blank=True)
    ProductGroup = models.CharField(max_length=250, null=True,blank=True)
    ProductGroupDescription = models.TextField(null=True)
    InventoryClass = models.CharField(max_length=250, null=True,blank=True)
    InventoryClassDescription = models.TextField(null=True,blank=True)
    ParentProductGroup = models.CharField(max_length=250, null=True,blank=True)
    ParentProductGroupDescription = models.TextField(null=True,blank=True)
    ProductFamily = models.CharField(max_length=250, null=True,blank=True)
    ProductFamilyDescription = models.TextField(null=True,blank=True)
    DressMass = models.FloatField(null=True,blank=True)
    CastMass = models.FloatField(null=True,blank=True)
    Grade = models.CharField(max_length=250, null=True,blank=True)
    PartClassID = models.CharField(max_length=250, null=True,blank=True)
    PartClassDescription = models.TextField(null=True,blank=True)
    ExistsInEpicor = models.BooleanField(null=True,blank=True)

    def __str__(self):
        return self.Product
    
class MasterDataProductPictures(models.Model):
    product = models.ForeignKey(MasterDataProductModel, on_delete=models.CASCADE)
    Image = models.ImageField(null=True)

class MasterDataProductAttributesModel(models.Model):
    version = models.CharField(max_length=250)
    Product = models.CharField(max_length=250)
    Region = models.CharField(max_length=250)
    DynamicSafetyStock = models.FloatField()
    StaticSafetyStock = models.FloatField()
    OrderMultipleQty = models.FloatField()
    MinOrderQty = models.FloatField()

    def __str__(self):
        return f"{self.version} - {self.Product}"

class MasterDataSalesAllocationToPlantModel(models.Model):
    # this class is used to store the data related to revenue based demand where data is not in SKU level
    # fixed plant data is not stored in this class
    version = models.CharField(max_length=250)
    Plant = models.CharField(max_length=250)
    SalesClass = models.CharField(max_length=250)
    Allocation = models.FloatField()
    
    def __str__(self):
        return f"{self.version} - {self.Plant} - {self.SalesClass}"

class MasterDataSalesModel(models.Model):
    # this class is used to store the data related to revenue based demand where data is not in SKU level
    # fixed plant data is not stored in this class
    version = models.CharField(max_length=250)
    SalesClass = models.CharField(max_length=250)
    GrossMargin = models.FloatField()
    InHouseProduction = models.FloatField()
    CostAUDPerKg = models.FloatField()
    
    def __str__(self):
        return f"{self.version} - {self.SalesClass}"

class MasterDataSKUTransferModel(models.Model):
    version = models.CharField(max_length=250)
    Product = models.CharField(max_length=250)
    Date = models.DateField()
    Supplier = models.CharField(max_length=250)

    def __str__(self):
        return f"{self.version} - {self.Product} - {self.Date}"
    
class MasterDataScheduleModel(models.Model):
    version = models.ForeignKey(scenarios, on_delete=models.CASCADE, default=None)  # Removed `to_field='version'`
    Plant = models.CharField(max_length=250, blank=True, null=True)
    SalesClass = models.CharField(max_length=250, blank=True, null=True)
    ProductGroup = models.CharField(max_length=250, blank=True, null=True)
    Date = models.DateField(blank=True, null=True)
    ScheduleQty = models.FloatField(blank=True, null=True)
    UnitOfMeasure = models.CharField(max_length=250, blank=True, null=True)

    def __str__(self):
        return f"{self.version} - {self.Plant}"

class AggregatedForecast(models.Model):
    version = models.ForeignKey(scenarios , on_delete=models.CASCADE)
    tonnes = models.FloatField(default=0, null=True, blank=True)
    customer_code = models.CharField(max_length=100, blank=True, null=True)
    period = models.DateField(null=True, blank=True)
    product = models.ForeignKey(MasterDataProductModel, on_delete=models.CASCADE)
    product_group_description = models.TextField(null=True, blank=True)
    parent_product_group_description = models.TextField(null=True, blank=True)
    forecast_region = models.CharField(max_length=100, null=True, blank=True)  # New field

    def __str__(self):
        return f"{self.product.Product} - {self.version.version}"

class MasterDataInventory(models.Model):
    version = models.ForeignKey(scenarios , on_delete=models.CASCADE)  # Foreign key from scenarios
    date_of_snapshot = models.DateField()  # Date of the inventory snapshot
    product = models.CharField(max_length=250)  # Product identifier
    site = models.ForeignKey(MasterDataPlantModel, on_delete=models.CASCADE)  # Foreign key to MasterDataPlantModel
    site_region = models.CharField(max_length=250)  # Region of the site
    onhandstock_qty = models.FloatField(default=0)  # Quantity of on-hand stock
    intransitstock_qty = models.FloatField(default=0)  # Quantity of in-transit stock
    wip_stock_qty = models.FloatField(default=0)  # Quantity of WIP stock

    def __str__(self):
        return f"{self.version.version} - {self.product} - {self.date_of_snapshot}"

class MasterDataForecastRegionModel(models.Model):
    Forecast_region = models.CharField(primary_key=True,max_length=250)

class MasterDataFreightModel(models.Model):
    version = models.ForeignKey(scenarios , on_delete=models.CASCADE)  # Foreign key from scenarios
    ForecastRegion = models.ForeignKey(MasterDataForecastRegionModel,  on_delete=models.CASCADE)  # Foreign key from MasterDataForecastRegionModel
    ManufacturingSite = models.ForeignKey(MasterDataPlantModel, on_delete=models.CASCADE)  # Foreign key from MasterDataPlantModel
    PlantToDomesticPortDays = models.IntegerField()
    OceanFreightDays = models.IntegerField()
    PortToCustomerDays = models.IntegerField()

    def __str__(self):
        return f"{self.version.version} - {self.ForecastRegion.Forecast_region} - {self.ManufacturingSite.SiteName}"

class MasterDataCastToDespatchModel(models.Model):
    version = models.ForeignKey(scenarios , on_delete=models.CASCADE)  # Foreign key from scenarios
    Foundry = models.ForeignKey(MasterDataPlantModel, on_delete=models.CASCADE)  # Foreign key from MasterDataPlantModel
    CastToDespatchDays = models.IntegerField()

    def __str__(self):
        return self.version.version

class CalcualtedReplenishmentModel(models.Model):
    version = models.ForeignKey(scenarios , on_delete=models.CASCADE)
    Product = models.ForeignKey(MasterDataProductModel, on_delete=models.CASCADE)
    Location = models.CharField( max_length=100, null=True, blank=True)  # Allow NULL values
    Site = models.ForeignKey(MasterDataPlantModel, max_length=250, null=True, blank=True, on_delete=models.CASCADE)  # Allow NULL values
    ShippingDate = models.DateField()
    ReplenishmentQty = models.FloatField(default=0)

    def __str__(self):
        return f"{self.version.version} - {self.Product} - {self.Site}"
    
class CalculatedProductionModel(models.Model):
    version = models.ForeignKey(scenarios , on_delete=models.CASCADE)  # Foreign key from scenarios
    product = models.ForeignKey(MasterDataProductModel, on_delete=models.CASCADE)
    site = models.ForeignKey(MasterDataPlantModel, max_length=250, null=True, blank=True, on_delete=models.CASCADE)  # Allow NULL values
    pouring_date = models.DateField()
    production_quantity = models.FloatField(default=0)
    tonnes = models.FloatField(default=0)

    def __str__(self):
        return f"{self.version.version} - {self.product.Product} - {self.site.SiteName} - {self.pouring_date}"
    
class MasterDataSuppliersModel(models.Model):
    VendorID = models.CharField(primary_key=True, max_length=250)
    TradingName = models.CharField(max_length=250, null=True, blank=True)
    Address1 = models.CharField(max_length=250, null=True, blank=True)
    
    def __str__(self):
        return self.VendorID or "Unknown Supplier"

class MasterDataCustomersModel(models.Model):
    CustomerId = models.CharField(primary_key=True, max_length=250)
    CustomerName = models.CharField(max_length=250, null=True, blank=True)
    CustomerRegion = models.CharField(max_length=250, null=True, blank=True)
    ForecastRegion = models.CharField(max_length=250, null=True, blank=True)

    def __str__(self):
        return self.CustomerId or "Unknown Customer"
    
class MasterDataSupplyOptionsModel(models.Model):
    Product = models.ForeignKey(MasterDataProductModel, on_delete=models.CASCADE)  # Foreign key from MasterDataProductModel
    InhouseOrOutsource = models.CharField(max_length=250, default='Inhouse')  # Default value set to 'Inhouse'
    Supplier = models.ForeignKey(
        MasterDataSuppliersModel,
       
        on_delete=models.CASCADE,
        null=True,  # Allow NULL values
        blank=True  # Allow blank values in forms
    )
    Site = models.ForeignKey(
        MasterDataPlantModel,
        
        on_delete=models.CASCADE,
        null=True,  # Allow NULL values
        blank=True  # Allow blank values in forms
    )  # Foreign key from MasterDataPlantModel
    SourceName = models.CharField(max_length=250, null=True, blank=True)
    DateofSupply = models.DateField(null=True, blank=True)
    Qty = models.FloatField(default=0, null=True, blank=True)
    Tonnes = models.FloatField(default=0, null=True, blank=True)  # New field to store pre-calculated Tonnes

    @property
    def Source(self):
        """Return the value of Supplier or Site, whichever is not None."""
        if self.Supplier:
            return self.Supplier.VendorID
        elif self.Site:
            return self.Site.SiteName
        return "No Source Assigned"

    def __str__(self):
        return f"{self.Product.Product} - {self.Source}"
    
class MasterDataEpicorSupplierMasterDataModel(models.Model):
    version = models.ForeignKey(scenarios, on_delete=models.CASCADE, null=True, blank=True)  # Add foreign key to scenarios
    Company = models.CharField(max_length=100, null=True, blank=True)
    Plant = models.CharField(max_length=100, null=True, blank=True)
    PartNum = models.CharField(max_length=100, null=True, blank=True)
    VendorID = models.CharField(max_length=100, null=True, blank=True)
    SourceType = models.CharField(max_length=50, null=True, blank=True)  # New field for SourceType

    def __str__(self):
        return f"{self.Company} - {self.Plant} - {self.PartNum} - {self.VendorID} - {self.SourceType}"
    
class MasterDataEpicorBillOfMaterialModel(models.Model):    
    Company = models.CharField(max_length=100, null=True, blank=True)
    Plant = models.CharField(max_length=100, null=True, blank=True)
    Parent = models.CharField(max_length=100, null=True, blank=True)
    ComponentSeq = models.IntegerField(max_length=100, null=True, blank=True)
    Component = models.CharField(max_length=100, null=True, blank=True)
    ComponentUOM = models.CharField(max_length=100, null=True, blank=True)
    QtyPer = models.CharField(max_length=100, null=True, blank=True)
    EstimatedScrap = models.FloatField(max_length=100, null=True, blank=True)
    SalvageQtyPer = models.FloatField(max_length=100, null=True, blank=True)


    def __str__(self):
        return f"{self.Company} - {self.Plant} - {self.Parent}"
    
class MasterDataManuallyAssignProductionRequirement(models.Model):
    version = models.ForeignKey(scenarios, on_delete=models.CASCADE, null=True, blank=True)  # Add foreign key to scenarios
    Product = models.ForeignKey(MasterDataProductModel, on_delete=models.CASCADE, null=True, blank=True)  # Foreign key from MasterDataProductModel
    Site = models.ForeignKey(MasterDataPlantModel, on_delete=models.CASCADE, null=True, blank=True)  # Foreign key from MasterDataPlantModel
    ShippingDate = models.DateField(null=True, blank=True)
    Percentage = models.FloatField(default=0, null=True, blank=True)  # Percentage of the production requirement

    def __str__(self):
        return f"{self.Product.Product} - {self.Site.SiteName} - {self.ShippingDate}"