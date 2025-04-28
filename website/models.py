from django.db import models

# Create your models here.


class test(models.Model):
    test = models.CharField(max_length=100)

    def __str__(self):
        return self.test

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
    version = models.ForeignKey(scenarios, to_field='version', on_delete=models.CASCADE)
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
    version = models.ForeignKey(scenarios, to_field='version', on_delete=models.CASCADE)
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
    version = models.ForeignKey(scenarios, to_field='version', on_delete=models.CASCADE)
    site = models.CharField(max_length=100)
    productkey = models.CharField(max_length=100)

    def __str__(self):
        return f"{self.version.version} - {self.productkey}"

class MasterDataCapacityModel(models.Model):
    version = models.CharField(max_length=250, null=True, blank=True)
    subversion = models.CharField(max_length=250, null=True, blank=True)
    Foundry = models.CharField(max_length=250, null=True, blank=True)
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
        return self.Workcentre
    
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
    version = models.ForeignKey(scenarios, to_field='version', on_delete=models.CASCADE)  # Changed to ForeignKey
    Product = models.CharField(max_length=250)
    Foundry = models.CharField(max_length=250)
    ProductionMonth = models.DateField()
    ProductionQty = models.IntegerField()

    def __str__(self):
        return f"{self.version.version} - {self.Product}"
    
class MasterDataIncotTermTypesModel(models.Model):
    version = models.CharField(max_length=250)
    IncoTerm = models.CharField(max_length=250)
    IncoTermCaregory = models.CharField(max_length=250)

    def __str__(self):
        return self.version
    
class MasterdataIncoTermsModel(models.Model):
    version = models.CharField(max_length=250)
    CustomerCode = models.CharField(max_length=250)
    Incoterm = models.CharField(max_length=250)

    def __str__(self):
        return self.version
    
class MasterDataLeadTimesModel(models.Model):
    version = models.CharField(max_length=250)
    Foundry = models.CharField(max_length=250)
    SalesClass = models.CharField(max_length=250)
    InventoryClass = models.CharField(max_length=250)
    LeatTimeDays = models.IntegerField()

    def __str__(self):
        return self.version
    
class MasterDataPlan(models.Model):
    Version	= models.CharField(max_length=250)
    SubVersion = models.CharField(max_length=250)
    Foundry = models.CharField(max_length=250)
    FB = models.CharField(max_length=250)
    PouringDaysperweek = models.IntegerField()
    CalendarDays = models.IntegerField()
    Month = models.DateField()
    Yield = models.FloatField()
    WasterPercentage = models.FloatField()
    PlanDressMass = models.FloatField()
    UnavailableDays = models.IntegerField()
    AvailableDays = models.IntegerField()
    PlannedMaintenanceDays = models.IntegerField()
    PublicHolidays = models.IntegerField()
    Weekends = models.IntegerField()
    OtherNonPouringDays = models.IntegerField()
    HeatsPerweek = models.FloatField()
    heatsperdays = models.FloatField()
    CastMass = models.FloatField()
    TonsPerHeat = models.FloatField()
    CastTonsPerDay = models.FloatField()

    def __str__(self):
        return f"{self.Version} - {self.Foundry}"
    
class MasterDataPlantModel(models.Model):
    SiteName = models.CharField(primary_key=True, max_length=250)
    Company = models.CharField(max_length=250,null=True, blank=True)
    Country = models.CharField(max_length=250,null=True, blank=True)
    Location = models.CharField(max_length=250,null=True, blank=True)
    PlantRegion = models.CharField(max_length=250,null=True, blank=True)
    SiteType = models.CharField(max_length=250,null=True, blank=True)
    About = models.TextField(null=True,max_length=3000, blank=True)

    def __str__(self):
        return self.SiteName or "Unknown Site"

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
    product = models.ForeignKey(MasterDataProductModel, to_field='Product', on_delete=models.CASCADE)
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
    Version = models.CharField(max_length=250)
    Plant = models.CharField(max_length=250)
    SalesClass = models.CharField(max_length=250)
    Allocation = models.FloatField()
    
    def __str__(self):
        return f"{self.Version} - {self.Plant} - {self.SalesClass}"

class MasterDataSalesModel(models.Model):
    # this class is used to store the data related to revenue based demand where data is not in SKU level
    # fixed plant data is not stored in this class
    Version = models.CharField(max_length=250)
    SalesClass = models.CharField(max_length=250)
    GrossMargin = models.FloatField()
    InHouseProduction = models.FloatField()
    CostAUDPerKg = models.FloatField()
    
    def __str__(self):
        return f"{self.Version} - {self.SalesClass}"

class MasterDataSKUTransferModel(models.Model):
    version = models.CharField(max_length=250)
    Product = models.CharField(max_length=250)
    Date = models.DateField()
    Supplier = models.CharField(max_length=250)

    def __str__(self):
        return f"{self.version} - {self.Product} - {self.Date}"

class MasterDataScheduleModel(models.Model):
    Scenario_Foreign_Key = models.ForeignKey(scenarios, to_field='version', on_delete=models.CASCADE, default=None)
    Version_id = models.CharField(max_length=250, blank=True, null=True)
    Plant = models.CharField(max_length=250,blank=True,null=True )
    SalesClass = models.CharField(max_length=250, blank=True,null=True)
    ProductGroup = models.CharField(max_length=250, blank=True,null=True)
    Date = models.DateField(blank=True,null=True)
    ScheduleQty = models.FloatField(blank=True,null=True)
    UnitOfMeasure = models.CharField(max_length=250, blank=True,null=True)

    def __str__(self):
        return f"{self.Version_id} - {self.Plant}"
    
class AggregatedForecast(models.Model):
    version = models.ForeignKey(scenarios, to_field='version', on_delete=models.CASCADE)
    tonnes = models.FloatField(default=0, null=True, blank=True)
    customer_code = models.CharField(max_length=100, blank=True, null=True)
    period = models.DateField(null=True, blank=True)
    product = models.ForeignKey(MasterDataProductModel, to_field='Product', on_delete=models.CASCADE)
    product_group_description = models.TextField(null=True, blank=True)
    parent_product_group_description = models.TextField(null=True, blank=True)
    forecast_region = models.CharField(max_length=100, null=True, blank=True)  # New field

    def __str__(self):
        return f"{self.product.Product} - {self.version.version}"

class MasterDataInventory(models.Model):
    version = models.ForeignKey(scenarios, to_field='version', on_delete=models.CASCADE)  # Foreign key from scenarios
    date_of_snapshot = models.DateField()  # Date of the inventory snapshot
    product = models.CharField(max_length=250)  # Product identifier
    site = models.ForeignKey(MasterDataPlantModel, to_field='SiteName', on_delete=models.CASCADE)  # Foreign key to MasterDataPlantModel
    site_region = models.CharField(max_length=250)  # Region of the site
    onhandstock_qty = models.FloatField(default=0)  # Quantity of on-hand stock
    intransitstock_qty = models.FloatField(default=0)  # Quantity of in-transit stock
    wip_stock_qty = models.FloatField(default=0)  # Quantity of WIP stock

    def __str__(self):
        return f"{self.version.version} - {self.product} - {self.date_of_snapshot}"

class MasterDataForecastRegionModel(models.Model):
    Forecast_region = models.CharField(primary_key=True,max_length=250)

class MasterDataFreightModel(models.Model):
    version = models.ForeignKey(scenarios, to_field='version', on_delete=models.CASCADE)  # Foreign key from scenarios
    ForecastRegion = models.ForeignKey(MasterDataForecastRegionModel, to_field='Forecast_region', on_delete=models.CASCADE)  # Foreign key from MasterDataForecastRegionModel
    ManufacturingSite = models.ForeignKey(MasterDataPlantModel, to_field='SiteName', on_delete=models.CASCADE)  # Foreign key from MasterDataPlantModel
    PlantToDomesticPortDays = models.IntegerField()
    OceanFreightDays = models.IntegerField()
    PortToCustomerDays = models.IntegerField()

    def __str__(self):
        return f"{self.version.version} - {self.ForecastRegion.Forecast_region} - {self.ManufacturingSite.SiteName}"

class MasterDataCustomersModel(models.Model):
    CustomerCode = models.CharField(primary_key=True,max_length=250)
    CustomerName = models.CharField(max_length=250,null=True, blank=True)
    CustomerGroup = models.CharField(max_length=250,null=True, blank=True)
    CustomerGroupDescription = models.TextField(null=True,blank=True)
    SalesClass = models.CharField(max_length=250,null=True, blank=True)
    SalesClassDescription = models.TextField(null=True,blank=True)
    ProductGroup = models.CharField(max_length=250,null=True, blank=True)
    ProductGroupDescription = models.TextField(null=True,blank=True)

    def __str__(self):
        return self.CustomerCode or "Unknown Customer"

class MasterDataCastToDespatchModel(models.Model):
    version = models.ForeignKey(scenarios, to_field='version', on_delete=models.CASCADE)  # Foreign key from scenarios
    Foundry = models.ForeignKey(MasterDataPlantModel, to_field='SiteName', on_delete=models.CASCADE)  # Foreign key from MasterDataPlantModel
    CastToDespatchDays = models.IntegerField()

    def __str__(self):
        return self.version.version