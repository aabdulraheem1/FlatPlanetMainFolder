
# ==================================================================================
# WARNING TO FUTURE DEVELOPERS AND COPILOT:
# NO CACHE LOGIC IS ACCEPTED IN THIS SYSTEM
# NO FALLBACK LOGIC IS ACCEPTED IN THIS SYSTEM  
# IF DATA DOES NOT EXIST, FAIL FAST WITH CLEAR ERROR MESSAGE
# DO NOT ATTEMPT TO IMPLEMENT CACHING OR FALLBACK SOLUTIONS
# USER EXPLICITLY REJECTED ALL CACHING AND FALLBACK APPROACHES
# ==================================================================================

# ==================================================================================
# MODEL CLASSIFICATION FOR CHANGE TRACKING:
#
# 🟢 INPUT MODELS (tracked for changes):
#   - Master data models that users can modify (MasterDataPlantModel, etc.)
#   - Forecast data (SMART_Forecast_Model, Revenue_Forecast_Model) 
#   - Configuration models (scenarios, MasterDataCapacityModel, etc.)
#   - Models with version/scenario fields that contain USER INPUT data
#
# 🔴 CALCULATED/OUTPUT MODELS (excluded from change tracking):
#   - CalculatedProductionModel - populated BY calculate_model process
#   - CalcualtedReplenishmentModel - populated BY calculate_model process  
#   - AggregatedForecast - populated BY calculate_model process
#   - All Cached* models - temporary calculation results
#   - InventoryProjectionModel - calculated from input data
#   - OpeningInventorySnapshot - cached external data
#   - MonthlyPouredDataModel - cached external data
#
# 🔍 HOW TO DISTINGUISH:
#   - INPUT: User enters/modifies this data directly
#   - OUTPUT: Calculate_model process populates this data
#   - If unsure, ask: "Does the user directly edit this data, or is it calculated?"
# ==================================================================================

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
    mark_as_outsource_supplier = models.BooleanField(default=False, help_text="Mark this plant as an outsource supplier")
    
    # Data source tracking fields
    is_user_created = models.BooleanField(default=False, help_text="True if this record was created manually by user")
    last_imported_from_epicor = models.DateTimeField(null=True, blank=True, help_text="Last time this record was updated from Epicor")
    user_modified_fields = models.JSONField(default=dict, help_text="Track which fields were modified by users")
    created_by_user = models.CharField(max_length=100, null=True, blank=True, help_text="Username who created this record manually")
    last_modified_by_user = models.CharField(max_length=100, null=True, blank=True, help_text="Username who last modified this record")
    last_user_modification_date = models.DateTimeField(null=True, blank=True, help_text="When user last modified this record")

    def __str__(self):
        return self.SiteName or "Unknown Site"


class ReceiptedQuantity(models.Model):
    """Model to store receipted quantities from suppliers - not scenario related"""
    supplier = models.ForeignKey(MasterDataPlantModel, on_delete=models.CASCADE, help_text="Supplier (VendorID from PowerBI)")
    product = models.CharField(max_length=250, help_text="Product identifier")
    purchased_qty = models.FloatField(help_text="Transaction Qty from PowerBI")
    purchased_tonnes = models.FloatField(help_text="Transaction Qty * Dress Mass")
    month_of_supply = models.DateField(help_text="Month of supply (grouped by month)")
    receipt_date = models.DateField(help_text="Original receipt date")
    dress_mass = models.FloatField(null=True, blank=True, help_text="Dress mass from product")
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        verbose_name = "Receipted Quantity"
        verbose_name_plural = "Receipted Quantities"
        ordering = ['-month_of_supply', 'supplier']
        indexes = [
            models.Index(fields=['supplier', 'month_of_supply']),
            models.Index(fields=['product', 'month_of_supply']),
        ]
    
    def __str__(self):
        return f"{self.supplier.SiteName} - {self.product} - {self.month_of_supply.strftime('%Y-%m')}"


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
    
    # Model calculation tracking - NO CACHING, REAL-TIME CHANGE DETECTION
    last_calculated = models.DateTimeField(null=True, blank=True, help_text="When calculate_model was last run successfully")
    calculation_status = models.CharField(max_length=50, default='never_calculated', choices=[
        ('never_calculated', 'Never Calculated'),
        ('up_to_date', 'Up to Date'),
        ('changes_detected', 'Changes Detected - Recalculation Needed'),
        ('calculating', 'Currently Calculating'),
        ('calculation_failed', 'Calculation Failed')
    ], help_text="Current calculation status")

# ========== INPUT MODELS - TRACKED FOR CHANGES ==========
# 🟢 These models contain user input data and are tracked for changes

class SMART_Forecast_Model(models.Model):
    """
    🟢 INPUT MODEL - TRACKED FOR CHANGES
    Contains forecast data that users can upload/modify directly.
    Changes to this model should trigger recalculation.
    """
    version = models.ForeignKey(scenarios,  on_delete=models.CASCADE)
    Data_Source = models.CharField(max_length=100,blank=True, null=True)
    Forecast_Region = models.CharField(max_length=100,blank=True, null=True)
    Product_Group = models.CharField(max_length=100,blank=True, null=True)
    Product = models.CharField(max_length=100,blank=True, null=True)
    ProductFamilyDescription = models.CharField(max_length=100,blank=True, null=True)
    Customer_code = models.CharField(max_length=100,blank=True, null=True)
    Location = models.CharField(max_length=100,blank=True, null=True)
    Forecasted_Weight_Curr = models.FloatField(default=0, null=True,blank=True)
    PriceAUD = models.FloatField(default=0, null=True,blank=True)
    DP_Cycle = models.DateField( null=True,blank=True)
    Period_AU = models.DateField( null=True,blank=True)
    Qty = models.FloatField(default=0, null=True,blank=True)
    Tonnes = models.FloatField(default=0, null=True, blank=True) # New field to store pre-calculated Tonnes
    
    # Timestamp fields for change tracking
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return self.Product   
    
class Revenue_Forecast_Model(models.Model):
    """
    🟢 INPUT MODEL - TRACKED FOR CHANGES
    Contains revenue forecast data that users can upload/modify directly.
    Changes to this model should trigger recalculation.
    """
    version = models.ForeignKey(scenarios , on_delete=models.CASCADE)
    Data_Source = models.CharField(max_length=100,blank=True, null=True)
    Forecast_Region = models.CharField(max_length=100,blank=True, null=True)
    ParentProductGroupDescription = models.CharField(max_length=100,blank=True, null=True)
    ProductGroupDescription = models.CharField(max_length=100,blank=True, null=True)
    Period_AU = models.DateField( null=True)
    Revenue = models.FloatField(default=0, null=True)
    
    # Timestamp fields for change tracking
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
   
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
    
    # Timestamp fields for change tracking
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

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
    CalendarDays = models.IntegerField(null=True, blank=True)  # Stored calculated value
    AvailableDays = models.IntegerField(null=True, blank=True)  # Stored calculated value  
    PlanDressMass = models.FloatField(null=True, blank=True)  # Stored calculated value

    def calculate_calendar_days(self):
        """Calculate the number of days in the given month."""
        if self.Month:
            from calendar import monthrange
            return monthrange(self.Month.year, self.Month.month)[1]
        return 0

    def calculate_available_days(self):
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

    def calculate_plan_dress_mass(self):
        """Calculate and return dress mass value."""
        if self.AvailableDays and self.heatsperdays and self.TonsPerHeat and self.Yield:
            return (
                self.AvailableDays
                * self.heatsperdays
                * self.TonsPerHeat
                * (self.Yield / 100)  # Convert percentage to decimal
                * (1 - (self.WasterPercentage or 0) / 100)
            )
        return 0

    def save(self, *args, **kwargs):
        """Override save to automatically calculate all derived fields."""
        # Calculate and store all calculated fields
        self.CalendarDays = self.calculate_calendar_days()
        self.AvailableDays = self.calculate_available_days()
        self.PlanDressMass = self.calculate_plan_dress_mass()
        super().save(*args, **kwargs)

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['Foundry', 'Month', 'version'], name='unique_foundry_month_version')
        ]

    def __str__(self):
        return f"{self.Foundry.SiteName} - {self.Month} - {self.version.version}"
    
class MasterDataProductModel(models.Model):
    Product = models.CharField(max_length=250, primary_key=True)
    ProductDescription = models.TextField(null=True,blank=True)
    SalesClass = models.CharField(max_length=250, null=True,blank=True)
    SalesClassDescription = models.TextField(null=True,blank=True)
    ProductGroup = models.CharField(max_length=250, null=True,blank=True)
    ProductGroupDescription = models.TextField(null=True,blank=True)
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
    
    # Customer data fields (populated from PowerBI for performance optimization)
    latest_customer_name = models.CharField(max_length=500, null=True, blank=True, help_text="Latest customer name from PowerBI invoices")
    latest_invoice_date = models.DateField(null=True, blank=True, help_text="Date of latest invoice from PowerBI")
    customer_data_last_updated = models.DateTimeField(null=True, blank=True, help_text="When customer data was last fetched from PowerBI")
    
    # Data source tracking fields
    is_user_created = models.BooleanField(default=False, help_text="True if this record was created manually by user")
    last_imported_from_epicor = models.DateTimeField(null=True, blank=True, help_text="Last time this record was updated from Epicor")
    user_modified_fields = models.JSONField(default=dict, help_text="Track which fields were modified by users")
    created_by_user = models.CharField(max_length=100, null=True, blank=True, help_text="Username who created this record manually")
    last_modified_by_user = models.CharField(max_length=100, null=True, blank=True, help_text="Username who last modified this record")
    last_user_modification_date = models.DateTimeField(null=True, blank=True, help_text="When user last modified this record")

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
    """
    🔴 CALCULATED/OUTPUT MODEL - EXCLUDED FROM CHANGE TRACKING
    This model is populated BY the calculate_model process, not input TO it.
    Contains aggregated forecast data calculated from SMART_Forecast_Model and Revenue_Forecast_Model.
    """
    version = models.ForeignKey(scenarios , on_delete=models.CASCADE)
    tonnes = models.FloatField(default=0, null=True, blank=True)
    customer_code = models.CharField(max_length=100, blank=True, null=True)
    period = models.DateField(null=True, blank=True)
    product = models.ForeignKey(MasterDataProductModel, on_delete=models.CASCADE)
    product_group_description = models.TextField(null=True, blank=True)
    parent_product_group_description = models.TextField(null=True, blank=True)
    forecast_region = models.CharField(max_length=100, null=True, blank=True)  # New field
    cogs_aud = models.FloatField(default=0, null=True, blank=True)
    revenue_aud = models.FloatField(default=0, null=True, blank=True)
    qty = models.FloatField(default=0, null=True, blank=True)

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
    cost_aud = models.FloatField(default=0, null=True, blank=True)  # Cost in AUD

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
    
    # Timestamp fields for change tracking
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    def __str__(self):
        return f"{self.version.version} - {self.ForecastRegion.Forecast_region} - {self.ManufacturingSite.SiteName}"

class MasterDataCastToDespatchModel(models.Model):
    version = models.ForeignKey(scenarios , on_delete=models.CASCADE)  # Foreign key from scenarios
    Foundry = models.ForeignKey(MasterDataPlantModel, on_delete=models.CASCADE)  # Foreign key from MasterDataPlantModel
    CastToDespatchDays = models.IntegerField()

    def __str__(self):
        return self.version.version

class CalcualtedReplenishmentModel(models.Model):
    """
    🔴 CALCULATED/OUTPUT MODEL - EXCLUDED FROM CHANGE TRACKING
    This model is populated BY the calculate_model process, not input TO it.
    Contains calculated replenishment requirements based on forecast and inventory data.
    """
    version = models.ForeignKey(scenarios , on_delete=models.CASCADE)
    Product = models.ForeignKey(MasterDataProductModel, on_delete=models.CASCADE)
    Location = models.CharField( max_length=100, null=True, blank=True)  # Allow NULL values
    Site = models.ForeignKey(MasterDataPlantModel, max_length=250, null=True, blank=True, on_delete=models.CASCADE)  # Allow NULL values
    ShippingDate = models.DateField()
    ReplenishmentQty = models.FloatField(default=0)
    latest_customer_invoice = models.CharField(max_length=250, null=True, blank=True)  # Latest customer name from invoice
    latest_customer_invoice_date = models.DateField(null=True, blank=True)  # Latest invoice date

    def __str__(self):
        return f"{self.version.version} - {self.Product} - {self.Site}"
    
class CalculatedProductionModel(models.Model):
    """
    🔴 CALCULATED/OUTPUT MODEL - EXCLUDED FROM CHANGE TRACKING
    This model is populated BY the calculate_model process, not input TO it.
    Contains calculated production schedules based on replenishment requirements.
    """
    version = models.ForeignKey(scenarios , on_delete=models.CASCADE)
    product = models.ForeignKey(MasterDataProductModel, on_delete=models.CASCADE)
    site = models.ForeignKey(MasterDataPlantModel, max_length=250, null=True, blank=True, on_delete=models.CASCADE)
    pouring_date = models.DateField()
    production_quantity = models.FloatField(default=0)
    tonnes = models.FloatField(default=0)
    product_group = models.CharField(max_length=250, null=True, blank=True)  # <-- Add this line
    parent_product_group = models.CharField(max_length=250, null=True, blank=True)  # <-- Add this line    
    price_aud = models.FloatField(default=0, null=True, blank=True)
    cost_aud = models.FloatField(default=0, null=True, blank=True)  # Keep for DB compatibility (unused)
    production_aud = models.FloatField(default=0, null=True, blank=True)
    revenue_aud = models.FloatField(default=0, null=True, blank=True)
    latest_customer_invoice = models.CharField(max_length=250, null=True, blank=True)  # Latest customer name from invoice
    latest_customer_invoice_date = models.DateField(null=True, blank=True)  # Latest invoice date
    is_outsourced = models.BooleanField(default=False, help_text="True if this production is from an outsourced site")

    def __str__(self):
        return f"{self.version.version} - {self.product.Product} - {self.site.SiteName} - {self.pouring_date}"
    
class MasterDataSuppliersModel(models.Model):
    VendorID = models.CharField(primary_key=True, max_length=250)
    TradingName = models.CharField(max_length=250, null=True, blank=True)
    Address1 = models.CharField(max_length=250, null=True, blank=True)
    
    # Data source tracking fields
    is_user_created = models.BooleanField(default=False, help_text="True if this record was created manually by user")
    last_imported_from_epicor = models.DateTimeField(null=True, blank=True, help_text="Last time this record was updated from Epicor")
    user_modified_fields = models.JSONField(default=dict, help_text="Track which fields were modified by users")
    created_by_user = models.CharField(max_length=100, null=True, blank=True, help_text="Username who created this record manually")
    last_modified_by_user = models.CharField(max_length=100, null=True, blank=True, help_text="Username who last modified this record")
    last_user_modification_date = models.DateTimeField(null=True, blank=True, help_text="When user last modified this record")
    
    def __str__(self):
        return self.VendorID or "Unknown Supplier"

class MasterDataCustomersModel(models.Model):
    CustomerId = models.CharField(primary_key=True, max_length=250)
    CustomerName = models.CharField(max_length=250, null=True, blank=True)
    CustomerRegion = models.CharField(max_length=250, null=True, blank=True)
    ForecastRegion = models.CharField(max_length=250, null=True, blank=True)
    
    # Data source tracking fields
    is_user_created = models.BooleanField(default=False, help_text="True if this record was created manually by user")
    last_imported_from_epicor = models.DateTimeField(null=True, blank=True, help_text="Last time this record was updated from Epicor")
    user_modified_fields = models.JSONField(default=dict, help_text="Track which fields were modified by users")
    created_by_user = models.CharField(max_length=100, null=True, blank=True, help_text="Username who created this record manually")
    last_modified_by_user = models.CharField(max_length=100, null=True, blank=True, help_text="Username who last modified this record")
    last_user_modification_date = models.DateTimeField(null=True, blank=True, help_text="When user last modified this record")

    def __str__(self):
        return self.CustomerId or "Unknown Customer"
    
class MasterDataSupplyOptionsModel(models.Model):
    Product = models.ForeignKey(MasterDataProductModel, on_delete=models.CASCADE)  # Foreign key from MasterDataProductModel
    InhouseOrOutsource = models.CharField(max_length=250, default='Inhouse')  # Default value set to 'Inhouse'
    Supplier = models.ForeignKey(
        MasterDataSuppliersModel,
        related_name='supply_options',
        on_delete=models.CASCADE,
        null=True,  # Allow NULL values
        blank=True  # Allow blank values in forms
    )
    Site = models.ForeignKey(
        MasterDataPlantModel,
        related_name='supply_options',
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
    ComponentSeq = models.IntegerField(null=True, blank=True)
    Component = models.CharField(max_length=100, null=True, blank=True)
    ComponentUOM = models.CharField(max_length=100, null=True, blank=True)
    QtyPer = models.CharField(max_length=100, null=True, blank=True)
    EstimatedScrap = models.FloatField(null=True, blank=True)
    SalvageQtyPer = models.FloatField(null=True, blank=True)

    # Data source tracking fields
    is_user_created = models.BooleanField(default=False, help_text="True if this record was created manually by user")
    last_imported_from_epicor = models.DateTimeField(null=True, blank=True, help_text="Last time this record was updated from Epicor")
    user_modified_fields = models.JSONField(default=dict, help_text="Track which fields were modified by users")
    created_by_user = models.CharField(max_length=100, null=True, blank=True, help_text="Username who created this record manually")
    last_modified_by_user = models.CharField(max_length=100, null=True, blank=True, help_text="Username who last modified this record")
    last_user_modification_date = models.DateTimeField(null=True, blank=True, help_text="When user last modified this record")

    def __str__(self):
        return f"{self.Company} - {self.Plant} - {self.Parent}"
    
class MasterDataManuallyAssignProductionRequirement(models.Model):
    class Meta:
        unique_together = ("Product", "version")
    version = models.ForeignKey(scenarios, on_delete=models.CASCADE, null=True, blank=True)  # Add foreign key to scenarios
    Product = models.ForeignKey(MasterDataProductModel, on_delete=models.CASCADE, null=True, blank=True)  # Foreign key from MasterDataProductModel
    Site = models.ForeignKey(MasterDataPlantModel, on_delete=models.CASCADE, null=True, blank=True)  # Foreign key from MasterDataPlantModel

    def __str__(self):
        return f"{self.Product.Product} - {self.Site.SiteName}"
    
class ProductSiteCostModel(models.Model):
    version = models.ForeignKey(scenarios, on_delete=models.CASCADE)
    product = models.ForeignKey(MasterDataProductModel, on_delete=models.CASCADE)
    site = models.ForeignKey(MasterDataPlantModel, on_delete=models.CASCADE)
    cost_aud = models.FloatField(null=True, blank=True)
    cost_date = models.DateField(null=True, blank=True)
    revenue_cost_aud = models.FloatField(null=True, blank=True)  # <-- Add this line

    class Meta:
        unique_together = ('version', 'product', 'site')

    def __str__(self):
        return f"{self.version.version} - {self.product.Product} - {self.site.SiteName}"
    

class FixedPlantConversionModifiersModel(models.Model):
    version = models.ForeignKey(scenarios, on_delete=models.CASCADE)
    Product = models.ForeignKey(MasterDataProductModel, on_delete=models.CASCADE)
    Site = models.ForeignKey(MasterDataPlantModel, on_delete=models.CASCADE)
    GrossMargin = models.FloatField(default=0.0, null=True, blank=True)  # New field for GrossMargin
    ManHourCost = models.FloatField(default=0.0, null=True, blank=True)  # New field for ManHourCost
    ExternalMaterialComponents = models.FloatField(default=0.0, null=True, blank=True)  # New field for ExternalMaterialComponents
    FreightPercentage = models.FloatField(default=0.0, null=True, blank=True)  # New field for FreightPercentage
    MaterialCostPercentage = models.FloatField(default=0.0, null=True, blank=True)  # New field for MaterialCostPercentage
    CostPerHourAUD = models.FloatField(default=0.0, null=True, blank=True)  # New field for CostPerHourAUD
    CostPerSQMorKgAUD = models.FloatField(default=0.0, null=True, blank=True)  # New field for CostPerKgAUD
    
    class Meta:
        unique_together = ('version', 'Product', 'Site')
    
    def __str__(self):
        return f"{self.Product.Product} - {self.Site.SiteName} - GM:{self.GrossMargin}"

class RevenueToCogsConversionModel(models.Model):
    """Model to convert Revenue Forecast to COGS and Tonnes"""
    version = models.ForeignKey(scenarios, on_delete=models.CASCADE)
    Product = models.ForeignKey(MasterDataProductModel, on_delete=models.CASCADE)
    GrossMargin = models.FloatField(default=0.0, null=True, blank=True)  # Percentage
    InHouseProduction = models.FloatField(default=0.0, null=True, blank=True)  # Percentage
    CostAUDPerKG = models.FloatField(default=0.0, null=True, blank=True)  # Cost per KG

    class Meta:
        unique_together = ('version', 'Product')
    
    def __str__(self):
        return f"{self.Product.Product} - GM:{self.GrossMargin}% - Cost:{self.CostAUDPerKG}"

class SiteAllocationModel(models.Model):
    """Model to allocate converted revenue data to specific sites"""
    version = models.ForeignKey(scenarios, on_delete=models.CASCADE)
    Product = models.ForeignKey(MasterDataProductModel, on_delete=models.CASCADE)
    Site = models.ForeignKey(MasterDataPlantModel, on_delete=models.CASCADE)
    AllocationPercentage = models.FloatField(default=0.0, null=True, blank=True)  # Percentage of total to allocate to this site

    class Meta:
        unique_together = ('version', 'Product', 'Site')
    
    def __str__(self):
        return f"{self.Product.Product} - {self.Site.SiteName} - {self.AllocationPercentage}%"

class ProductionAllocationModel(models.Model):
    """
    🟢 INPUT MODEL - TRACKED FOR CHANGES
    Model to store production allocation percentages by product, site, and month.
    Used for percentage-based work transfer allocation in production planning.
    This contains user input data for allocation decisions.
    """
    version = models.ForeignKey(scenarios, on_delete=models.CASCADE, help_text="Scenario version")
    product = models.ForeignKey(MasterDataProductModel, on_delete=models.CASCADE, help_text="Product to allocate")
    site = models.ForeignKey(MasterDataPlantModel, on_delete=models.CASCADE, help_text="Site to allocate to")
    month_year = models.CharField(max_length=10, help_text="Format: 'Jul-25', 'Aug-25', etc.")
    allocation_percentage = models.FloatField(default=0.0, help_text="Percentage of total production (0-100)")
    created_date = models.DateTimeField(auto_now_add=True, help_text="Timestamp when record was created")
    modified_date = models.DateTimeField(auto_now=True, help_text="Timestamp when record was last modified")
    
    class Meta:
        unique_together = ['version', 'product', 'site', 'month_year']
        verbose_name = "Production Allocation"
        verbose_name_plural = "Production Allocations"
    
    def __str__(self):
        return f"{self.version.version} - {self.product.Product} - {self.site.SiteName} - {self.month_year}: {self.allocation_percentage}%"
    
class MasterDataEpicorMethodOfManufacturingModel(models.Model):
    Company = models.CharField(max_length=50, null=True, blank=True)
    Plant = models.CharField(max_length=50, null=True, blank=True)
    ProductKey = models.CharField(max_length=100, null=True, blank=True)
    SiteName = models.CharField(max_length=100, null=True, blank=True)
    OperationSequence = models.IntegerField(null=True, blank=True)
    OperationDesc = models.CharField(max_length=255, null=True, blank=True)
    WorkCentre = models.CharField(max_length=100, null=True, blank=True)
    
    # Data source tracking fields
    is_user_created = models.BooleanField(default=False, help_text="True if this record was created manually by user")
    last_imported_from_epicor = models.DateTimeField(null=True, blank=True, help_text="Last time this record was updated from Epicor")
    user_modified_fields = models.JSONField(default=dict, help_text="Track which fields were modified by users")
    created_by_user = models.CharField(max_length=100, null=True, blank=True, help_text="Username who created this record manually")
    last_modified_by_user = models.CharField(max_length=100, null=True, blank=True, help_text="Username who last modified this record")
    last_user_modification_date = models.DateTimeField(null=True, blank=True, help_text="When user last modified this record")
    
    class Meta:
        db_table = 'website_masterdataepicormethodofmanufacturingmodel'
        unique_together = ('Plant', 'ProductKey', 'OperationSequence')  # Prevent duplicates
        
    def __str__(self):
        return f"{self.ProductKey} - {self.SiteName} - {self.OperationSequence}"
    

class MasterDataSafetyStocks(models.Model):
    version = models.ForeignKey(scenarios, on_delete=models.CASCADE)
    Plant = models.CharField(max_length=10, blank=True, null=True)
    PartNum = models.CharField(max_length=50, blank=True, null=True)
    MinimumQty = models.DecimalField(max_digits=15, decimal_places=5, blank=True, null=True)
    SafetyQty = models.DecimalField(max_digits=15, decimal_places=5, blank=True, null=True)
    
    # Timestamp fields for change tracking
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        db_table = 'MasterDataSafetyStocks'
        unique_together = ('version', 'Plant', 'PartNum')
    
    def __str__(self):
        return f"{self.Plant} - {self.PartNum}"

class ScenarioOptimizationState(models.Model):
    """Track optimization state for scenarios to prevent multiple optimizations"""
    version = models.OneToOneField(scenarios, on_delete=models.CASCADE, primary_key=True)
    auto_optimization_applied = models.BooleanField(default=False)
    last_optimization_date = models.DateTimeField(null=True, blank=True)
    last_reset_date = models.DateTimeField(null=True, blank=True)
    
    def __str__(self):
        return f"{self.version.version} - Optimized: {self.auto_optimization_applied}"


# ========== CALCULATED/OUTPUT MODELS - EXCLUDED FROM CHANGE TRACKING ==========
# 🔴 These models contain calculated/cached data, not user input
# 🔴 They are populated BY the calculation process, not input TO it

class CachedControlTowerData(models.Model):
    """
    🔴 CALCULATED/OUTPUT MODEL - EXCLUDED FROM CHANGE TRACKING
    Cache control tower calculations to avoid expensive real-time computation
    """
    version = models.OneToOneField(scenarios, on_delete=models.CASCADE, primary_key=True)
    combined_demand_plan = models.JSONField()  # Store the demand plan data
    poured_data = models.JSONField()          # Store the poured data  
    pour_plan = models.JSONField()            # Store the pour plan data
    calculation_date = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"Control Tower Data - {self.version.version}"

class CachedFoundryData(models.Model):
    """Cache foundry chart data for each foundry site"""
    version = models.ForeignKey(scenarios, on_delete=models.CASCADE)
    foundry_site = models.CharField(max_length=10)  # MTJ1, COI2, XUZ1, etc.
    chart_data = models.JSONField()         # Store the chart data
    top_products = models.JSONField()       # Store top products data
    monthly_pour_plan = models.JSONField()  # Store monthly pour plan
    calculation_date = models.DateTimeField(auto_now=True)
    
    class Meta:
        unique_together = ('version', 'foundry_site')
    
    def __str__(self):
        return f"Foundry Data - {self.version.version} - {self.foundry_site}"

class CachedForecastData(models.Model):
    """Cache forecast chart data by aggregation type"""
    version = models.ForeignKey(scenarios, on_delete=models.CASCADE)
    data_type = models.CharField(max_length=50)  # parent_product_group, product_group, region, customer, data_source
    chart_data = models.JSONField()              # Store the chart data
    calculation_date = models.DateTimeField(auto_now=True)
    
    class Meta:
        unique_together = ('version', 'data_type')
    
    def __str__(self):
        return f"Forecast Data - {self.version.version} - {self.data_type}"

class CachedInventoryData(models.Model):
    """Cache complex inventory calculations including opening stock"""
    version = models.OneToOneField(scenarios, on_delete=models.CASCADE, primary_key=True)
    inventory_months = models.JSONField()               # Monthly data
    inventory_cogs = models.JSONField()                 # COGS data
    inventory_revenue = models.JSONField()              # Revenue data  
    production_aud = models.JSONField()                 # Production AUD data
    production_cogs_group_chart = models.JSONField()   # Production COGS by group
    top_products_by_group_month = models.JSONField()   # Top products data
    parent_product_groups = models.JSONField()         # Product groups list
    cogs_data_by_group = models.JSONField()            # COGS data by group
    calculation_date = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"Inventory Data - {self.version.version}"

class CachedSupplierData(models.Model):
    """Cache supplier production data"""
    version = models.ForeignKey(scenarios, on_delete=models.CASCADE)
    supplier_code = models.CharField(max_length=20)    # HBZJBF02, etc.
    chart_data = models.JSONField()                    # Production chart data
    top_products = models.JSONField()                  # Top products data
    calculation_date = models.DateTimeField(auto_now=True)
    
    class Meta:
        unique_together = ('version', 'supplier_code')
    
    def __str__(self):
        return f"Supplier Data - {self.version.version} - {self.supplier_code}"

class CachedDetailedInventoryData(models.Model):
    """Cache detailed inventory view data (usually empty until searched)"""
    version = models.OneToOneField(scenarios, on_delete=models.CASCADE, primary_key=True)
    inventory_data = models.JSONField(default=list)    # Detailed inventory data
    production_data = models.JSONField(default=list)   # Detailed production data
    calculation_date = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"Detailed Inventory Data - {self.version.version}"


# ===== AGGREGATED CHART DATA MODELS =====
# NOTE: Cache models removed - replaced with direct polars queries for 218x-720x performance improvement
# Previous models: AggregatedForecastChartData, AggregatedFoundryChartData, AggregatedInventoryChartData
# These pre-calculated cache tables took 12+ minutes to populate 
# Now replaced with 1-3 second real-time polars queries

class AggregatedFinancialChartData(models.Model):
    """Pre-calculated financial chart data by groups for Cost Analysis"""
    version = models.OneToOneField(scenarios, on_delete=models.CASCADE, primary_key=True)
    
    # Financial data by group
    financial_by_group = models.JSONField(default=dict)    # {group: {revenue: [...], cogs: [...], production: [...], inventory_projection: [...]}}
    parent_product_groups = models.JSONField(default=list) # List of parent product groups for filter dropdown
    
    # Summary metrics for each financial line
    total_revenue_aud = models.FloatField(default=0)
    total_cogs_aud = models.FloatField(default=0) 
    total_production_aud = models.FloatField(default=0)
    total_inventory_projection = models.FloatField(default=0)
    
    # Chart data structure for frontend
    revenue_chart_data = models.JSONField(default=dict)    # Chart.js format for Revenue by group
    cogs_chart_data = models.JSONField(default=dict)       # Chart.js format for COGS by group  
    production_chart_data = models.JSONField(default=dict) # Chart.js format for Production by group
    inventory_projection_data = models.JSONField(default=dict) # Chart.js format for Inventory Projection by group
    
    # Combined 4-line chart data (company totals)
    combined_financial_data = models.JSONField(default=dict) # Chart.js format for 4-line chart
    
    calculation_date = models.DateTimeField(auto_now=True)
    
    def __str__(self):
        return f"Financial Chart Data - {self.version.version}"


class InventoryProjectionModel(models.Model):
    """Model to store monthly inventory projections by parent product group only"""
    version = models.ForeignKey(scenarios, on_delete=models.CASCADE, related_name='inventory_projections')
    month = models.DateField(help_text="Month for this projection")
    parent_product_group = models.CharField(max_length=250, help_text="Parent Product Group Description")
    production_aud = models.FloatField(default=0, help_text="Production value in AUD")
    cogs_aud = models.FloatField(default=0, help_text="Cost of Goods Sold in AUD")
    revenue_aud = models.FloatField(default=0, help_text="Revenue in AUD")
    opening_inventory_aud = models.FloatField(default=0, help_text="Opening inventory value in AUD")
    closing_inventory_aud = models.FloatField(default=0, help_text="Closing inventory value in AUD")
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        unique_together = [['version', 'month', 'parent_product_group']]
        ordering = ['version', 'parent_product_group', 'month']
        indexes = [
            models.Index(fields=['version', 'parent_product_group']),
            models.Index(fields=['version', 'month']),
        ]
    
    def __str__(self):
        return f"{self.version.version} - {self.parent_product_group} - {self.month.strftime('%Y-%m')}"


class OpeningInventorySnapshot(models.Model):
    """
    Stores opening inventory data snapshots to avoid expensive SQL Server queries.
    Replaces 400+ second external database queries with < 5 second local queries.
    Gets populated automatically when upload_on_hand_stock is executed.
    
    IMPORTANT: Snapshots are SHARED across scenarios by snapshot_date.
    Multiple scenarios can use the same snapshot date without regenerating data.
    """
    
    # NO scenario field - snapshots are shared globally by date
    
    # Snapshot metadata
    snapshot_date = models.DateField(db_index=True, help_text="Date of inventory snapshot (shared across scenarios)")
    created_at = models.DateTimeField(auto_now_add=True, db_index=True, help_text="When this snapshot was created")
    updated_at = models.DateTimeField(auto_now=True, help_text="When this snapshot was last updated")
    
    # Inventory data per product group
    parent_product_group = models.CharField(max_length=255, db_index=True, help_text="Parent product group description")
    inventory_value_aud = models.DecimalField(max_digits=20, decimal_places=2, help_text="Opening inventory value in AUD")
    
    # Additional metadata for auditing and debugging
    source_system = models.CharField(max_length=50, default='PowerBI', help_text="Source system (PowerBI, Manual, etc)")
    data_freshness_hours = models.IntegerField(default=0, help_text="How old the source data was when captured")
    refresh_reason = models.CharField(max_length=100, default='auto_upload', help_text="Why this snapshot was refreshed")
    created_by_user = models.CharField(max_length=100, null=True, blank=True, help_text="User who triggered the refresh")
    scenarios_using_this_snapshot = models.JSONField(default=list, help_text="List of scenario versions using this snapshot")
    
    class Meta:
        db_table = 'website_openinginventorysnapshot'
        verbose_name = "Opening Inventory Snapshot"
        verbose_name_plural = "Opening Inventory Snapshots"
        
        # Compound indexes for fast lookups
        indexes = [
            models.Index(fields=['snapshot_date'], name='opening_inv_date_idx'),
            models.Index(fields=['parent_product_group'], name='opening_inv_group_idx'),
            models.Index(fields=['created_at'], name='opening_inv_created_idx'),
            models.Index(fields=['snapshot_date', 'parent_product_group'], name='opening_inv_date_group_idx'),
        ]
        
        # Prevent duplicate entries per date + product group
        unique_together = [
            ('snapshot_date', 'parent_product_group')
        ]
        
        ordering = ['snapshot_date', 'parent_product_group']
    
    def __str__(self):
        scenario_count = len(self.scenarios_using_this_snapshot) if self.scenarios_using_this_snapshot else 0
        return f"{self.snapshot_date} - {self.parent_product_group} (used by {scenario_count} scenarios)"
    
    @classmethod
    def get_or_create_snapshot(cls, scenario, snapshot_date, force_refresh=False, user=None, reason='auto'):
        """
        Get cached snapshot data or create it if missing/stale.
        Snapshots are shared across scenarios - no duplication!
        
        Args:
            scenario: Scenario object (used only for tracking and fallback)
            snapshot_date: Date of inventory snapshot
            force_refresh: Force refresh even if data exists
            user: User who triggered the refresh
            reason: Reason for the refresh
            
        Returns:
            dict: {parent_group: inventory_value}
        """
        from datetime import timedelta
        from django.utils import timezone
        
        # Track which scenario is using this snapshot
        scenario_version = scenario.version
        
        # Check if we have fresh data for this date (within last 24 hours)
        cutoff_time = timezone.now() - timedelta(hours=24)
        
        existing_data = cls.objects.filter(
            snapshot_date=snapshot_date,
            created_at__gte=cutoff_time
        )
        
        if existing_data.exists() and not force_refresh:
            print(f"📊 Using SHARED inventory snapshot for date {snapshot_date} (used by multiple scenarios)")
            
            # Update scenarios_using_this_snapshot tracking
            cls._track_scenario_usage(snapshot_date, scenario_version)
            
            # Return cached data as dictionary
            return dict(
                existing_data.values_list('parent_product_group', 'inventory_value_aud')
            )
        
        # Data is stale or missing - refresh from SQL Server
        print(f"📊 Creating SHARED inventory snapshot for date {snapshot_date} (Reason: {reason})")
        print(f"    This snapshot will be reusable by ALL scenarios with the same date!")
        
        fresh_data = cls._fetch_from_sql_server(scenario, snapshot_date)
        
        if fresh_data:
            # Clear old data for this date (not scenario-specific)
            deleted_count = cls.objects.filter(snapshot_date=snapshot_date).delete()
            if deleted_count[0] > 0:
                print(f"📊 Deleted {deleted_count[0]} old snapshot records for date {snapshot_date}")
            
            # Bulk create new snapshot records
            snapshot_records = [
                cls(
                    snapshot_date=snapshot_date,
                    parent_product_group=group,
                    inventory_value_aud=value,
                    refresh_reason=reason,
                    created_by_user=user.username if user else None,
                    scenarios_using_this_snapshot=[scenario_version]  # Start with current scenario
                )
                for group, value in fresh_data.items()
            ]
            
            cls.objects.bulk_create(snapshot_records, batch_size=1000)
            print(f"📊 Created {len(snapshot_records)} SHARED inventory snapshot records for {snapshot_date}")
            
            return fresh_data
        else:
            print(f"⚠️ Could not fetch fresh inventory data from SQL Server for {snapshot_date}")
            return {}
    
    @classmethod
    def _track_scenario_usage(cls, snapshot_date, scenario_version):
        """
        Track which scenarios are using this snapshot date
        """
        try:
            # Get all records for this snapshot date
            snapshot_records = cls.objects.filter(snapshot_date=snapshot_date)
            
            for record in snapshot_records:
                current_scenarios = record.scenarios_using_this_snapshot or []
                if scenario_version not in current_scenarios:
                    current_scenarios.append(scenario_version)
                    record.scenarios_using_this_snapshot = current_scenarios
                    record.save(update_fields=['scenarios_using_this_snapshot'])
            
            print(f"📊 Tracked scenario {scenario_version} as using snapshot {snapshot_date}")
            
        except Exception as e:
            print(f"⚠️ Could not track scenario usage: {e}")
    
    @classmethod
    def _fetch_from_sql_server(cls, scenario, snapshot_date):
        """
        Fetch fresh data from SQL Server PowerBI (expensive operation)
        This queries PowerBI.Inventory Monthly History and aggregates by parent product group
        """
        try:
            from django.db import connection
            from sqlalchemy import create_engine, text
            import pandas as pd
            
            print(f"🔄 Fetching fresh inventory data from SQL Server for {snapshot_date}...")
            
            # Convert snapshot_date to skReportDateId format (YYYYMMDD)
            snapshot_date_id = snapshot_date.strftime('%Y%m%d')
            
            # Database connection string (same pattern as used in views.py)
            Server = 'bknew-sql02'
            Database = 'Bradken_Data_Warehouse'
            Driver = 'ODBC Driver 17 for SQL Server'
            Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
            
            # Create SQLAlchemy engine
            engine = create_engine(Database_Con)
            
            with engine.connect() as conn:
                # SQL query to get parent product group inventory aggregation
                parent_group_sql = f"""
                    SELECT 
                        Products.ParentProductGroupDescription,
                        SUM(ISNULL(Inventory.StockOnHandValueAUD, 0)) as inventory_value_aud
                    FROM PowerBI.[Inventory Daily History] AS Inventory
                    LEFT JOIN PowerBI.Products AS Products 
                        ON Inventory.skProductId = Products.skProductId
                    WHERE Inventory.skReportDateId = {snapshot_date_id}
                      AND (Products.RowEndDate IS NULL OR Products.RowEndDate IS NULL)
                      AND Products.ParentProductGroupDescription IS NOT NULL
                      AND Products.ParentProductGroupDescription != ''
                    GROUP BY Products.ParentProductGroupDescription
                    HAVING SUM(ISNULL(Inventory.StockOnHandValueAUD, 0)) > 0
                    ORDER BY Products.ParentProductGroupDescription
                """
                
                print(f"� SQL Query: Aggregating inventory by parent product group for date {snapshot_date_id}")
                
                # Execute query
                df = pd.read_sql(parent_group_sql, conn)
                
                if len(df) == 0:
                    print(f"⚠️ No inventory data found in PowerBI for date {snapshot_date_id}")
                    return {}
                
                # Convert to dictionary format
                result = dict(zip(
                    df['ParentProductGroupDescription'].tolist(),
                    df['inventory_value_aud'].tolist()
                ))
                
                print(f"✅ SQL Server query completed: {len(result)} parent product groups, total value: ${sum(result.values()):,.2f}")
                return result
                
        except Exception as e:
            print(f"❌ Error fetching from SQL Server: {e}")
            import traceback
            print(f"Full traceback: {traceback.format_exc()}")
            return {}
    
    @classmethod
    def clear_stale_snapshots(cls, days_old=7):
        """
        Clean up old snapshots to prevent database bloat
        """
        from datetime import timedelta
        from django.utils import timezone
        
        cutoff_date = timezone.now() - timedelta(days=days_old)
        deleted_count = cls.objects.filter(created_at__lt=cutoff_date).delete()
        
        if deleted_count[0] > 0:
            print(f"🧹 Cleaned up {deleted_count[0]} old inventory snapshots older than {days_old} days")
        
        return deleted_count[0]
    
    @classmethod
    def get_snapshot_statistics(cls):
        """
        Get statistics about snapshot usage across scenarios
        """
        from django.db.models import Count, Max, Min
        
        stats = cls.objects.aggregate(
            total_snapshots=Count('snapshot_date', distinct=True),
            total_records=Count('id'),
            earliest_snapshot=Min('snapshot_date'),
            latest_snapshot=Max('snapshot_date'),
            oldest_created=Min('created_at'),
            newest_created=Max('created_at')
        )
        
        # Get scenarios per snapshot
        snapshot_usage = {}
        for record in cls.objects.values('snapshot_date', 'scenarios_using_this_snapshot'):
            date = record['snapshot_date']
            scenarios = record['scenarios_using_this_snapshot'] or []
            if date not in snapshot_usage:
                snapshot_usage[date] = set()
            snapshot_usage[date].update(scenarios)
        
        stats['snapshots_with_usage'] = {
            str(date): list(scenarios) for date, scenarios in snapshot_usage.items()
        }
        
        return stats


class MonthlyPouredDataModel(models.Model):
    r"""
    Store monthly poured data locally for fast access
    Populated from PowerBI when inventory snapshot is uploaded via upload_on_hand_stock
    Replaces slow external PowerBI queries with fast local database access
    
    🚨 CRITICAL: This model's populate_for_scenario() method is NOT called automatically!
    ❌ ISSUE IDENTIFIED: The populate_for_scenario() method exists but is never invoked from upload_on_hand_stock
    ✅ TODO: Add MonthlyPouredDataModel.populate_for_scenario(scenario, snapshot_date) call to upload_on_hand_stock function
    
    ⚠️ DEVELOPER REMINDER: When creating analysis/debug files for this model:
    - ALL analysis files must go in: C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR\temporary\
    - NO files should be created in project root or SPR directory
    - Only Django app files belong in SPR/website/ directory
    """
    version = models.ForeignKey(scenarios, on_delete=models.CASCADE)
    site_name = models.CharField(max_length=250)
    fiscal_year = models.CharField(max_length=10)  # FY24, FY25, etc.
    year = models.IntegerField()
    month = models.IntegerField() 
    month_year_display = models.CharField(max_length=20)  # "Jan 2025", "Feb 2025", etc.
    monthly_tonnes = models.FloatField(default=0)
    record_count = models.IntegerField(default=0)
    min_date = models.DateField(null=True, blank=True)
    max_date = models.DateField(null=True, blank=True)
    data_fetched_date = models.DateTimeField(auto_now_add=True)
    snapshot_date = models.DateField(help_text="Inventory snapshot date this data was fetched for")
    
    class Meta:
        unique_together = ['version', 'site_name', 'fiscal_year', 'year', 'month']
        indexes = [
            models.Index(fields=['version', 'site_name', 'fiscal_year']),
            models.Index(fields=['version', 'site_name', 'year', 'month']),
            models.Index(fields=['snapshot_date']),
        ]
    
    def __str__(self):
        return f"{self.version.version} - {self.site_name} {self.month_year_display}: {self.monthly_tonnes} tonnes"
    
    @classmethod
    def populate_for_scenario(cls, scenario, snapshot_date):
        """
        Populate monthly poured data for all sites when inventory is uploaded
        """
        from datetime import date
        from sqlalchemy import create_engine, text
        from calendar import monthrange
        
        print(f"🔄 Populating monthly poured data for scenario {scenario.version} with snapshot date {snapshot_date}...")
        
        # Define fiscal year ranges
        fy_ranges = {
            "FY24": (date(2024, 4, 1), date(2025, 3, 31)),
            "FY25": (date(2025, 4, 1), date(2026, 3, 31)),
            "FY26": (date(2026, 4, 1), date(2027, 3, 31)),
            "FY27": (date(2027, 4, 1), date(2028, 3, 31)),
        }
        
        # Site mapping
        sites = ["MTJ1", "COI2", "XUZ1", "MER1", "WUN1", "WOD1", "CHI1"]
        
        # Database connection
        Server = 'bknew-sql02'
        Database = 'Bradken_Data_Warehouse'
        Driver = 'ODBC Driver 17 for SQL Server'
        Database_Con = f'mssql+pyodbc://@{Server}/{Database}?driver={Driver}'
        
        # Clear existing data for this scenario
        cls.objects.filter(version=scenario).delete()
        
        records_to_create = []
        
        try:
            engine = create_engine(Database_Con)
            
            with engine.connect() as connection:
                for site in sites:
                    for fy, (fy_start, fy_end) in fy_ranges.items():
                        # Limit data to snapshot date
                        filter_end_date = min(snapshot_date, fy_end)
                        
                        if snapshot_date < fy_start:
                            continue
                            
                        print(f"   Fetching data for {site} {fy}...")
                        
                        query = text("""
                            SELECT 
                                YEAR(hp.TapTime) as TapYear,
                                MONTH(hp.TapTime) as TapMonth,
                                COUNT(*) as RecordCount,
                                SUM(hp.CastQty * p.DressMass / 1000) as MonthlyTonnes,
                                MIN(hp.TapTime) as MinDate,
                                MAX(hp.TapTime) as MaxDate
                            FROM PowerBI.HeatProducts hp
                            INNER JOIN PowerBI.Products p ON hp.skProductId = p.skProductId
                            INNER JOIN PowerBI.Site s ON hp.SkSiteId = s.skSiteId
                            WHERE s.SiteName = :site_name
                                AND hp.TapTime >= :start_date
                                AND hp.TapTime <= :end_date
                                AND hp.TapTime IS NOT NULL 
                                AND p.DressMass IS NOT NULL
                                AND hp.CastQty IS NOT NULL
                            GROUP BY YEAR(hp.TapTime), MONTH(hp.TapTime)
                            ORDER BY TapYear, TapMonth
                        """)
                        
                        result = connection.execute(query, {
                            'site_name': site,
                            'start_date': fy_start,
                            'end_date': filter_end_date
                        })
                        
                        for row in result:
                            month_date = date(row.TapYear, row.TapMonth, 1)
                            month_str = month_date.strftime('%b %Y')
                            
                            records_to_create.append(cls(
                                version=scenario,
                                site_name=site,
                                fiscal_year=fy,
                                year=row.TapYear,
                                month=row.TapMonth,
                                month_year_display=month_str,
                                monthly_tonnes=round(row.MonthlyTonnes or 0),
                                record_count=row.RecordCount,
                                min_date=row.MinDate.date() if hasattr(row.MinDate, 'date') else row.MinDate,
                                max_date=row.MaxDate.date() if hasattr(row.MaxDate, 'date') else row.MaxDate,
                                snapshot_date=snapshot_date
                            ))
        
            # Bulk create all records
            if records_to_create:
                cls.objects.bulk_create(records_to_create, batch_size=1000)
                print(f"✅ Created {len(records_to_create)} monthly poured data records")
            else:
                print("⚠️ No monthly poured data records created")
                
        except Exception as e:
            print(f"❌ Error populating monthly poured data: {e}")
            import traceback
            print(f"Full traceback: {traceback.format_exc()}")
    
    @classmethod
    def get_monthly_data_for_site_and_fy(cls, scenario, site, fy):
        """
        Get monthly poured data for a specific site and fiscal year from local database (FAST!)
        """
        monthly_data = {}
        
        records = cls.objects.filter(
            version=scenario,
            site_name=site,
            fiscal_year=fy
        ).order_by('year', 'month')
        
        for record in records:
            monthly_data[record.month_year_display] = record.monthly_tonnes
        
        return monthly_data