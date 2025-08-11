from django.contrib import admin
from . models import SMART_Forecast_Model
from . models import Product_Model, scenarios, MasterDataOrderBook
from import_export.admin import ImportExportModelAdmin
from . models import *


# Register your models here.


admin.site.register(SMART_Forecast_Model)

admin.site.register(Product_Model)
admin.site.register(scenarios)
admin.site.register(MasterDataProductAttributesModel)
admin.site.register(MasterDataSalesAllocationToPlantModel)
admin.site.register(MasterDataPlantModel)
admin.site.register(MasterDataCapacityModel)
admin.site.register(MasterDataCommentModel)
admin.site.register(MasterDataFreightModel)
admin.site.register(MasterDataHistoryOfProductionModel)
admin.site.register(MasterdataIncoTermsModel)
admin.site.register(MasterDataIncotTermTypesModel)
admin.site.register(MasterDataLeadTimesModel)
admin.site.register(MasterDataOrderBook)
admin.site.register(MasterDataScheduleModel)
admin.site.register(MasterDataSalesModel)
admin.site.register(MasterDataSKUTransferModel)
admin.site.register(MasterDataPlan)
admin.site.register(MasterDataForecastRegionModel)



