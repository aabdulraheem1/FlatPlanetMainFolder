from  import_export import resources
from .models import SMART_Forecast_Model

class SMASMART_Forecast_Model(resources.ModelResource):
    class meta:
        model = SMART_Forecast_Model
