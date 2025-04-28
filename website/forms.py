from django import forms
from .models import SMART_Forecast_Model, scenarios
from .models import MasterDataProductModel, MasterDataProductPictures, MasterDataPlantModel
from django.forms import modelformset_factory


class UploadFileForm(forms.Form):
    file = forms.FileField()

class ScenarioForm(forms.ModelForm):
    class Meta:
        model = scenarios
        fields = ['version', 'scenario_description', 'open_to_update', 'visible_to_users', 'approval1', 'approval2', 'approval3']
        widgets = {
            'scenario_description': forms.Textarea(attrs={'rows': 3, 'style': 'width:100%;'}),
        }

class ProductForm(forms.ModelForm):
    class Meta:
        model = MasterDataProductModel
        fields = '__all__'

class ProductPictureForm(forms.ModelForm):
    class Meta:
        model = MasterDataProductPictures
        fields = ['Image']

    def save(self, commit=True):
        instance = super(ProductPictureForm, self).save(commit=False)
        if commit:
            instance.save()
        return instance

class MasterDataPlantsForm(forms.ModelForm):
    class Meta:
        model = MasterDataPlantModel
        fields = '__all__'

from django import forms

class UploadFileForm(forms.Form):
    file = forms.FileField()

class SMARTForecastForm(forms.ModelForm):
    class Meta:
        model = SMART_Forecast_Model
        fields = ['version', 'id', 'Data_Source', 'Forecast_Region', 'Product_Group', 'Product', 'ProductFamilyDescription', 'Customer_code', 'Location', 'Forecasted_Weight_Curr', 'PriceAUD', 'DP_Cycle', 'Period_AU', 'Qty']
        widgets = {
            'version': forms.HiddenInput(),
            'id': forms.HiddenInput(),
        }

SMARTForecastFormSet = modelformset_factory(SMART_Forecast_Model, form=SMARTForecastForm, extra=0)



SMARTForecastFormSet = modelformset_factory(SMART_Forecast_Model, form=SMARTForecastForm, extra=0)

class ForecastFilterForm(forms.Form):
    forecast_region = forms.CharField(required=False)
    product_group = forms.CharField(required=False)
    customer_code = forms.CharField(required=False)

from django import forms
from .models import MasterDataForecastRegionModel

class ForecastRegionForm(forms.ModelForm):
    class Meta:
        model = MasterDataForecastRegionModel
        fields = ['Forecast_region']

from django import forms
from .models import MasterDataFreightModel

class MasterDataFreightForm(forms.ModelForm):
    class Meta:
        model = MasterDataFreightModel
        fields = ['ForecastRegion', 'ManufacturingSite', 'PlantToDomesticPortDays', 'OceanFreightDays', 'PortToCustomerDays']