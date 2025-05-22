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
        fields = ['Forecast_region']  # Include relevant fields

from django import forms
from .models import MasterDataFreightModel

class MasterDataFreightForm(forms.ModelForm):
    class Meta:
        model = MasterDataFreightModel
        fields = ['ForecastRegion', 'ManufacturingSite', 'PlantToDomesticPortDays', 'OceanFreightDays', 'PortToCustomerDays']

from django import forms
from .models import MasterDataIncotTermTypesModel

class MasterDataIncotTermTypesForm(forms.ModelForm):
    class Meta:
        model = MasterDataIncotTermTypesModel
        fields = ['version', 'IncoTerm', 'IncoTermCaregory']

from django import forms
from .models import MasterDataIncotTermTypesModel

class MasterDataIncotTermTypesForm(forms.ModelForm):
    class Meta:
        model = MasterDataIncotTermTypesModel
        exclude = ['version']  # Exclude the version field        

from django import forms
from .models import MasterdataIncoTermsModel

class MasterdataIncoTermsForm(forms.ModelForm):
    class Meta:
        model = MasterdataIncoTermsModel
        exclude = ['version']  # Exclude the version field

from django import forms
from .models import MasterDataPlan
from datetime import datetime, timedelta, date

class MasterDataPlanForm(forms.ModelForm):
    CalendarDays = forms.CharField(disabled=True, required=False)  # Readonly field
    AvailableDays = forms.CharField(disabled=True, required=False)  # Readonly field
    PlanDressMass = forms.CharField(disabled=True, required=False)  # Readonly field

    Month = forms.ChoiceField(required=False)  # Define the field without static choices

    class Meta:
        model = MasterDataPlan
        fields = [
            'id',  # Include the id field
            'Foundry', 'Month', 'CalendarDays', 'Yield', 'WasterPercentage',
            'PlannedMaintenanceDays', 'PublicHolidays', 'Weekends',
            'OtherNonPouringDays', 'AvailableDays', 'heatsperdays',
            'TonsPerHeat', 'PlanDressMass'
        ]
        widgets = {
            'id': forms.HiddenInput(),  # Render the id field as a hidden input
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        three_months_ago = date.today() - timedelta(days=90)

        # Generate the list of months starting from 3 months ago
        month_choices = [
            (
                (three_months_ago.replace(day=1) + timedelta(days=30 * i)).strftime('%Y-%m-%d'),  # Format as YYYY-MM-DD
                (three_months_ago.replace(day=1) + timedelta(days=30 * i)).strftime('%B %Y')
            )
            for i in range(0, 120)  # Generate up to 10 years of future months
        ]

        if self.instance and self.instance.pk:
            # Existing record: Include the current value in the dropdown
            current_month = self.instance.Month.strftime('%Y-%m-%d')  # Format as YYYY-MM-DD
            if (current_month, self.instance.Month.strftime('%B %Y')) not in month_choices:
                month_choices.insert(0, (current_month, self.instance.Month.strftime('%B %Y')))
        else:
            # Extra form: Set the default value for Month to blank
            self.fields['Month'].initial = None  # Explicitly set to None for blank

        # Set the choices for the Month field
        self.fields['Month'].choices = [('', '---------')] + month_choices  # Add a blank option

        # Populate readonly fields with calculated values
        if self.instance:
            self.fields['CalendarDays'].initial = self.instance.CalendarDays
            self.fields['AvailableDays'].initial = self.instance.AvailableDays
            self.fields['PlanDressMass'].initial = self.instance.PlanDressMass
    
    

from django import forms
from .models import MasterDataCapacityModel

class MasterDataCapacityForm(forms.ModelForm):
    class Meta:
        model = MasterDataCapacityModel
        fields = '__all__'

from django import forms
from .models import MasterDataPlantModel

class PlantForm(forms.ModelForm):
    class Meta:
        model = MasterDataPlantModel
        fields = ['SiteName', 'Company', 'Location', 'SiteType']