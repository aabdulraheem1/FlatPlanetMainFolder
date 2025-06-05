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

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['Image'].required = False  # <-- This makes it optional

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

from .models import MasterDataPlantModel

class MasterDataPlanForm(forms.ModelForm):
    CalendarDays = forms.CharField(disabled=True, required=False)
    AvailableDays = forms.CharField(disabled=True, required=False)
    PlanDressMass = forms.CharField(disabled=True, required=False)

    Month = forms.ChoiceField(required=False)

    # Only allow these plant codes
    FOUNDRY_CODES = ['COI2', 'MTJ1', 'XUZ1', 'WOD1', 'WUN1', 'MER1']

    Foundry = forms.ModelChoiceField(
        queryset=MasterDataPlantModel.objects.filter(SiteName__in=FOUNDRY_CODES),
        required=False
    )

    class Meta:
        model = MasterDataPlan
        fields = [
            'id', 'Foundry', 'Month', 'CalendarDays', 'Yield', 'WasterPercentage',
            'PlannedMaintenanceDays', 'PublicHolidays', 'Weekends',
            'OtherNonPouringDays', 'AvailableDays', 'heatsperdays',
            'TonsPerHeat', 'PlanDressMass'
        ]
        widgets = {
            'id': forms.HiddenInput(),
        }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        three_months_ago = date.today() - timedelta(days=90)

        # Generate the list of months starting from 3 months ago
        month_choices = [
            (
                (three_months_ago.replace(day=1) + timedelta(days=30 * i)).strftime('%Y-%m-%d'),
                (three_months_ago.replace(day=1) + timedelta(days=30 * i)).strftime('%B %Y')
            )
            for i in range(0, 120)
        ]

        # Always include the current value if it's not in the list
        if self.instance and self.instance.pk and self.instance.Month:
            current_month = self.instance.Month
            # Try to handle both date and string types
            try:
                current_month_str = current_month.strftime('%Y-%m-%d')
                current_month_label = current_month.strftime('%B %Y')
            except AttributeError:
                current_month_str = str(current_month)
                try:
                    current_month_label = date.fromisoformat(current_month_str).strftime('%B %Y')
                except Exception:
                    current_month_label = current_month_str
            # Insert at the top if not present
            if (current_month_str, current_month_label) not in month_choices:
                month_choices.insert(0, (current_month_str, current_month_label))
        else:
            self.fields['Month'].initial = None  # Explicitly set to None for blank

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

# forms.py

from django import forms
from .models import MasterDataManuallyAssignProductionRequirement

from django import forms

from datetime import date, timedelta

class ManuallyAssignProductionRequirementForm(forms.Form):
    Product = forms.CharField(label='Product', widget=forms.TextInput())
    Site = forms.CharField(label='Site', widget=forms.TextInput())
    # Only allow first day of each month for the next 24 months
    def _first_days():
        today = date.today().replace(day=1)
        return [
            ( (today + timedelta(days=32*i)).replace(day=1), (today + timedelta(days=32*i)).replace(day=1).strftime('%Y-%m-%d') )
            for i in range(0, 24)
        ]
    ShippingDate = forms.ChoiceField(
        label='Shipping Date',
        choices=[('', '---------')] + [(d.strftime('%Y-%m-%d'), d.strftime('%B %Y')) for d, _ in _first_days()],
    )
    Percentage = forms.FloatField(label='Percentage')

    def clean_ShippingDate(self):
        value = self.cleaned_data['ShippingDate']
        # Convert string to date
        return date.fromisoformat(value)