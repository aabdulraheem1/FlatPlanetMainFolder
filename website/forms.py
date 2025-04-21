from django import forms
from .models import SMART_Forecast_Model, scenarios
from django import forms
from .models import MasterDataProductModel, MasterDataProductPictures


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


