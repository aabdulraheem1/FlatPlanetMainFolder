from django.shortcuts import render, redirect


import pandas as pd
from django.core.files.storage import FileSystemStorage
from .models import SMART_Forecast_Model

import pandas as pd
from django.shortcuts import render, redirect
from django.http import HttpResponseRedirect
from .forms import UploadFileForm, ScenarioForm
from .models import SMART_Forecast_Model

from django.contrib.auth.decorators import login_required




@login_required
def welcomepage(request):
    user_name = request.user.username
    
    return render(request, 'website/welcome_page.html', { 'user_name': user_name})




@login_required
def create_scenario(request):
    smart_forecast_exists = False
    if request.method == 'POST':
        form = ScenarioForm(request.POST)
        if form.is_valid():
            scenario = form.save(commit=False)
            scenario.created_by = request.user.username
            scenario.save()
            smart_forecast_exists = SMART_Forecast_Model.objects.filter(version=scenario).exists()
            return redirect('some_view_name')  # Redirect to a relevant view after saving
    else:
        form = ScenarioForm()
    
    return render(request, 'website/create_scenario.html', {'form': form, 'smart_forecast_exists': smart_forecast_exists})





    


    
    
   
    



    