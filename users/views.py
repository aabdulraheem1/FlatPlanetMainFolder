from django.shortcuts import render

# Create your views here.


from django.shortcuts import render, redirect

from django.shortcuts import render, redirect
from django.contrib.auth import login, authenticate
from .forms import UserRegisterForm

def register(request):
    if request.method == 'POST':
        form = UserRegisterForm(request.POST)
        if form.is_valid():
            form.save()
            username = form.cleaned_data.get('username')
            password = form.cleaned_data.get('password1')
            user = authenticate(username=username, password=password)
            login(request, user)
            return redirect('user_login')
    else:
        form = UserRegisterForm()
    return render(request, 'users/register.html', {'form': form})

from django.shortcuts import render, redirect
from django.contrib.auth.forms import AuthenticationForm
from django.contrib.auth import login

def user_login(request):
    if request.method == 'POST':
        form = AuthenticationForm(data=request.POST)
        if form.is_valid():
            user = form.get_user()
            login(request, user)
            return redirect('welcomepage')
    else:
        form = AuthenticationForm()
    return render(request, 'users/login.html', {'form': form})

from django.shortcuts import redirect
from django.contrib.auth import logout

def user_logout(request):
    logout(request)
    return redirect('home')

def home(request):
    if request.user.is_authenticated:
        return redirect('welcomepage')  # Replace 'welcome' with the name of your welcome page URL pattern
    context = {
        'title': 'Flat Planet',
        'image_url': '/static/images/flat_planet.jpg'  # Update this path to your image location
    }
    return render(request, 'users/home.html', context)



