@echo off
REM Django Server Startup Script for SPR Project
REM This script automatically activates the virtual environment and starts the server from the correct directory

echo 🚀 Starting SPR Django Server...
echo 📁 Activating virtual environment...

cd /d "C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR"
call .venv\Scripts\activate.bat

echo 📁 Changing to spr directory (where manage.py and correct settings are)...
cd spr

echo 🔧 Starting Django development server...
echo 📍 Server will run from: %cd%
echo 🌐 Templates will be loaded from: ..\templates\
echo.

python manage.py runserver

echo.
echo 🛑 Server stopped.
pause
