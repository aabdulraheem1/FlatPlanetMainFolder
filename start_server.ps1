# SPR Django Server Startup Script (PowerShell)
# Double-click this file or run from PowerShell to start the Django server

Write-Host "🚀 SPR Django Server Startup" -ForegroundColor Green
Write-Host "=============================" -ForegroundColor Green

# Change to project directory
$ProjectDir = "C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR"
$SprDir = "$ProjectDir\spr"

Write-Host "📁 Navigating to project directory: $ProjectDir" -ForegroundColor Yellow
Set-Location $ProjectDir

Write-Host "🔧 Activating virtual environment..." -ForegroundColor Yellow
& ".\\.venv\\Scripts\\Activate.ps1"

Write-Host "📁 Changing to spr directory: $SprDir" -ForegroundColor Yellow
Set-Location $SprDir

Write-Host "🌐 Templates will be loaded from: $ProjectDir\templates\" -ForegroundColor Cyan
Write-Host "⚙️  Settings file: SPR\settings.py" -ForegroundColor Cyan
Write-Host ""

Write-Host "🚀 Starting Django development server..." -ForegroundColor Green
Write-Host "Press Ctrl+C to stop the server" -ForegroundColor Red
Write-Host ""

# Start Django server
python manage.py runserver

Write-Host ""
Write-Host "🛑 Server stopped." -ForegroundColor Red
Read-Host "Press Enter to close this window"
