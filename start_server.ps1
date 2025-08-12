# SPR Django Server Startup Script (PowerShell) - BASIC VERSION
# This runs the basic 'spr' project WITHOUT inventory functionality
# For FULL INVENTORY features, use start_inventory_server.ps1 instead

Write-Host "🚀 SPR Django Server Startup - BASIC VERSION" -ForegroundColor Green
Write-Host "=============================================" -ForegroundColor Green
Write-Host "⚠️  NOTE: This is the BASIC server without inventory features" -ForegroundColor Yellow
Write-Host "📦 For INVENTORY functionality, use start_inventory_server.ps1" -ForegroundColor Yellow
Write-Host ""

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
Write-Host "⚙️  Settings file: settings.py (local)" -ForegroundColor Cyan
Write-Host "📦 Inventory functionality: ❌ NOT AVAILABLE" -ForegroundColor Red
Write-Host ""

Write-Host "🚀 Starting Django development server..." -ForegroundColor Green
Write-Host "Press Ctrl+C to stop the server" -ForegroundColor Red
Write-Host ""

# Start Django server
python manage.py runserver

Write-Host ""
Write-Host "🛑 Server stopped." -ForegroundColor Red
Read-Host "Press Enter to close this window"
