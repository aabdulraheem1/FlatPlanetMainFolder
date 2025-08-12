# SPR Django Server with INVENTORY Functionality (PowerShell)
# Use this script to start the server with full inventory upload/management features

Write-Host "🚀 SPR Django Server with INVENTORY FUNCTIONALITY" -ForegroundColor Green
Write-Host "==================================================" -ForegroundColor Green

# Change to project directory
$ProjectDir = "C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR"
$SprMainDir = "$ProjectDir\SPR"

Write-Host "📁 Navigating to project directory: $ProjectDir" -ForegroundColor Yellow
Set-Location $ProjectDir

Write-Host "🔧 Activating virtual environment..." -ForegroundColor Yellow
& ".\\.venv\\Scripts\\Activate.ps1"

Write-Host "📁 Changing to SPR main directory: $SprMainDir" -ForegroundColor Yellow
Set-Location $SprMainDir

Write-Host "🌐 Templates will be loaded from: $ProjectDir\templates\" -ForegroundColor Cyan
Write-Host "⚙️  Settings file: SPR\settings.py" -ForegroundColor Cyan
Write-Host "📦 Inventory functionality: ✅ ENABLED" -ForegroundColor Green
Write-Host "🔧 Upload/Delete inventory: ✅ AVAILABLE" -ForegroundColor Green
Write-Host ""

Write-Host "🚀 Starting Django development server with FULL FUNCTIONALITY..." -ForegroundColor Green
Write-Host "Press Ctrl+C to stop the server" -ForegroundColor Red
Write-Host ""

# Start Django server
python manage.py runserver

Write-Host ""
Write-Host "Server stopped." -ForegroundColor Red
Read-Host "Press Enter to close this window"
