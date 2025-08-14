# SPR Django Project Structure

## Project Overview
This is the SPR (Strategic Production Resource) Django application for inventory management, production planning, and scenario analysis.

## 📁 Correct Project Structure

**⚠️ CRITICAL: Always maintain this exact structure!**

```
C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR\
├── README.md                          # This file
├── .git/                              # Git repository
├── .venv/                             # Python virtual environment
├── requirements.txt                   # Python dependencies
├── db.sqlite3                         # SQLite database
├── start_server.bat                   # Windows batch script to start server
├── start_server.ps1                   # PowerShell script to start server
├── start_inventory_server.ps1         # PowerShell script for inventory server
├── media/                             # User uploaded files (Excel, images, etc.)
├── static/                            # Static files (CSS, JS, images) - SHARED
├── templates/                         # Django templates - ROOT LEVEL ✅
│   ├── users/
│   │   ├── base.html
│   │   └── [other user templates]
│   └── website/
│       ├── control_tower.html
│       ├── list_scenarios.html
│       ├── delete_safety_stocks.html
│       └── [other website templates]
├── templatetags/                      # Custom template tags
├── temporary/                         # Temporary files for debugging/testing
├── SPR/                              # Django project directory ✅
│   ├── manage.py                     # Django management script
│   ├── __init__.py
│   ├── settings.py                   # Django settings
│   ├── urls.py                       # Main URL configuration
│   ├── wsgi.py                       # WSGI configuration
│   ├── asgi.py                       # ASGI configuration
│   ├── users/                        # Users Django app
│   │   ├── __init__.py
│   │   ├── models.py
│   │   ├── views.py
│   │   ├── urls.py
│   │   ├── admin.py
│   │   ├── apps.py
│   │   └── migrations/
│   └── website/                      # Main Django app ✅
│       ├── __init__.py
│       ├── models.py                 # Database models
│       ├── views.py                  # View functions
│       ├── views_ajax.py             # AJAX view functions
│       ├── urls.py                   # URL patterns
│       ├── admin.py                  # Django admin configuration
│       ├── apps.py                   # App configuration
│       ├── customized_function.py    # Custom business logic
│       ├── direct_polars_queries.py  # Polars data processing
│       ├── migrations/               # Database migrations
│       └── management/               # Django management commands
│           └── commands/
└── [root level files and other directories]
```

## 🎯 Key Rules

### Template Files
- **✅ CORRECT:** `C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR\templates\website\template.html`
- **❌ WRONG:** `C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR\SPR\templates\website\template.html`

### Django Application Files
- **✅ CORRECT:** `C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR\SPR\website\views.py`
- **❌ WRONG:** `C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR\website\views.py`

### Static Files
- **✅ CORRECT:** `C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR\static\css\style.css`
- **❌ WRONG:** `C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR\SPR\static\css\style.css`

## 🚀 Development Commands

### Start Development Server
```bash
# Navigate to project root
cd "C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR"

# Activate virtual environment
.\.venv\Scripts\Activate.ps1

# Navigate to Django project
cd SPR

# Start server
python manage.py runserver
```

### Database Operations
```bash
# Make migrations
python manage.py makemigrations

# Apply migrations  
python manage.py migrate

# Create superuser
python manage.py createsuperuser

# Django shell
python manage.py shell
```

## 📊 Application Structure

### Main Applications
- **users/**: User authentication and management
- **website/**: Core business logic, inventory, production planning

### Key Features
- **Control Tower**: Dashboard showing demand vs. pour plans
- **Scenario Management**: Create and manage production scenarios
- **Inventory Tracking**: Real-time inventory monitoring
- **Production Planning**: Monthly production scheduling
- **Data Import/Export**: Excel file processing

### Database Models
- **CalculatedProductionModel**: Future production projections
- **MonthlyPouredDataModel**: Historical actual production data
- **MasterDataPlan**: Production planning data
- **MasterDataSafetyStocks**: Safety stock levels
- **scenarios**: Production scenarios

## ⚠️ Important Notes

1. **Never create Django files at root level** - they belong in `SPR/website/`
2. **Templates always go in root `templates/` folder** - not in `SPR/templates/`
3. **Static files are shared at root level** - not in individual apps
4. **Always activate virtual environment** before running Django commands
5. **Use absolute paths** when referencing files in scripts

## 🔧 Configuration

### Virtual Environment
- Location: `.venv/` 
- Python Version: 3.12.10
- Django Version: 5.0.14

### Database
- Type: SQLite3
- Location: `db.sqlite3` (root level)

### Settings
- Main settings: `SPR/settings.py`
- Debug mode: Enabled for development
- Template directories: Configured to find templates at root level

## 📝 Development Guidelines

1. **Always check file paths** before creating new files
2. **Follow Django naming conventions** for models, views, URLs
3. **Use proper imports** - reference apps correctly
4. **Test locally** before deploying changes
5. **Keep this README updated** when structure changes

---
**Last Updated:** August 14, 2025  
**Django Version:** 5.0.14  
**Python Version:** 3.12.10
