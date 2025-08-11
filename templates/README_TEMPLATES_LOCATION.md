# ⚠️ CRITICAL TEMPLATE LOCATION NOTICE ⚠️

## OFFICIAL TEMPLATE LOCATION
**ALL Django templates MUST be stored in:**
```
C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR\templates\
```

## ❌ FORBIDDEN TEMPLATE LOCATIONS
**NO templates should EVER be stored in:**
- `C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR\SPR\templates\` (REMOVED)
- Any other location outside the main templates directory

## WHY THIS MATTERS
1. **Django Template Discovery**: Django looks for templates in the configured TEMPLATES directories
2. **Maintenance Nightmare**: Duplicate templates cause confusion and inconsistent updates
3. **Deployment Issues**: Multiple template locations can cause production deployment problems
4. **Version Control**: Having templates in multiple locations makes version control messy

## DJANGO SETTINGS CONFIGURATION
The project is configured to use:
```python
TEMPLATES = [
    {
        'DIRS': [os.path.join(BASE_DIR, 'templates')],
        # This points to: C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR\templates\
    }
]
```

## 🚨 ENFORCEMENT RULE
**IF YOU FIND TEMPLATES ANYWHERE OTHER THAN THE MAIN templates/ DIRECTORY:**
1. DELETE them immediately
2. Ensure the correct template exists in `templates/`
3. Update this README with the date you found duplicates

## LAST CLEANUP
- **Date**: August 11, 2025
- **Action**: Removed entire `SPR/templates/` directory tree
- **Files Removed**: 120+ duplicate template files
- **By**: GitHub Copilot automated cleanup

## TEMPLATE STRUCTURE
```
templates/
├── users/
│   ├── home.html
│   ├── login.html
│   └── register.html
└── website/
    ├── base.html
    ├── control_tower.html
    ├── review_scenario.html
    ├── sections/
    │   ├── control_tower.html
    │   ├── forecast.html
    │   ├── foundry.html
    │   └── supplier.html
    └── includes/
        └── chart_scripts.html
```

## VERIFICATION COMMAND
To verify no duplicate templates exist, run:
```powershell
Get-ChildItem -Path "C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR" -Name "templates" -Recurse
```

**Expected Result**: Only ONE "templates" directory should be found at the root level.

## 🚀 EASY SERVER STARTUP
**No more remembering environments!** Use these scripts:

### Option 1: Batch File (Windows)
```batch
# Double-click this file:
start_server.bat
```

### Option 2: PowerShell Script  
```powershell
# Right-click → "Run with PowerShell":
start_server.ps1
```

### Option 3: Python Script for Shell Access
```python
# For Django shell access:
python quick_shell.py
```

All scripts automatically:
- ✅ Activate the virtual environment
- ✅ Navigate to the correct directory (spr/)
- ✅ Use the correct templates directory (../templates/)
- ✅ Start with proper Django settings

---
**Remember: ONE TEMPLATES DIRECTORY TO RULE THEM ALL! 🏗️**
