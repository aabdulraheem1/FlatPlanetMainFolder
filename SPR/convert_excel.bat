@echo off
echo =========================================
echo Site Allocation Excel Converter
echo =========================================
echo.

REM Check if Python is available
python --version >nul 2>&1
if errorlevel 1 (
    echo Error: Python is not installed or not in PATH
    echo Please install Python or add it to your PATH
    pause
    exit /b 1
)

REM Check if pandas is installed
python -c "import pandas" >nul 2>&1
if errorlevel 1 (
    echo Error: pandas is not installed
    echo Installing pandas...
    pip install pandas openpyxl
    if errorlevel 1 (
        echo Failed to install pandas
        pause
        exit /b 1
    )
)

echo Ready to convert your Excel file!
echo.
echo Instructions:
echo 1. Make sure your input file has these columns:
echo    Product, Date, Site1_Name, Site1_Percentage, Site2_Name, Site2_Percentage, Site3_Name, Site3_Percentage
echo.
echo 2. The script will create a new file with columns:
echo    Product, Site, AllocationPercentage
echo.

REM Get input file
set /p input_file="Enter the path to your input Excel file: "
if not exist "%input_file%" (
    echo Error: File '%input_file%' does not exist
    pause
    exit /b 1
)

REM Generate output filename
for %%F in ("%input_file%") do (
    set "output_file=%%~dpnF_converted%%~xF"
)

echo.
echo Input file:  %input_file%
echo Output file: %output_file%
echo.

REM Run the conversion
python convert_site_allocation_excel.py "%input_file%" "%output_file%"

echo.
echo Conversion complete! Press any key to exit.
pause >nul
