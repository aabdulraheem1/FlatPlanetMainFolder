# 🔧 SPR SYSTEM - AI ASSISTANT CONTEXT GUIDE

**Purpose:** Provide complete context for AI assistants working on the SPR (Supply Planning & Replenishment) system  
**Last Updated:** August 20, 2025  
**System Status:** Production Ready with Safety Stock Integration

## 🎯 SYSTEM OVERVIEW

The SPR system is a Django-based supply planning and replenishment application that calculates production requirements based on forecast demand, inventory levels, and safety stock requirements. The system follows a strict "no caching, no fallback" philosophy - if data doesn't exist, fail fast with clear error messages.

## 📊 RECENT MAJOR IMPLEMENTATION

### Safety Stock Integration (COMPLETED)
- **Business Requirement:** Integrate safety stock using formula `SafetyQty + MinimumQty`
- **Implementation:** Completed in `populate_calculated_replenishment_v2.py`
- **Test Product:** T810EP (fully validated end-to-end)
- **Status:** ✅ PRODUCTION READY

**Key Files Modified:**
- `populate_calculated_replenishment_v2.py` (lines 348-409)
- Added safety stock loading, calculation, and application logic

## 🗂️ SYSTEM ARCHITECTURE

### Core Models Classification

**🟢 INPUT MODELS (User Data - Tracked for Changes):**
- `MasterDataSafetyStocks` - Safety stock configuration (SafetyQty + MinimumQty)
- `SMART_Forecast_Model` - Forecast data uploads
- `Revenue_Forecast_Model` - Revenue forecast data
- `MasterDataPlantModel` - Plant/site configuration
- `MasterDataProductModel` - Product master data
- `MasterDataInventory` - Inventory snapshots
- `scenarios` - Scenario management

**🔴 OUTPUT MODELS (Calculated Data - NOT Tracked):**
- `CalculatedProductionModel` - Production schedules (populated BY calculation)
- `CalcualtedReplenishmentModel` - Replenishment requirements (populated BY calculation)
- `AggregatedForecast` - Aggregated forecast data (populated BY calculation)
- All `Cached*` models - Temporary calculation results

### Database Structure
- **Primary Database:** Django ORM with PostgreSQL/SQLite
- **External Systems:** PowerBI integration for invoice data
- **Safety Stock Source:** `MasterDataSafetyStocks` table

## 🔄 CALCULATION FLOW

### Standard Process Flow:
1. **Forecast Analysis** → Load demand from SMART_Forecast_Model
2. **Safety Stock Integration** → Apply SafetyQty + MinimumQty formula
3. **Inventory Assessment** → Check available inventory at source locations
4. **Replenishment Calculation** → Create replenishment requirements (ALL forecasts create replenishment records)
5. **Production Scheduling** → Convert replenishment to production schedules

### ⚡ CRITICAL FLOW RULE (Updated August 20, 2025):
**NO PRODUCTION BYPASSES REPLENISHMENT LOGIC**

- ALL forecast records MUST create replenishment records
- NO "direct production" skipping allowed
- Complete traceability: Forecast → Replenishment → Production
- This ensures proper audit trail and calculation consistency
2. **Safety Stock Integration** → Apply SafetyQty + MinimumQty
3. **Replenishment Calculation** → Net requirement = Demand + Safety Stock - Inventory  
4. **Production Planning** → Convert replenishment to production schedules

### Key Management Commands:
```bash
# Replenishment calculation (includes safety stock)
python manage.py populate_calculated_replenishment_v2 "Aug 25 SPR"

# Production calculation  
python manage.py populate_calculated_production "Aug 25 SPR"

# Analysis commands
python manage.py analyze_forecast "Aug 25 SPR" --product T810EP
python manage.py analyze_replenishment "Aug 25 SPR" --product T810EP
```

## 🛡️ SAFETY STOCK IMPLEMENTATION DETAILS

### Formula: `SafetyQty + MinimumQty`
```python
# Safety stock loading (lines 348-351)
safety_stocks = MasterDataSafetyStocks.objects.filter(version=scenario).values(
    'Plant', 'PartNum', 'SafetyQty', 'MinimumQty'
)

# Combined calculation (lines 352-360)
for safety_stock in safety_stocks:
    safety_qty = float(safety_stock['SafetyQty'] or 0)
    minimum_qty = float(safety_stock['MinimumQty'] or 0)
    combined_safety_stock = safety_qty + minimum_qty

# Application in net requirements (lines 400-404)
safety_stock = safety_stock_map.get((product_key, location), 0)
gross_demand_with_safety = demand + safety_stock
net_requirement = max(0, gross_demand_with_safety - remaining_inventory)
```

### Validation Results:
- **Test Product:** T810EP
- **Safety Stock Records:** 126,426 loaded successfully
- **Formula Verification:** SafetyQty + MinimumQty working correctly
- **End-to-End Test:** Complete flow validated (Forecast → Replenishment → Production)

## 🚨 CRITICAL SYSTEM RULES

### DO NOT IMPLEMENT:
1. **Caching Logic** - System explicitly rejects all caching approaches
2. **Fallback Solutions** - Fail fast with clear error messages
3. **Data Approximation** - Use exact data or fail clearly

### ALWAYS REMEMBER:
1. **Safety Stock Formula:** Must always be `SafetyQty + MinimumQty`
2. **Net Requirements:** `Demand + Safety Stock - Available Inventory`
3. **Database Constraints:** Respect `unique_together` constraints
4. **Scenario Isolation:** All calculations are scenario-specific

## 📁 PROJECT STRUCTURE

```
SPR/
├── SPR/website/
│   ├── models.py                    # Core data models
│   ├── management/commands/         # Django management commands
│   │   ├── populate_calculated_replenishment_v2.py  # Main replenishment logic
│   │   └── populate_calculated_production.py       # Production calculation
│   └── views.py                     # Web interface
├── templates/                       # Django templates
├── static/                          # Static files
└── requirements.txt                 # Python dependencies
```

## 🔍 TESTING APPROACH

### Standard Test Product: T810EP
- **Why T810EP:** Comprehensive test case with outsourced supply chain
- **Test Scenario:** "Aug 25 SPR"
- **Validation Points:**
  - Forecast loading (1,761 units across 4 locations)
  - Safety stock application (19 plants with safety stock)
  - Replenishment generation (1,228 units to HBZJBF02)
  - Production calculation (157 records created)

### Test Commands:
```bash
# Complete flow test
python manage.py analyze_forecast "Aug 25 SPR" --product T810EP
python manage.py analyze_replenishment "Aug 25 SPR" --product T810EP  
python manage.py populate_calculated_production "Aug 25 SPR" --product T810EP
```

## 💡 COMMON DEBUGGING SCENARIOS

### Safety Stock Issues:
- **Missing Safety Stock:** Check MasterDataSafetyStocks has records for scenario
- **Zero Application:** Verify Plant/PartNum exact match in lookup dictionary
- **Wrong Formula:** Ensure both SafetyQty AND MinimumQty included

### Replenishment Issues:
- **No Replenishment Generated:** Check if inventory covers all demand + safety stock
- **Negative Quantities:** Review net requirement calculation logic
- **Missing Supplier Assignment:** Verify MasterDataSupplyOptionsModel configuration

### Production Issues:
- **No Production Records:** Check if replenishment exists for the product
- **Outsourced vs Internal:** Review plant configuration (InhouseOrOutsource field)

## 🎯 SUCCESS INDICATORS

### Technical Success:
- Safety stock records load without errors
- Calculations complete within reasonable time (<5 seconds typical)
- No negative quantities generated
- All foreign key relationships maintained

### Business Success:
- Safety stock properly protects against stockouts
- Replenishment quantities include appropriate buffers
- Production schedules align with supply chain strategy
- Geographic distribution matches business rules

## 📋 WHEN WORKING WITH THIS SYSTEM:

### For AI Assistants:
1. **Read Documentation First:** This file + SAFETY_STOCK_INTEGRATION_COMPLETE.md
2. **Use T810EP for Testing:** Proven test case with known good results
3. **Respect System Philosophy:** No caching, no fallback, fail fast
4. **Validate with Flow Analysis:** Always test end-to-end when making changes
5. **Check Existing Implementation:** Safety stock integration is complete and working

### For Developers:
1. **Follow Django Best Practices:** Use ORM, respect model constraints
2. **Test with Real Data:** T810EP scenario provides comprehensive test case
3. **Document Changes:** Update this guide when making significant changes
4. **Preserve Business Logic:** Safety stock formula is fixed business requirement

---

**System Status:** ✅ PRODUCTION READY  
**Safety Stock Integration:** ✅ COMPLETE AND TESTED  
**Next Development:** Use existing system, no safety stock changes needed
