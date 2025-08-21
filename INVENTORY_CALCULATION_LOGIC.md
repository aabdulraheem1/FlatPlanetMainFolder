# Inventory Calculation Logic Documentation

## Overview
This document describes the correct logic flow for calculating replenishment and production requirements based on forecast demand and inventory levels across multiple sites.

## Problem Statement
The current system has a fundamental flaw in how it handles inventory deductions:
- **Current (Wrong):** Calculate replenishment ignoring source inventory, then try to fix with production-stage inventory deduction
- **Correct:** Inventory at source sites should reduce replenishment requirements FIRST, then inventory at production site should reduce production requirements

## Logic Flow Diagram

```
┌─────────────────┐
│   FORECAST      │
│  (Product X)    │
│  Site A: 20     │
│  Site B: 10     │
└─────────┬───────┘
          │
          ▼
┌─────────────────┐
│ CHECK INVENTORY │
│  AT EACH        │
│  FORECAST SITE  │
└─────────┬───────┘
          │
          ▼
    ┌─────────────┐      ┌─────────────┐
    │   CASE 1:   │      │   CASE 2:   │
    │ Forecast @  │      │ Forecast @  │
    │Production   │      │Different    │
    │   Site      │      │   Site      │
    └──────┬──────┘      └──────┬──────┘
           │                    │
           ▼                    ▼
    ┌─────────────┐      ┌─────────────┐
    │DIRECT CALC: │      │REPLENISH    │
    │Production = │      │CALCULATION: │
    │max(0,       │      │Net Demand = │
    │Forecast -   │      │max(0,       │
    │Local Inv)   │      │Forecast -   │
    └─────────────┘      │Source Inv)  │
                         └──────┬──────┘
                                │
                                ▼
                         ┌─────────────┐
                         │PRODUCTION   │
                         │CALCULATION: │
                         │Production = │
                         │max(0,       │
                         │Replenish -  │
                         │Prod Site    │
                         │Inventory)   │
                         └─────────────┘
```

## Detailed Examples

### Example 1: Forecast at Different Sites (Multi-Site Scenario)
```
Initial State:
- Forecast: DTC1=20, DTC2=10 (total demand = 30)
- Inventory: DTC1=50, DTC2=2, COI2(production)=4

Step 1 - Calculate Replenishment Needs:
- DTC1: 50 ≥ 20 → Replenishment needed = 0
- DTC2: 2 < 10 → Replenishment needed = 10-2 = 8
- Total replenishment to COI2 = 0 + 8 = 8

Step 2 - Calculate Production Needs:
- COI2 inventory = 4
- Production needed = max(0, 8-4) = 4

Result: Replenishment=8, Production=4
```

### Example 2: Forecast at Production Site (Direct Scenario)
```
Initial State:
- Forecast: COI2=30 (at production site)
- Inventory: COI2=4

Step 1 - Direct Calculation:
- No replenishment needed (forecast already at production site)
- Production needed = max(0, 30-4) = 26

Result: Replenishment=0, Production=26
```

## Current System Issues

### BK57592A Case Study
```
Current Results (WRONG):
- Forecast: 98.0 units (all going to COI2)
- Replenishment: 98.0 units (ignoring source inventory)
- Inventory: COI2=11.0, DTC1=18.0 (total=29.0)
- Production: 87.0 units (only deducting COI2 inventory)
- Error: +18.0 units (DTC1 inventory ignored)

Expected Results (CORRECT):
- If forecast sources are at different sites:
  - Check inventory at each source site first
  - Only ship what's needed after local inventory
- If forecast is at COI2 directly:
  - Production = max(0, 98.0 - 29.0) = 69.0 units
```

## Implementation Strategy

### Phase 1: Identify Forecast Sources
- Determine where each forecast record originates
- Map forecast location to production site relationships

### Phase 2: Source-Level Inventory Deduction
- For each forecast at non-production sites:
  - Check local inventory at forecast site
  - Calculate net replenishment needed = max(0, forecast - local_inventory)

### Phase 3: Production-Level Inventory Deduction
- Aggregate all replenishment requirements at production site
- Apply production site inventory deduction
- Calculate final production needed = max(0, total_replenishment - production_site_inventory)

## Database Models Affected

### Input Models
- `SMART_Forecast_Model` - Contains forecast data with site information
- `MasterDataInventory` - Contains inventory levels by product and site

### Calculation Models
- `CalcualtedReplenishmentModel` - Should reflect net replenishment after source inventory
- `CalculatedProductionModel` - Should reflect net production after all inventory deductions

## Code Changes Required

### 1. Replenishment Command (`populate_calculated_replenishment_v2.py`)
- Add source site inventory checking
- Modify net requirement calculation to account for source inventory
- Implement forecast location detection logic

### 2. Production Command (`populate_calculated_production.py`)
- Aggregate inventory across ALL sites for each product
- Remove site-specific inventory limitation
- Ensure proper inventory deduction sequence

## Testing Strategy

### Unit Tests
- Test Case 1: Forecast at production site (direct calculation)
- Test Case 2: Forecast at different sites (replenishment flow)
- Test Case 3: Mixed scenarios with multiple sites
- Test Case 4: Zero inventory scenarios
- Test Case 5: Excess inventory scenarios

### Integration Tests
- End-to-end flow from forecast to production
- Multi-product scenarios
- Cross-site inventory sharing
- Performance with large datasets

## Performance Considerations

### Optimization Opportunities
- Batch inventory lookups by product
- Pre-aggregate inventory by product across all sites
- Use efficient data structures for site-product mappings
- Minimize database queries with proper joins

### Memory Management
- Process large datasets in chunks
- Use streaming for bulk operations
- Monitor memory usage during processing

## Migration Strategy

### Backwards Compatibility
- Maintain existing API interfaces
- Add feature flags for new logic
- Provide comparison utilities for validation

### Data Validation
- Compare old vs new calculation results
- Identify and document discrepancies
- Validate against known test cases

### Rollout Plan
1. Implement new logic with feature flag (disabled)
2. Run parallel calculations for validation
3. Enable new logic for test scenarios
4. Gradual rollout to production scenarios
5. Deprecate old logic after validation period
