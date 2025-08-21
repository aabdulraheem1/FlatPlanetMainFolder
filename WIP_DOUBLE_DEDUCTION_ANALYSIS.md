## WIP DOUBLE DEDUCTION ANALYSIS

Based on the code analysis of the populate_calculated_replenishment_v2.py and populate_calculated_production.py commands, here's the quantity flow:

### CURRENT LOGIC:

**Step 1: Replenishment Calculation (populate_calculated_replenishment_v2.py)**
- Line 474-485: When delivery_location ≠ production_site:
  - `available_stock = on_hand + in_transit + wip - safety_stock`
  - `net_requirement = max(0, gross_demand - available_stock)`
  - **WIP IS DEDUCTED HERE** ✅

- Line 487-489: When delivery_location = production_site:
  - `net_requirement = gross_demand` (full demand)
  - **WIP IS NOT DEDUCTED** - left for production command
  - This is correct logic ✅

**Step 2: Production Calculation (populate_calculated_production.py)**
- Line 346: `remaining_stock = opening_inventory['onhand'] + opening_inventory['intransit'] + opening_inventory['wip']`
- Lines 351-362: Production quantity calculation:
  - `production_quantity = replenishment_qty`
  - If `remaining_stock > 0`:
    - `production_quantity -= remaining_stock` 
  - **WIP IS DEDUCTED AGAIN HERE** ⚠️

### THE PROBLEM:

**Scenario A: Different Delivery and Production Sites**
- Replenishment: WIP deducted at delivery location ✅
- Production: WIP deducted again at production site ❌ DOUBLE DEDUCTION

**Scenario B: Same Delivery and Production Site** 
- Replenishment: WIP not deducted (correct) ✅
- Production: WIP deducted (correct) ✅

### CONCLUSION:

**YES, WIP IS BEING DEDUCTED TWICE** when the delivery location is different from the production site.

**The Logic Error:**
1. Replenishment command deducts WIP at delivery location
2. Production command then deducts WIP again at production site
3. This results in WIP being counted twice, leading to artificially low production quantities

**The Fix:**
The production command should only deduct inventory (including WIP) when delivery_location = production_site, or it should be made aware of what inventory was already deducted in the replenishment calculation.
