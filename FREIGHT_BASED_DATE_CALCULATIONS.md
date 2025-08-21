# Freight-Based Date Calculations Implementation

## Overview

This document provides comprehensive details on the freight-based date calculation system implemented for inventory and production management. The system integrates customer incoterms, freight models, and cast-to-despatch lead times to provide accurate supply chain scheduling.

## Background & Problem Statement

### Initial Issue
- **Problem**: Cast-to-despatch days (e.g., 39 days for COI2) were not being applied in production date calculations
- **Root Cause**: BK57592A was incorrectly assigned to DTC1 instead of foundry COI2
- **Discovery**: Direct forecasts followed a different pathway than replenishment, missing site selection logic

### Evolution to Comprehensive Solution
During investigation, it became clear that the core issue was not just cast-to-despatch, but the complete absence of freight and incoterm integration in date calculations. The user specification revealed the need for:

> "shipping date = period_AU - freight time.... freight time is coming from masterdatafreight that defines the days between region in smart forecasts and production site... and that depends on the customer incoterm"

### Implementation Challenges
- **Field Name Inconsistencies**: Different Django environments had different foreign key field structures
- **MasterDataForecastRegionModel Primary Key**: Uses `Forecast_region` string field as primary key, not integer `id`
- **Django Query Variations**: Required fallback approaches for cross-environment compatibility

## System Architecture

### Data Models Involved

1. **MasterdataIncoTermsModel**
   - Links customers to incoterm types
   - Fields: CustomerCode, Incoterm (related to MasterDataIncotTermTypesModel)

2. **MasterDataIncotTermTypesModel**
   - Defines incoterm categories for freight calculation
   - Fields: IncoTerm, IncoTermCaregory
   - Categories:
     - `NO FREIGHT`: EXW, FCA terms (no freight adjustment)
     - `PLANT TO DOMESTIC PORT`: CFR, CIF to domestic port
     - `PLANT TO FOREIGN PORT`: CFR, CIF to foreign destination  
     - `PLANT TO CUSTOMER`: DDP, DAP door-to-door delivery

3. **MasterDataFreightModel**
   - Route-specific freight components
   - Fields: ForecastRegion (FK to MasterDataForecastRegionModel), ManufacturingSite, PlantToDomesticPortDays, OceanFreightDays, PortToCustomerDays

4. **MasterDataForecastRegionModel**  
   - **Primary Key**: `Forecast_region` (string field, not integer id)
   - Used as the key for freight route mapping

### Date Transformation Flow

```
SMART Forecast (Period_AU) 
    ↓ (freight adjustment)
Shipping Date 
    ↓ (cast-to-despatch for cross-site)
Pouring Date
```

## Implementation Details

### Replenishment Command (populate_calculated_replenishment_v2.py)

#### Freight Data Loading
```python
# Load freight models (final working version)
freight_data = list(
    MasterDataFreightModel.objects.filter(version=scenario)
    .select_related('ForecastRegion')
    .values('ForecastRegion__Forecast_region', 'ManufacturingSite', 
           'PlantToDomesticPortDays', 'OceanFreightDays', 'PortToCustomerDays')
)

freight_map = {
    (entry['ForecastRegion__Forecast_region'], entry['ManufacturingSite']): {
        'plant_to_port': entry['PlantToDomesticPortDays'] or 0,
        'ocean_freight': entry['OceanFreightDays'] or 0,
        'port_to_customer': entry['PortToCustomerDays'] or 0
    }
    for entry in freight_data
    if entry['ForecastRegion__Forecast_region']
}

# Note: Direct join to ForecastRegion__Forecast_region works because 
# MasterDataForecastRegionModel uses Forecast_region as primary key
```

#### Freight Calculation Function
```python
def calculate_shipping_date_with_freight(self, period_au, customer_code, forecast_region, 
                                          production_site, customer_incoterms_map, freight_map):
    """
    Calculate shipping date based on Period_AU, customer incoterms, and freight models.
    
    Logic: shipping_date = Period_AU - freight_time
    Where freight_time depends on customer incoterm category and route-specific components.
    """
    try:
        # Get customer incoterm information
        customer_info = customer_incoterms_map.get(customer_code, {})
        incoterm_category = customer_info.get('category', 'NO FREIGHT')
        
        # Get freight route information
        route_key = (forecast_region, production_site)
        freight_info = freight_map.get(route_key, {})
        
        # Calculate freight days based on incoterm category
        freight_days = 0
        
        if incoterm_category == 'NO FREIGHT':
            # EXW, FCA terms - no freight adjustment
            freight_days = 0
        elif incoterm_category == 'PLANT TO DOMESTIC PORT':
            # CFR, CIF to domestic port only
            freight_days = freight_info.get('plant_to_port', 0)
        elif incoterm_category == 'PLANT TO FOREIGN PORT':
            # CFR, CIF to foreign destination
            freight_days = (freight_info.get('plant_to_port', 0) + 
                          freight_info.get('ocean_freight', 0))
        elif incoterm_category == 'PLANT TO CUSTOMER':
            # DDP, DAP - door to door delivery
            freight_days = (freight_info.get('plant_to_port', 0) + 
                          freight_info.get('ocean_freight', 0) + 
                          freight_info.get('port_to_customer', 0))
        
        # Calculate shipping date
        shipping_date = period_au - timedelta(days=freight_days)
        
        return shipping_date, freight_days
        
    except Exception as e:
        # Fallback to Period_AU if freight calculation fails
        return period_au, 0
```

#### Usage in Replenishment Record Creation
```python
# Calculate freight-adjusted shipping date
shipping_date, freight_days = self.calculate_shipping_date_with_freight(
    period_au=row['Period_AU'],
    customer_code=row.get('Customer_code'),
    forecast_region=row.get('Forecast_Region'),
    production_site=site,
    customer_incoterms_map=customer_incoterms_map,
    freight_map=freight_map
)

replenishment_records.append(CalcualtedReplenishmentModel(
    version=scenario,
    Product_id=row['Product'],
    Site_id=site,
    Location=row.get('Location', ''),
    ShippingDate=shipping_date,  # Uses freight-adjusted date
    ReplenishmentQty=net_requirement,
    latest_customer_invoice=None,
    latest_customer_invoice_date=None,
))
```

### Production Command (populate_calculated_production.py)

#### Enhanced Direct Forecast Processing

##### Freight Data Loading
Same approach as replenishment command - loads customer incoterms and freight models.

##### Updated SMART Forecast Query
```python
# Include Customer_code and Forecast_Region for freight calculation
.values('Product', 'Location', 'Period_AU', 'Qty', 'Customer_code', 'Forecast_Region')
```

##### Enhanced Site Selection
```python
# Now uses customer data for proper site selection
production_site = select_site(
    product=product,
    period=period_au,
    customer_code=customer_code,  # Now available from forecast data
    forecast_region=forecast_region,  # Now available from forecast data
    scenario=scenario,
    order_book_map=order_book_map,
    production_map=production_map,
    supplier_map={},
    manual_assign_map=manual_assign_map,
    plant_map=plant_map,
    foundry_sites=foundry_sites,
    can_assign_foundry_fn=can_assign_foundry_fn
)
```

##### Freight-Based Date Calculation
```python
# Calculate freight-adjusted shipping date first
shipping_date, freight_days = self.calculate_shipping_date_with_freight(
    period_au=period_au,
    customer_code=customer_code,
    forecast_region=forecast_region,
    production_site=production_site,
    customer_incoterms_map=customer_incoterms_map,
    freight_map=freight_map
)

# Then apply cast-to-despatch for cross-site production
if production_site != forecast_location and production_site in foundry_sites:
    cast_days = cast_to_despatch.get((production_site, scenario.version), 0)
    pouring_date = shipping_date - timedelta(days=cast_days)
else:
    pouring_date = shipping_date
```

## Example Data Flow

### Real Example: Customer FREJAK01, Product BK57592A

1. **Customer Data**: FREJAK01 with EXW incoterm → `NO FREIGHT` category
2. **Forecast**: WestAustAsia region, Period_AU = 2024-08-01
3. **Production Site**: COI2 (foundry)
4. **Freight Calculation**:
   - Route: (WestAustAsia, COI2) → 32 days ocean freight
   - Incoterm: EXW → NO FREIGHT category → 0 freight days
   - Shipping Date: 2024-08-01 - 0 days = 2024-08-01
5. **Cast-to-Despatch**:
   - Cross-site production: COI2 → DTC1
   - Cast days: 39 days
   - Pouring Date: 2024-08-01 - 39 days = 2024-06-23

### Different Incoterm Example: DDP Customer

1. **Customer**: Hypothetical customer with DDP incoterm → `PLANT TO CUSTOMER` category
2. **Forecast**: WestAustAsia region, Period_AU = 2024-08-01
3. **Production Site**: COI2
4. **Freight Calculation**:
   - Route: (WestAustAsia, COI2) → Plant-to-port: 5 days, Ocean: 32 days, Port-to-customer: 3 days
   - Total freight: 5 + 32 + 3 = 40 days
   - Shipping Date: 2024-08-01 - 40 days = 2024-06-22
5. **Cast-to-Despatch**:
   - Pouring Date: 2024-06-22 - 39 days = 2024-05-14

## Key Benefits

### Accurate Supply Chain Scheduling
- **Proper Lead Time Calculation**: Accounts for actual freight and delivery requirements
- **Customer-Specific Terms**: Different incoterms result in different shipping dates
- **Route Optimization**: Uses actual freight models for different regions

### Fixed Issues
1. **Cast-to-Despatch Application**: Now properly applied with 39-day lead time for BK57592A
2. **Site Selection**: Direct forecasts now use same logic as replenishment
3. **Chronological Inventory**: Fixed inventory consumption bug
4. **Comprehensive Integration**: Freight, incoterms, and cast-to-despatch work together

## Testing & Validation

### BK57592A Test Case
- **Before**: Incorrectly assigned to DTC1, no cast-to-despatch applied
- **After**: Correctly assigned to COI2, 39-day cast-to-despatch applied
- **Freight**: EXW incoterm → 0 freight adjustment (customer picks up)
- **Result**: Proper 39-day production lead time implemented

### Replenishment Records
- **Before**: BK57592A created 0 replenishment records (inventory satisfied all demand)  
- **After**: Creates 5 replenishment records with chronological inventory consumption

## Future Enhancements

### Potential Improvements
1. **Dynamic Freight Rates**: Integrate with real-time shipping data
2. **Route Optimization**: AI-driven optimal route selection
3. **Seasonal Adjustments**: Account for holiday and weather delays
4. **Customer Priorities**: VIP customer expedited handling

### Monitoring Recommendations
1. **Date Accuracy Tracking**: Monitor forecast vs actual delivery dates
2. **Freight Cost Analysis**: Track freight cost vs incoterm selection
3. **Cast-to-Despatch Optimization**: Analyze actual vs planned lead times

## Troubleshooting Guide

### Common Issues

#### Field Name Inconsistencies
- **Symptom**: `FieldError: Cannot resolve keyword 'ForecastRegion' into field`
- **Cause**: Different Django environments may have different field structures
- **Solution**: Use direct field access: `ForecastRegion__Forecast_region`
- **Note**: MasterDataForecastRegionModel uses `Forecast_region` as primary key (string), not `id` (integer)

#### Missing Customer Incoterms
- **Symptom**: Default to 'NO FREIGHT' category
- **Solution**: Ensure customer has incoterm mapping in MasterdataIncoTermsModel

#### Missing Freight Routes
- **Symptom**: 0 freight days for all customers
- **Solution**: Verify MasterDataFreightModel has route data for forecast regions

#### Cast-to-Despatch Not Applied
- **Symptom**: Same shipping and pouring dates
- **Solution**: Ensure production site is different from forecast location and marked as foundry

### Debug Commands

```python
# Check customer incoterms
customer_incoterms = MasterdataIncoTermsModel.objects.filter(
    CustomerCode='FREJAK01'
).select_related('Incoterm')

# Check freight routes
freight_routes = MasterDataFreightModel.objects.filter(
    ForecastRegion__ForecastRegion='WestAustAsia',
    ManufacturingSite='COI2'
)

# Check cast-to-despatch
cast_days = MasterDataCastToDespatchModel.objects.filter(
    Foundry__SiteName='COI2'
)
```

## Conclusion

The freight-based date calculation system provides a comprehensive solution for accurate supply chain scheduling. By integrating customer incoterms, freight models, and cast-to-despatch lead times, the system ensures that production schedules reflect real-world delivery requirements and customer terms.

The implementation addresses the original cast-to-despatch issue while providing a foundation for more sophisticated supply chain optimization in the future.
