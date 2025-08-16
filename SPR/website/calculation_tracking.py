"""
🚫🚫🚫 CRITICAL WARNING TO ALL FUTURE DEVELOPERS 🚫🚫🚫

⚠️ ⚠️ ⚠️  NO FALLBACKS ALLOWED IN THIS FILE  ⚠️ ⚠️ ⚠️
⚠️ ⚠️ ⚠️  NO CACHING ALLOWED IN THIS FILE  ⚠️ ⚠️ ⚠️
⚠️ ⚠️ ⚠️  NO DEFAULT VALUES WHEN TRACKING FAILS  ⚠️ ⚠️ ⚠️

CALCULATION TRACKING UTILITY - MISSION CRITICAL SYSTEM

This system provides real-time change detection for scenario data models.
It tracks changes in any model that has 'version' or 'scenario' fields
to determine if calculate_model needs to be re-run.

🔴 ABSOLUTELY PROHIBITED TECHNIQUES: 🔴
- Caching model lists or metadata - WILL CAUSE DATA CORRUPTION
- Storing change detection results - WILL CAUSE STALE DATA
- Memoization of model introspection - WILL MISS CHANGES
- Try/catch blocks that return default values - WILL HIDE CRITICAL ERRORS
- Fallback button states - WILL COMPROMISE DATA INTEGRITY

🟢 REQUIRED BEHAVIOR: 🟢
- All change detection MUST be performed in real-time from database
- If this system fails, the entire application MUST fail
- Every function call MUST return accurate, real-time data
- Database errors MUST propagate up - NO SILENT FAILURES

IF YOU ADD FALLBACKS OR CACHING TO THIS FILE:
🔥 YOU WILL COMPROMISE DATA INTEGRITY 🔥
🔥 YOU WILL CAUSE INCORRECT CALCULATIONS 🔥  
🔥 YOU WILL BE RESPONSIBLE FOR DATA CORRUPTION 🔥

When in doubt, ASK - don't add fallbacks!
"""

import logging
from datetime import datetime
from django.apps import apps
from django.db import models
from django.utils import timezone

logger = logging.getLogger(__name__)

def get_scenario_related_models():
    """
    Get all INPUT models that have 'version' or 'scenario' fields (NO CACHING).
    Always performs fresh model introspection.
    
    EXCLUDES calculated/output models that are populated BY the calculation process:
    - CalculatedProductionModel (output of calculation)
    - CalcualtedReplenishmentModel (output of calculation) 
    - AggregatedForecast (output of calculation)
    - All Cached* models (cached data)
    - InventoryProjectionModel (calculated projections)
    - OpeningInventorySnapshot (cached data)
    - MonthlyPouredDataModel (cached data)
    """
    scenario_models = []
    
    # Models to EXCLUDE - these are output/calculated models, not input models
    EXCLUDED_CALCULATED_MODELS = {
        'calculatedproductionmodel',          # Output of calculation process
        'calcualtedreplenishmentmodel',       # Output of calculation process
        'aggregatedforecast',                 # Output of calculation process
        'cachedcontroltowerdata',            # Cached calculation results
        'cachedfoundrydata',                 # Cached calculation results
        'cachedforecastdata',                # Cached calculation results
        'cachedinventorydata',               # Cached calculation results
        'cachedsupplierdata',                # Cached calculation results
        'cacheddetailedinventorydata',       # Cached calculation results
        'aggregatedfinancialchartdata',      # Cached calculation results
        'inventoryprojectionmodel',          # Calculated projections
        'openinginventorysnapshot',          # Cached snapshot data
        'monthlypoureddatamodel',            # Cached data from external source
        'scenariooptimizationstate',        # Optimization state tracking
    }
    
    # Get all installed apps and their models
    for app in apps.get_app_configs():
        for model in app.get_models():
            # Skip excluded calculated/output models
            model_name_lower = model._meta.model_name.lower()
            if model_name_lower in EXCLUDED_CALCULATED_MODELS:
                logger.debug(f"🚫 EXCLUDING calculated model: {model._meta.app_label}.{model._meta.model_name}")
                continue
            
            # Check if model has version or scenario fields
            field_names = [f.name for f in model._meta.get_fields()]
            
            has_version = any('version' in field_name.lower() for field_name in field_names)
            has_scenario = any('scenario' in field_name.lower() for field_name in field_names)
            
            if has_version or has_scenario:
                # Get the actual version/scenario field
                version_field = None
                for field in model._meta.get_fields():
                    if ('version' in field.name.lower() or 'scenario' in field.name.lower()) and hasattr(field, 'related_model'):
                        version_field = field
                        break
                    elif field.name.lower() == 'version' and isinstance(field, models.ForeignKey):
                        version_field = field
                        break
                
                logger.debug(f"✅ INCLUDING input model: {model._meta.app_label}.{model._meta.model_name}")
                scenario_models.append({
                    'model': model,
                    'app_label': model._meta.app_label,
                    'model_name': model._meta.model_name,
                    'version_field': version_field.name if version_field else None,
                    'table_name': model._meta.db_table
                })
    
    logger.info(f"📊 Found {len(scenario_models)} INPUT models for change tracking (excluded calculated/output models)")
    return scenario_models

def check_scenario_data_changes(scenario, last_calculated_time=None):
    """
    Real-time check if any scenario-related data has changed since last calculation.
    NO CACHING - always queries database directly.
    
    Args:
        scenario: scenarios model instance
        last_calculated_time: datetime when model was last calculated (optional)
    
    Returns:
        dict: {
            'has_changes': bool,
            'changed_models': list,
            'total_models_checked': int,
            'check_timestamp': datetime
        }
    """
    
    if not last_calculated_time:
        last_calculated_time = scenario.last_calculated
    
    if not last_calculated_time:
        # Never calculated before - definitely needs calculation
        return {
            'has_changes': True,
            'changed_models': ['Never calculated before'],
            'total_models_checked': 0,
            'check_timestamp': timezone.now(),
            'reason': 'never_calculated'
        }
    
    logger.info(f"🔍 REAL-TIME CHANGE DETECTION: Checking scenario '{scenario.version}' against {last_calculated_time}")
    
    changed_models = []
    total_models_checked = 0
    
    # Get all scenario-related models (NO CACHING)
    scenario_models = get_scenario_related_models()
    
    for model_info in scenario_models:
        model = model_info['model']
        version_field = model_info['version_field']
        
        try:
            if version_field:
                # Model has foreign key to scenarios - filter by scenario
                if hasattr(model, version_field):
                    # Use Django ORM to check for changes
                    filter_kwargs = {version_field: scenario}
                    
                    # Check if model has updated_at, modified_at, or similar timestamp field
                    timestamp_fields = []
                    for field in model._meta.get_fields():
                        if field.name in ['updated_at', 'modified_at', 'last_modified', 'timestamp']:
                            timestamp_fields.append(field.name)
                    
                    if timestamp_fields:
                        # Use timestamp field for change detection
                        for ts_field in timestamp_fields:
                            recent_changes = model.objects.filter(
                                **filter_kwargs,
                                **{f'{ts_field}__gt': last_calculated_time}
                            ).exists()
                            
                            if recent_changes:
                                changed_models.append(f"{model_info['app_label']}.{model_info['model_name']} (via {ts_field})")
                                break
                    else:
                        # No timestamp field - cannot reliably detect changes
                        # For models without timestamps, assume NO changes if calculation completed successfully
                        # This prevents false positives that make buttons always green
                        logger.debug(f"⚠️ Model {model_info['model_name']} has no timestamp field - assuming no changes since last calculation")
                        # Do not add to changed_models - we cannot detect changes without timestamps
            
            total_models_checked += 1
            
        except Exception as e:
            logger.warning(f"⚠️  Error checking {model_info['model_name']}: {e}")
            # Don't count failed checks, continue with others
            continue
    
    has_changes = len(changed_models) > 0
    
    logger.info(f"🎯 CHANGE DETECTION COMPLETE: {len(changed_models)} models changed out of {total_models_checked} checked")
    
    return {
        'has_changes': has_changes,
        'changed_models': changed_models,
        'total_models_checked': total_models_checked,
        'check_timestamp': timezone.now(),
        'reason': 'data_changes_detected' if has_changes else 'no_changes'
    }

def mark_calculation_started(scenario):
    """Mark that calculation has started for this scenario"""
    from .models import scenarios
    scenarios.objects.filter(version=scenario.version).update(
        calculation_status='calculating'
    )
    logger.info(f"🚀 Marked scenario '{scenario.version}' as calculating")

def mark_calculation_completed(scenario):
    """Mark that calculation completed successfully for this scenario"""
    from .models import scenarios
    scenarios.objects.filter(version=scenario.version).update(
        last_calculated=timezone.now(),
        calculation_status='up_to_date'
    )
    logger.info(f"✅ Marked scenario '{scenario.version}' as up to date")

def mark_calculation_failed(scenario, error_message=None):
    """Mark that calculation failed for this scenario"""
    from .models import scenarios
    scenarios.objects.filter(version=scenario.version).update(
        calculation_status='calculation_failed'
    )
    logger.error(f"❌ Marked scenario '{scenario.version}' as calculation failed: {error_message}")

def get_calculation_button_state(scenario):
    """
    Determine the state of the calculate model button for a scenario.
    Real-time check - NO CACHING.
    
    Returns:
        dict: {
            'button_class': str,  # 'btn-success' (green) or 'btn-secondary' (grey)
            'button_text': str,
            'button_disabled': bool,
            'tooltip': str,
            'force_clickable': bool  # True if grey button should still be clickable
        }
    """
    
    # Check for changes in real-time
    change_info = check_scenario_data_changes(scenario)
    
    if scenario.calculation_status == 'never_calculated' or scenario.last_calculated is None:
        return {
            'button_class': 'btn-success',
            'button_text': 'Calculate Model',
            'button_disabled': False,
            'tooltip': 'Ready to calculate - click to process scenario data',
            'force_clickable': False
        }
    
    elif scenario.calculation_status == 'calculating':
        return {
            'button_class': 'btn-warning',
            'button_text': 'Calculating...',
            'button_disabled': True,
            'tooltip': 'Model calculation is currently in progress',
            'force_clickable': False
        }
    
    elif scenario.calculation_status == 'calculation_failed':
        return {
            'button_class': 'btn-danger',
            'button_text': 'Retry Calculation',
            'button_disabled': False,
            'tooltip': 'Previous calculation failed - click to retry',
            'force_clickable': False
        }
    
    elif change_info['has_changes']:
        return {
            'button_class': 'btn-success',
            'button_text': 'Calculate Model',
            'button_disabled': False,
            'tooltip': f'Changes detected in: {", ".join(change_info["changed_models"][:3])}{"..." if len(change_info["changed_models"]) > 3 else ""}',
            'force_clickable': False
        }
    
    else:
        # No changes detected - model is up to date
        return {
            'button_class': 'btn-secondary',
            'button_text': 'Model Already Calculated',
            'button_disabled': False,  # Still clickable for force recalculation
            'tooltip': f'Model already calculated and up to date (last calculated: {scenario.last_calculated.strftime("%Y-%m-%d %H:%M") if scenario.last_calculated else "Never"}). Click to force recalculation.',
            'force_clickable': True
        }
