"""
🚫🚫🚫 CRITICAL WARNING TO ALL FUTURE DEVELOPERS 🚫🚫🚫

⚠️ ⚠️ ⚠️  NO FALLBACKS ALLOWED IN SIGNAL HANDLERS  ⚠️ ⚠️ ⚠️
⚠️ ⚠️ ⚠️  NO SIGNAL CACHING OR BATCHING ALLOWED  ⚠️ ⚠️ ⚠️
⚠️ ⚠️ ⚠️  NO DELAYED SIGNAL PROCESSING ALLOWED  ⚠️ ⚠️ ⚠️

CALCULATION TRACKING SIGNALS - REAL-TIME CHANGE DETECTION

This system automatically tracks changes to scenario-related data and marks 
scenarios as needing recalculation when their source data changes.

🔴 ABSOLUTELY PROHIBITED TECHNIQUES: 🔴
- Signal result caching - WILL CAUSE MISSED CHANGES
- Bulk signal processing - WILL DELAY CRITICAL UPDATES
- Signal deferring/queuing - WILL CAUSE STALE STATUS  
- Try/catch blocks that ignore errors - WILL HIDE DATA CORRUPTION
- Conditional signal processing - WILL MISS CRITICAL CHANGES

🟢 REQUIRED BEHAVIOR: 🟢
- All signals MUST be processed immediately in real-time
- Database status updates MUST happen instantly  
- Signal failures MUST be logged but NEVER ignored
- Every data change MUST trigger immediate scenario status updates

IF YOU ADD CACHING OR FALLBACKS TO THESE SIGNALS:
🔥 USERS WILL SEE INCORRECT BUTTON STATES 🔥
🔥 CALCULATIONS WILL BE MISSED OR DUPLICATED 🔥
🔥 DATA INTEGRITY WILL BE COMPROMISED 🔥

When in doubt, ASK - don't add "optimizations"!
"""

from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
from django.utils import timezone
from django.apps import apps
import logging

logger = logging.getLogger(__name__)

def mark_scenario_data_changed(scenario):
    """
    Mark a scenario as having data changes that require recalculation.
    NO CACHING - immediate database update.
    """
    if scenario:
        try:
            from .models import scenarios
            scenarios.objects.filter(version=scenario.version).update(
                calculation_status='changes_detected'
            )
            logger.info(f"🔄 Marked scenario '{scenario.version}' as changes detected")
        except Exception as e:
            logger.error(f"❌ Failed to mark scenario data changed: {e}")

# Track changes to all models that have version/scenario fields
@receiver([post_save, post_delete])
def track_scenario_data_changes(sender, instance, **kwargs):
    """
    Signal receiver to track changes in any model with version/scenario fields.
    Automatically marks related scenarios as needing recalculation.
    """
    
    # Skip if this is the scenarios model itself to avoid recursive updates
    if sender.__name__ == 'scenarios':
        return
    
    # Check if the model has version or scenario fields
    version_field = None
    scenario_field = None
    
    for field in sender._meta.get_fields():
        if 'version' in field.name.lower() and hasattr(field, 'related_model'):
            if field.related_model and field.related_model.__name__ == 'scenarios':
                version_field = field.name
                break
        elif 'scenario' in field.name.lower() and hasattr(field, 'related_model'):
            if field.related_model and field.related_model.__name__ == 'scenarios':
                scenario_field = field.name
                break
    
    # Get the related scenario
    scenario = None
    try:
        if version_field and hasattr(instance, version_field):
            scenario = getattr(instance, version_field)
        elif scenario_field and hasattr(instance, scenario_field):
            scenario = getattr(instance, scenario_field)
        
        if scenario:
            logger.debug(f"📝 Data change detected in {sender.__name__} for scenario '{scenario.version}'")
            mark_scenario_data_changed(scenario)
        else:
            # Check if the instance itself has a version attribute that's a string
            if hasattr(instance, 'version') and isinstance(instance.version, str):
                from .models import scenarios
                try:
                    scenario = scenarios.objects.get(version=instance.version)
                    mark_scenario_data_changed(scenario)
                except scenarios.DoesNotExist:
                    pass
    
    except Exception as e:
        logger.debug(f"⚠️  Could not track changes for {sender.__name__}: {e}")
        # Don't fail the original operation if tracking fails
        pass
