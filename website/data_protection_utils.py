"""
Data Protection Utilities for preserving user modifications during Epicor data refresh
"""
from django.utils import timezone
from django.contrib.auth.models import User


def mark_field_as_user_modified(instance, field_name, username=None):
    """
    Mark a specific field as user-modified to prevent it from being overwritten during data refresh
    
    Args:
        instance: Model instance (Plant, Product, Supplier, Customer)
        field_name: Name of the field that was modified by user
        username: Username who made the modification
    """
    if not hasattr(instance, 'user_modified_fields'):
        return False
    
    # Initialize user_modified_fields if it's None or empty
    if not instance.user_modified_fields:
        instance.user_modified_fields = {}
    
    # Mark the field as user-modified with timestamp
    instance.user_modified_fields[field_name] = {
        'modified_by': username,
        'modified_date': timezone.now().isoformat(),
        'protected': True
    }
    
    # Update tracking fields
    instance.last_modified_by_user = username
    instance.last_user_modification_date = timezone.now()
    
    return True


def mark_record_as_user_created(instance, username=None):
    """
    Mark an entire record as user-created to prevent deletion during data refresh
    
    Args:
        instance: Model instance (Plant, Product, Supplier, Customer)
        username: Username who created the record
    """
    if not hasattr(instance, 'is_user_created'):
        return False
    
    instance.is_user_created = True
    instance.created_by_user = username
    instance.last_modified_by_user = username
    instance.last_user_modification_date = timezone.now()
    
    return True


def is_field_protected(instance, field_name):
    """
    Check if a field is protected from being overwritten by data refresh
    
    Args:
        instance: Model instance
        field_name: Name of the field to check
        
    Returns:
        bool: True if field is protected, False otherwise
    """
    if not hasattr(instance, 'user_modified_fields') or not instance.user_modified_fields:
        return False
    
    field_info = instance.user_modified_fields.get(field_name, {})
    return field_info.get('protected', False)


def safe_update_from_epicor(instance, epicor_data, username=None):
    """
    Safely update model instance from Epicor data while preserving user modifications
    
    Args:
        instance: Model instance to update
        epicor_data: Dictionary of field values from Epicor
        username: Username performing the import (optional)
        
    Returns:
        dict: Summary of what was updated, skipped, and protected
    """
    updated_fields = []
    protected_fields = []
    skipped_fields = []
    
    for field_name, new_value in epicor_data.items():
        # Skip if field doesn't exist on model
        if not hasattr(instance, field_name):
            skipped_fields.append(field_name)
            continue
            
        # Skip tracking fields
        if field_name in ['is_user_created', 'last_imported_from_epicor', 'user_modified_fields', 
                         'created_by_user', 'last_modified_by_user', 'last_user_modification_date']:
            continue
            
        # Check if field is protected by user modification
        if is_field_protected(instance, field_name):
            protected_fields.append(field_name)
            continue
            
        # Update the field
        setattr(instance, field_name, new_value)
        updated_fields.append(field_name)
    
    # Update import tracking
    instance.last_imported_from_epicor = timezone.now()
    
    return {
        'updated_fields': updated_fields,
        'protected_fields': protected_fields,
        'skipped_fields': skipped_fields
    }


def get_user_created_records(model_class):
    """
    Get all records that were created manually by users (should not be deleted during refresh)
    
    Args:
        model_class: Model class (MasterDataPlantModel, etc.)
        
    Returns:
        QuerySet: Records created by users
    """
    if not hasattr(model_class, '_meta'):
        return model_class.objects.none()
    
    # Check if model has user tracking fields
    field_names = [f.name for f in model_class._meta.fields]
    if 'is_user_created' not in field_names:
        return model_class.objects.none()
    
    return model_class.objects.filter(is_user_created=True)


def get_epicor_managed_records(model_class):
    """
    Get all records that are managed by Epicor (can be deleted/updated during refresh)
    
    Args:
        model_class: Model class (MasterDataPlantModel, etc.)
        
    Returns:
        QuerySet: Records managed by Epicor
    """
    if not hasattr(model_class, '_meta'):
        return model_class.objects.none()
    
    # Check if model has user tracking fields
    field_names = [f.name for f in model_class._meta.fields]
    if 'is_user_created' not in field_names:
        return model_class.objects.all()  # If no tracking, assume all are Epicor-managed
    
    return model_class.objects.filter(is_user_created=False)


def create_data_refresh_summary(model_updates):
    """
    Create a summary report of data refresh operations
    
    Args:
        model_updates: Dict of model updates {model_name: {updated: [], protected: [], created: [], deleted: []}}
        
    Returns:
        str: Formatted summary report
    """
    summary = []
    summary.append("=" * 60)
    summary.append("DATA REFRESH SUMMARY")
    summary.append("=" * 60)
    
    for model_name, operations in model_updates.items():
        summary.append(f"\n📊 {model_name}:")
        summary.append(f"   ✅ Updated: {len(operations.get('updated', []))} records")
        summary.append(f"   🛡️  Protected: {len(operations.get('protected', []))} records")
        summary.append(f"   ➕ Created: {len(operations.get('created', []))} records")
        summary.append(f"   ❌ Deleted: {len(operations.get('deleted', []))} records")
        
        if operations.get('protected'):
            summary.append("   🛡️  Protected records:")
            for record in operations['protected'][:5]:  # Show first 5
                summary.append(f"      - {record}")
            if len(operations['protected']) > 5:
                summary.append(f"      ... and {len(operations['protected']) - 5} more")
    
    summary.append("\n" + "=" * 60)
    summary.append("✅ Data refresh completed with user modifications preserved!")
    
    return "\n".join(summary)


# Example usage functions for forms

def handle_plant_form_save(form, request):
    """
    Handle plant form save with user modification tracking
    """
    instance = form.save(commit=False)
    
    # Track which fields were modified by user
    if form.changed_data:
        username = request.user.username if request.user.is_authenticated else 'Anonymous'
        
        for field_name in form.changed_data:
            mark_field_as_user_modified(instance, field_name, username)
        
        # If this is a new record, mark as user-created
        if not instance.pk:
            mark_record_as_user_created(instance, username)
    
    instance.save()
    return instance


def handle_product_form_save(form, request):
    """
    Handle product form save with user modification tracking
    """
    instance = form.save(commit=False)
    
    # Track which fields were modified by user
    if form.changed_data:
        username = request.user.username if request.user.is_authenticated else 'Anonymous'
        
        for field_name in form.changed_data:
            mark_field_as_user_modified(instance, field_name, username)
        
        # If this is a new record, mark as user-created
        if not instance.pk:
            mark_record_as_user_created(instance, username)
    
    instance.save()
    return instance
