from django import template
register = template.Library()

@register.filter
def get_item(dictionary, key):
    if dictionary is None:
        return None
    return dictionary.get(key)

@register.simple_tag
def warehouse_cost(lookup, product_pk, site_pk):
    if lookup is None:
        return None
    return lookup.get((product_pk, site_pk))