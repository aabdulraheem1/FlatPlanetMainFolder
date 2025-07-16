from django import template

register = template.Library()

@register.filter(name='add_class')
def add_class(field, css_class):
    """
    Add CSS class to form field
    """
    return field.as_widget(attrs={'class': css_class})