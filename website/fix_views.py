#!/usr/bin/env python3
"""
Script to fix corrupted null bytes in views.py
"""

# Read the corrupted file
with open('views.py', 'rb') as f:
    data = f.read()

print(f"Original file size: {len(data)} bytes")

# Find the last good position before the corrupted comment
# Look for the last occurrence of the return statement
good_end = data.rfind(b"return JsonResponse({'error': str(e)}, status=500)")
if good_end != -1:
    # Add the length of that line
    good_end += len(b"return JsonResponse({'error': str(e)}, status=500)")
    clean_data = data[:good_end]
    
    # Write the clean file
    with open('views.py', 'wb') as f:
        f.write(clean_data)
    
    print(f"Cleaned file size: {len(clean_data)} bytes")
    print("Removed corrupted null bytes from views.py")
else:
    print("Could not find the return statement to clean from")
