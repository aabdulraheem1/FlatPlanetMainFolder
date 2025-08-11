#!/usr/bin/env python3
"""
Quick Django Shell Access Script
Automatically sets up environment and opens Django shell
"""

import os
import sys
import subprocess

# Change to the correct directory
spr_dir = r"C:\Users\aali\OneDrive - bradken.com\Data\Training\SPR\spr"
os.chdir(spr_dir)

# Set Django settings
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'SPR.settings')

print(f"🐍 Starting Django shell from: {spr_dir}")
print(f"🔧 Using settings: SPR.settings")
print(f"📁 Templates directory: ../templates/")
print()

# Run Django shell
subprocess.run([sys.executable, 'manage.py', 'shell'])
