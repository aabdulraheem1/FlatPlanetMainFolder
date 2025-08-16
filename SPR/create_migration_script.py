#!/usr/bin/env python
"""
Script to create Django migration with automated responses to prompts.
This handles the interactive prompts for default values.
"""
import os
import sys
import subprocess
from io import StringIO

def create_migration():
    """Create migration with automated responses"""
    try:
        # Prepare the responses
        # Response 1: Select option 1 (provide one-off default)
        # Response 2: Accept default timezone.now by pressing Enter
        # Response 3: Accept default timezone.now for updated_at field by pressing Enter
        responses = "1\n\n\n"
        
        # Run the makemigrations command with automated input
        process = subprocess.Popen(
            [sys.executable, 'manage.py', 'makemigrations', '--name', 'add_production_allocation_model'],
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            cwd=os.getcwd()
        )
        
        # Send the responses
        stdout, stderr = process.communicate(input=responses)
        
        print("STDOUT:")
        print(stdout)
        
        if stderr:
            print("STDERR:")
            print(stderr)
        
        return process.returncode == 0
        
    except Exception as e:
        print(f"Error creating migration: {e}")
        return False

if __name__ == "__main__":
    success = create_migration()
    if success:
        print("Migration created successfully!")
    else:
        print("Migration creation failed!")
        sys.exit(1)
