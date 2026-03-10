#!/usr/bin/env python3
"""
Verification Script for Vimeo Live Event Classifier v3

This script verifies that all components are working correctly.
"""

import sys
from pathlib import Path

def check_file_exists(filepath, description):
    """Check if a file exists."""
    if Path(filepath).exists():
        print(f"✓ {description}")
        return True
    else:
        print(f"✗ {description} - FILE NOT FOUND")
        return False

def check_json_valid(filepath, description):
    """Check if a JSON file is valid."""
    try:
        import json
        with open(filepath, 'r') as f:
            json.load(f)
        print(f"✓ {description}")
        return True
    except Exception as e:
        print(f"✗ {description} - INVALID JSON: {e}")
        return False

def check_python_syntax(filepath, description):
    """Check if Python file has valid syntax."""
    try:
        with open(filepath, 'r') as f:
            compile(f.read(), filepath, 'exec')
        print(f"✓ {description}")
        return True
    except SyntaxError as e:
        print(f"✗ {description} - SYNTAX ERROR: {e}")
        return False

def check_import(module_name, description):
    """Check if a Python module can be imported."""
    try:
        __import__(module_name)
        print(f"✓ {description}")
        return True
    except ImportError as e:
        print(f"✗ {description} - IMPORT ERROR: {e}")
        return False

def main():
    """Run all verification checks."""
    print("\n" + "="*60)
    print("VIMEO CLASSIFIER v3 - VERIFICATION SCRIPT")
    print("="*60 + "\n")
    
    all_passed = True
    
    print("Checking Files...")
    print("-" * 60)
    all_passed &= check_file_exists("automaton_v3.py", "Main script exists")
    all_passed &= check_file_exists("review_videos.py", "Review tool exists")
    all_passed &= check_file_exists("live_events_config.json", "Config file exists")
    all_passed &= check_file_exists("review_queue.json", "Review queue exists")
    all_passed &= check_file_exists("run_classification.sh", "Cron script exists")
    all_passed &= check_file_exists("README_v3.md", "Documentation exists")
    all_passed &= check_file_exists("QUICKSTART.md", "Quick start guide exists")
    all_passed &= check_file_exists("logs/.gitkeep", "Logs directory exists")
    
    print("\nValidating JSON...")
    print("-" * 60)
    all_passed &= check_json_valid("live_events_config.json", "Configuration is valid JSON")
    all_passed &= check_json_valid("review_queue.json", "Review queue is valid JSON")
    
    print("\nChecking Python Syntax...")
    print("-" * 60)
    all_passed &= check_python_syntax("automaton_v3.py", "Main script syntax OK")
    all_passed &= check_python_syntax("review_videos.py", "Review tool syntax OK")
    
    print("\nChecking Dependencies...")
    print("-" * 60)
    all_passed &= check_import("vimeo", "PyVimeo installed")
    all_passed &= check_import("pytz", "pytz installed")
    all_passed &= check_import("dotenv", "python-dotenv installed")
    
    print("\nChecking Configuration...")
    print("-" * 60)
    try:
        from automaton_v3 import load_config
        config = load_config()
        print(f"✓ Configuration loads successfully")
        print(f"  - {len(config['events'])} events configured")
        print(f"  - Timezone: {config['settings']['timezone']}")
        print(f"  - Lookback: {config['settings']['lookback_hours']} hours")
        print(f"  - Min confidence: {config['settings']['min_confidence']}")
        print(f"  - Email enabled: {config['notifications']['email']['enabled']}")
    except Exception as e:
        print(f"✗ Configuration error: {e}")
        all_passed = False
    
    print("\nChecking Environment Variables...")
    print("-" * 60)
    import os
    from dotenv import load_dotenv
    load_dotenv()
    
    vimeo_token = os.environ.get("VIMEO_ACCESS_TOKEN")
    vimeo_id = os.environ.get("VIMEO_CLIENT_ID")
    vimeo_secret = os.environ.get("VIMEO_CLIENT_SECRET")
    
    if vimeo_token and vimeo_id and vimeo_secret:
        print("✓ Vimeo credentials configured")
    else:
        print("✗ Vimeo credentials missing in .env file")
        all_passed = False
    
    print("\n" + "="*60)
    if all_passed:
        print("✓ ALL CHECKS PASSED!")
        print("="*60)
        print("\nYou're ready to use the classifier!")
        print("\nNext steps:")
        print("  1. Add Gmail App Password to live_events_config.json")
        print("  2. Run: python3 automaton_v3.py --dry-run --verbose")
        print("  3. Run: python3 automaton_v3.py --verbose")
        print("\nFor help, see QUICKSTART.md or README_v3.md")
    else:
        print("✗ SOME CHECKS FAILED")
        print("="*60)
        print("\nPlease fix the errors above before proceeding.")
    print()

if __name__ == "__main__":
    main()
