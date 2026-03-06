#!/usr/bin/env python3
"""
Rippling Data Adapter for FP&A App
Pulls headcount data from Rippling HRIS
"""

import requests
import json
import os
from datetime import date, datetime
from collections import defaultdict

API_KEY = "RPAPIV1kbC2Fvidtuf3qhvnL65MD5RBouO30aKnbbhg8WLhykDEWiBOZU7gezJYM"
BASE_URL = "https://api.rippling.com/platform/api"
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def get_employees():
    """Fetch all employees from Rippling API."""
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json"
    }
    response = requests.get(f"{BASE_URL}/employees", headers=headers, timeout=60)
    response.raise_for_status()
    return response.json()


def refresh_headcount():
    """Pull employee data and save to JSON."""
    employees = get_employees()
    
    # Save raw data
    filepath = os.path.join(DATA_DIR, "rippling_employees.json")
    with open(filepath, "w") as f:
        json.dump(employees, f, indent=2)
    
    print(f"✓ Saved {len(employees)} employees to {filepath}")
    return employees


def get_headcount_summary():
    """Load cached employee data and return summary."""
    filepath = os.path.join(DATA_DIR, "rippling_employees.json")
    with open(filepath) as f:
        employees = json.load(f)
    
    active = [e for e in employees if e.get('roleState') == 'ACTIVE']
    
    # Group by department
    by_dept = defaultdict(list)
    for e in active:
        dept = e.get('department', 'Unknown')
        by_dept[dept].append(e)
    
    # Group by high-level function
    functions = {
        'Engineering': ['Infrastructure', 'Python', 'Elixir', 'Java', 'Ruby', 'React', 
                       'Telephony Engineering', 'Mobile Core', 'Network', 'NOC Engineering',
                       'AI Integrations', 'Engineering', 'Telephony'],
        'Sales': ['Account Executives', 'Sales Development', 'Solutions Engineering', 
                 'Sales Operations', 'Sales'],
        'Operations': ['NOC', 'Messaging Operations', 'Porting', 'IT Operations', 'Vendor Relations'],
        'Product': ['Product Management', 'Product Marketing'],
        'Customer Success': ['Customer Success'],
        'Marketing': ['Marketing Events', 'Product Marketing'],
        'G&A': ['Accounting', 'Legal', 'HR', 'Finance']
    }
    
    by_function = defaultdict(int)
    for dept, emps in by_dept.items():
        found = False
        for func, depts in functions.items():
            if dept in depts:
                by_function[func] += len(emps)
                found = True
                break
        if not found:
            by_function['Other'] += len(emps)
    
    return {
        'total': len(active),
        'by_department': {k: len(v) for k, v in sorted(by_dept.items(), key=lambda x: -len(x[1]))},
        'by_function': dict(by_function),
        'generated_at': str(date.today())
    }


def get_headcount_trend():
    """Calculate historical headcount by start date."""
    filepath = os.path.join(DATA_DIR, "rippling_employees.json")
    with open(filepath) as f:
        employees = json.load(f)
    
    # Get all start dates and build cumulative headcount
    events = []
    for e in employees:
        start = e.get('startDate') or e.get('w2StartDate')
        end = e.get('endDate')
        if start:
            events.append((start, 1))  # hire
        if end:
            events.append((end, -1))  # termination
    
    # Sort by date
    events.sort(key=lambda x: x[0])
    
    # Build monthly headcount
    monthly = {}
    running = 0
    for date_str, delta in events:
        month = date_str[:7]  # YYYY-MM
        running += delta
        monthly[month] = running
    
    return monthly


if __name__ == "__main__":
    print("Refreshing headcount data from Rippling...")
    refresh_headcount()
    
    summary = get_headcount_summary()
    print(f"\nTotal Active: {summary['total']}")
    print("\nBy Department:")
    for dept, count in list(summary['by_department'].items())[:10]:
        print(f"  {dept}: {count}")
    
    print("\nBy Function:")
    for func, count in sorted(summary['by_function'].items(), key=lambda x: -x[1]):
        print(f"  {func}: {count}")
