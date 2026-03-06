#!/usr/bin/env python3
"""
Telnyx FP&A App - Main Entry Point
"""

import sys
import os

# Add modules to path
sys.path.insert(0, os.path.dirname(__file__))

from modules.revenue import print_revenue_report, get_monthly_summary, get_ytd_summary


def main():
    print("\n" + "=" * 80)
    print("  TELNYX FP&A APP - Revenue Dashboard")
    print("  Data Source: Postgres (mission_control_monthly_revenue)")
    print("=" * 80)
    
    # 2025 Full Year
    print_revenue_report(2025)
    
    # 2026 YTD
    print_revenue_report(2026)
    
    # YTD Comparison
    print("\n" + "=" * 80)
    print("  YEAR-TO-DATE COMPARISON")
    print("=" * 80)
    
    ytd_2025 = get_ytd_summary(2025)
    ytd_2026 = get_ytd_summary(2026)
    
    # Compare same months (Jan-Mar for 2026 vs Jan-Mar 2025)
    monthly_2025 = get_monthly_summary(2025)[:3]  # First 3 months
    monthly_2026 = get_monthly_summary(2026)
    
    sum_2025 = sum(m["total"] for m in monthly_2025)
    sum_2026 = sum(m["total"] for m in monthly_2026)
    
    print(f"\nJan-Mar 2025: ${sum_2025:,.0f}")
    print(f"Jan-Mar 2026: ${sum_2026:,.0f}")
    if sum_2025 > 0:
        pct_change = (sum_2026 - sum_2025) / sum_2025 * 100
        print(f"YoY Change:   {'+' if pct_change > 0 else ''}{pct_change:.1f}%")
    
    print("\n✓ Data loaded successfully")
    print("  Run 'python adapters/postgres_adapter.py' to refresh data from Postgres")


if __name__ == "__main__":
    main()
