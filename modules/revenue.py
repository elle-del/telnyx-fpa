"""
Revenue Module for FP&A App
Provides revenue analysis and reporting functions
"""

import json
import os
from datetime import datetime
from typing import Optional

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def load_revenue_data() -> dict:
    """Load revenue by type data."""
    filepath = os.path.join(DATA_DIR, "revenue_by_type.json")
    with open(filepath, "r") as f:
        return json.load(f)


def get_monthly_summary(year: Optional[int] = None) -> list:
    """
    Get monthly revenue summary with Base/Variable/Political breakdown.
    Returns list of dicts with month, base, variable, political, total.
    """
    data = load_revenue_data()
    
    # Pivot by month
    months = {}
    for row in data["monthly_revenue"]:
        month = row["month"]
        if year and not month.startswith(str(year)):
            continue
        
        if month not in months:
            months[month] = {
                "month": month,
                "base": 0,
                "variable": 0,
                "political": 0,
                "total": 0
            }
        
        rev_type = row["revenue_type"].lower()
        if rev_type in months[month]:
            months[month][rev_type] = row["revenue"]
        months[month]["total"] += row["revenue"]
    
    return sorted(months.values(), key=lambda x: x["month"])


def get_ytd_summary(year: int) -> dict:
    """Get year-to-date revenue summary."""
    monthly = get_monthly_summary(year)
    
    return {
        "year": year,
        "months": len(monthly),
        "base": sum(m["base"] for m in monthly),
        "variable": sum(m["variable"] for m in monthly),
        "political": sum(m["political"] for m in monthly),
        "total": sum(m["total"] for m in monthly)
    }


def get_yoy_comparison(year1: int, year2: int) -> dict:
    """Compare two years of revenue."""
    ytd1 = get_ytd_summary(year1)
    ytd2 = get_ytd_summary(year2)
    
    return {
        "year1": ytd1,
        "year2": ytd2,
        "change": {
            "base": ytd2["base"] - ytd1["base"],
            "variable": ytd2["variable"] - ytd1["variable"],
            "total": ytd2["total"] - ytd1["total"],
            "pct_change": ((ytd2["total"] - ytd1["total"]) / ytd1["total"] * 100) if ytd1["total"] else 0
        }
    }


def print_revenue_report(year: Optional[int] = None):
    """Print a formatted revenue report."""
    monthly = get_monthly_summary(year)
    
    print("\n" + "=" * 80)
    print(f"REVENUE BY TYPE - {'All Time' if not year else year}")
    print("=" * 80)
    print(f"{'Month':<12} {'Base':>15} {'Variable':>15} {'Political':>12} {'Total':>15}")
    print("-" * 80)
    
    for m in monthly:
        print(f"{m['month']:<12} ${m['base']:>14,.0f} ${m['variable']:>14,.0f} ${m['political']:>11,.0f} ${m['total']:>14,.0f}")
    
    # Totals
    print("-" * 80)
    total_base = sum(m["base"] for m in monthly)
    total_var = sum(m["variable"] for m in monthly)
    total_pol = sum(m["political"] for m in monthly)
    total_all = sum(m["total"] for m in monthly)
    
    print(f"{'TOTAL':<12} ${total_base:>14,.0f} ${total_var:>14,.0f} ${total_pol:>11,.0f} ${total_all:>14,.0f}")
    print("=" * 80)
    
    # Percentages
    if total_all > 0:
        print(f"{'% of Total':<12} {total_base/total_all*100:>14.1f}% {total_var/total_all*100:>14.1f}% {total_pol/total_all*100:>11.1f}%")


if __name__ == "__main__":
    print_revenue_report(2024)
    print_revenue_report(2025)
    print_revenue_report(2026)
