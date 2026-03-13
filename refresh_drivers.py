#!/usr/bin/env python3
"""
Refresh driver data from the data warehouse.
Run this script to update data/drivers.json before deploying.
"""

import psycopg2
import json
import os
from datetime import date

DB_CONFIG = {
    "host": "10.5.224.24",
    "database": "domo_warehouse",
    "user": "domo",
    "password": os.environ.get("DOMO_PASSWORD", "Q3ugHwECA8ehpq"),
}

def refresh_drivers():
    print("Connecting to data warehouse...")
    conn = psycopg2.connect(**DB_CONFIG, connect_timeout=60)
    cur = conn.cursor()

    current_year = date.today().year
    last_year = current_year - 1

    # Get last year annual totals
    print(f"Fetching {last_year} annual data...")
    cur.execute("""
        SELECT product_category, SUM(value) as revenue
        FROM finance."CSM_Rev_GP_Monthly" 
        WHERE product_category != 'Total'
          AND metric_type = 'CSM Revenue'
          AND EXTRACT(YEAR FROM month_year) = %s
        GROUP BY product_category
    """, (last_year,))
    last_year_data = {row[0]: round(float(row[1]), 2) for row in cur.fetchall()}

    # Get current year YTD
    print(f"Fetching {current_year} YTD data...")
    cur.execute("""
        SELECT product_category, SUM(value) as revenue
        FROM finance."CSM_Rev_GP_Monthly" 
        WHERE product_category != 'Total'
          AND metric_type = 'CSM Revenue'
          AND EXTRACT(YEAR FROM month_year) = %s
        GROUP BY product_category
    """, (current_year,))
    current_year_data = {row[0]: round(float(row[1]), 2) for row in cur.fetchall()}

    # Get latest month
    cur.execute("SELECT MAX(month_year) FROM finance.\"CSM_Rev_GP_Monthly\"")
    latest_month = cur.fetchone()[0]
    print(f"Latest month: {latest_month}")

    cur.execute("""
        SELECT product_category, SUM(value) as revenue
        FROM finance."CSM_Rev_GP_Monthly" 
        WHERE product_category != 'Total'
          AND metric_type = 'CSM Revenue'
          AND month_year = %s
        GROUP BY product_category
    """, (latest_month,))
    latest_month_data = {row[0]: round(float(row[1]), 2) for row in cur.fetchall()}

    # Calculate growth rates (annualized)
    growth_rates = {}
    months_elapsed = latest_month.month
    for product in last_year_data:
        if product in current_year_data and last_year_data[product] > 0:
            annualized = current_year_data[product] * (12 / months_elapsed)
            growth = ((annualized - last_year_data[product]) / last_year_data[product]) * 100
            growth_rates[product] = round(growth, 1)

    # Build driver data
    product_map = {
        "Voice Products": "voice",
        "Messaging Products": "messaging", 
        "Number Products": "numbers",
        "AI Products": "ai",
        "Connectivity Products": "connectivity",
        "Identity Products": "identity",
        "Storage Products": "storage",
        "Support Products": "support",
        "Other Products": "other"
    }

    drivers = {}
    for product, key in product_map.items():
        drivers[key] = {
            "name": product,
            "lastYearActual": last_year_data.get(product, 0),
            "ytdActual": current_year_data.get(product, 0),
            "latestMonth": latest_month_data.get(product, 0),
            "yoyGrowth": growth_rates.get(product, 0)
        }

    data = {
        "lastYear": last_year,
        "currentYear": current_year,
        "latestMonth": latest_month.strftime("%Y-%m"),
        "drivers": drivers,
        "totals": {
            "lastYear": round(sum(last_year_data.values()), 2),
            "ytd": round(sum(current_year_data.values()), 2),
            "latestMonth": round(sum(latest_month_data.values()), 2)
        },
        "generated_at": str(date.today())
    }

    # Save to data directory
    os.makedirs("data", exist_ok=True)
    with open("data/drivers.json", "w") as f:
        json.dump(data, f, indent=2)

    conn.close()

    print()
    print("✓ Generated data/drivers.json")
    print(f"  {last_year} Total: ${data['totals']['lastYear']:,.0f}")
    print(f"  {current_year} YTD:  ${data['totals']['ytd']:,.0f}")
    print(f"  Latest Month: ${data['totals']['latestMonth']:,.0f}")
    print()
    print("By Product:")
    for key, driver in sorted(drivers.items(), key=lambda x: -x[1]['lastYearActual']):
        print(f"  {driver['name']:25} ${driver['lastYearActual']/1e6:>6.1f}M  {driver['yoyGrowth']:+5.1f}%")

    return data


if __name__ == "__main__":
    refresh_drivers()
