"""
Postgres Data Adapter for FP&A App
Pulls revenue data from domo_warehouse
"""

import psycopg2
import json
import os
from datetime import date
from typing import Optional

# Connection config
DB_CONFIG = {
    "host": "10.5.224.24",
    "database": "domo_warehouse",
    "user": "domo",
    "password": "Q3ugHwECA8ehpq",
}

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


def get_connection():
    return psycopg2.connect(**DB_CONFIG, connect_timeout=60)


def refresh_revenue_by_type(start_date: str = "2024-01-01") -> dict:
    """
    Pull revenue by type (Base/Variable/Political) from Postgres.
    Uses mission_control_monthly_revenue + accounts_consolidated.
    """
    conn = get_connection()
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '300s'")

    query = """
        SELECT 
            DATE_TRUNC('month', r.month)::date as month,
            COALESCE(a.rev_type::text, 'BASE') as revenue_type,
            SUM(r.month_revenue) as revenue,
            COUNT(DISTINCT r.user_id) as account_count
        FROM public.mission_control_monthly_revenue r
        LEFT JOIN public.accounts_consolidated a ON r.user_id = a.user_id
        WHERE r.month >= %s
        GROUP BY DATE_TRUNC('month', r.month), COALESCE(a.rev_type::text, 'BASE')
        ORDER BY month, revenue_type
    """

    cur.execute(query, (start_date,))
    rows = cur.fetchall()
    conn.close()

    data = {
        "source": "postgres",
        "table": "mission_control_monthly_revenue + accounts_consolidated",
        "generated_at": str(date.today()),
        "monthly_revenue": []
    }

    for row in rows:
        data["monthly_revenue"].append({
            "month": row[0].isoformat(),
            "revenue_type": row[1],
            "revenue": round(float(row[2]), 2),
            "account_count": row[3]
        })

    # Save to file
    os.makedirs(DATA_DIR, exist_ok=True)
    filepath = os.path.join(DATA_DIR, "revenue_by_type.json")
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    print(f"✓ Saved {len(rows)} rows to {filepath}")
    return data


def get_revenue_by_type() -> dict:
    """Load cached revenue data from JSON file."""
    filepath = os.path.join(DATA_DIR, "revenue_by_type.json")
    with open(filepath, "r") as f:
        return json.load(f)


def get_monthly_totals() -> list:
    """Get monthly revenue totals (all types combined)."""
    data = get_revenue_by_type()
    
    # Aggregate by month
    totals = {}
    for row in data["monthly_revenue"]:
        month = row["month"]
        if month not in totals:
            totals[month] = {"month": month, "revenue": 0, "accounts": 0}
        totals[month]["revenue"] += row["revenue"]
        totals[month]["accounts"] += row["account_count"]
    
    return sorted(totals.values(), key=lambda x: x["month"])


def get_revenue_by_type_pivot() -> dict:
    """Pivot revenue data: {month: {BASE: x, VARIABLE: y, POLITICAL: z, TOTAL: t}}"""
    data = get_revenue_by_type()
    
    pivot = {}
    for row in data["monthly_revenue"]:
        month = row["month"]
        if month not in pivot:
            pivot[month] = {"month": month, "BASE": 0, "VARIABLE": 0, "POLITICAL": 0, "TOTAL": 0}
        pivot[month][row["revenue_type"]] = row["revenue"]
        pivot[month]["TOTAL"] += row["revenue"]
    
    return pivot


if __name__ == "__main__":
    print("Refreshing revenue data from Postgres...")
    refresh_revenue_by_type()
