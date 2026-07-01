#!/usr/bin/env python3
"""
AI Chat Module - Pattern-based financial Q&A engine
Accepts natural language questions, maps to SQL queries, returns structured answers.
"""

import os
import re
import json
import psycopg2
from datetime import date, datetime
from calendar import month_name, month_abbr

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "10.5.224.24"),
    "database": os.environ.get("DB_NAME", "domo_warehouse"),
    "user": os.environ.get("DB_USER", "domo"),
    "password": os.environ.get("DB_PASSWORD", "Q3ugHwECA8ehpq"),
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG, connect_timeout=60)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

MONTH_MAP = {}
for i in range(1, 13):
    MONTH_MAP[month_name[i].lower()] = i
    MONTH_MAP[month_abbr[i].lower()] = i

def _fmt_money(val):
    """Format a number as a readable dollar amount."""
    if val is None:
        return "$0"
    abs_val = abs(val)
    if abs_val >= 1_000_000_000:
        return f"${val / 1_000_000_000:,.1f}B"
    if abs_val >= 1_000_000:
        return f"${val / 1_000_000:,.1f}M"
    if abs_val >= 1_000:
        return f"${val / 1_000:,.1f}K"
    return f"${val:,.2f}"


def _fmt_pct(val):
    return f"{val:+.1f}%" if val else "0.0%"


def _parse_month_year(text):
    """Try to extract a (month_number, year) from text like 'january 2025' or 'jan 2025' or 'last month'."""
    text_lower = text.lower()

    # "last month"
    if "last month" in text_lower:
        today = date.today()
        m = today.month - 1 if today.month > 1 else 12
        y = today.year if today.month > 1 else today.year - 1
        return m, y

    # "this month"
    if "this month" in text_lower:
        today = date.today()
        return today.month, today.year

    # Explicit month + year  e.g. "january 2025", "jan 2025", "march 26"
    for mname, mnum in MONTH_MAP.items():
        pattern = rf'\b{mname}\s+(\d{{2,4}})\b'
        match = re.search(pattern, text_lower)
        if match:
            yr = int(match.group(1))
            if yr < 100:
                yr += 2000
            return mnum, yr

    return None, None


def _parse_quarter_year(text):
    """Extract quarter (1-4) and year from text like 'Q1 2025'."""
    text_lower = text.lower()
    match = re.search(r'q(\d)\s*(\d{2,4})', text_lower)
    if match:
        q = int(match.group(1))
        yr = int(match.group(2))
        if yr < 100:
            yr += 2000
        return q, yr
    return None, None


def _parse_n_months(text):
    """Extract N from 'last N months' or 'past N months'."""
    match = re.search(r'(?:last|past)\s+(\d+)\s+months?', text.lower())
    if match:
        return int(match.group(1))
    return None


def _parse_year(text):
    """Extract a standalone year from text."""
    match = re.search(r'\b(20\d{2})\b', text)
    if match:
        return int(match.group(1))
    return None


# ---------------------------------------------------------------------------
# Query Handlers
# ---------------------------------------------------------------------------

def _handle_total_revenue_month(question):
    """Handle: 'What was total revenue last month / in January 2025?'"""
    m, y = _parse_month_year(question)
    if m is None:
        return None

    conn = get_connection()
    cur = conn.cursor()

    month_start = f"{y}-{m:02d}-01"

    # Try mission_control first
    cur.execute("""
        SELECT SUM(month_revenue), COUNT(DISTINCT user_id)
        FROM public.mission_control_monthly_revenue
        WHERE DATE_TRUNC('month', month) = %s::date
    """, (month_start,))
    row = cur.fetchone()
    mc_rev = float(row[0]) if row[0] else None
    mc_cust = int(row[1]) if row[1] else 0

    # Also get CSM total
    cur.execute("""
        SELECT SUM(value)
        FROM finance."CSM_Rev_GP_Monthly"
        WHERE metric_type = 'CSM Revenue'
          AND product_category = 'Total'
          AND DATE_TRUNC('month', month_year) = %s::date
    """, (month_start,))
    csm_row = cur.fetchone()
    csm_rev = float(csm_row[0]) if csm_row[0] else None

    conn.close()

    rev = csm_rev if csm_rev else mc_rev
    if rev is None:
        month_label = f"{month_name[m]} {y}"
        return {
            "answer": f"I don't have revenue data for {month_label}.",
            "data": {},
            "chartType": "none"
        }

    month_label = f"{month_name[m]} {y}"
    answer = f"Total revenue for **{month_label}** was **{_fmt_money(rev)}**."
    if mc_cust:
        answer += f" There were **{mc_cust:,}** unique customers."

    return {
        "answer": answer,
        "data": {
            "month": month_label,
            "revenue": round(rev, 2),
            "customers": mc_cust
        },
        "chartType": "none"
    }


def _handle_revenue_trend(question):
    """Handle: 'Show revenue trend for the last N months'"""
    n = _parse_n_months(question)
    if n is None:
        n = 6  # default

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT DATE_TRUNC('month', month) as mon, SUM(month_revenue)
        FROM public.mission_control_monthly_revenue
        WHERE month >= NOW() - INTERVAL '%s months'
        GROUP BY 1 ORDER BY 1
    """ % n)

    labels = []
    values = []
    for row in cur.fetchall():
        labels.append(row[0].strftime("%b %Y"))
        values.append(round(float(row[1]), 2))

    conn.close()

    if not values:
        return {
            "answer": f"No revenue data available for the last {n} months.",
            "data": {},
            "chartType": "none"
        }

    first = values[0]
    last = values[-1]
    change = ((last - first) / first * 100) if first else 0

    direction = "📈 increased" if change > 0 else "📉 decreased"
    answer = f"Revenue trend over the last **{n} months**: {direction} by **{abs(change):.1f}%** from {_fmt_money(first)} to {_fmt_money(last)}."

    return {
        "answer": answer,
        "data": {
            "labels": labels,
            "values": values
        },
        "chartType": "line"
    }


def _handle_fastest_growing_category(question):
    """Handle: 'Which product category is growing fastest?'"""
    conn = get_connection()
    cur = conn.cursor()

    # Compare latest full month vs 12 months prior
    cur.execute("""
        WITH latest AS (
            SELECT MAX(DATE_TRUNC('month', month_year)) as latest_month
            FROM finance."CSM_Rev_GP_Monthly"
        ),
        current_month AS (
            SELECT product_category, SUM(value) as rev
            FROM finance."CSM_Rev_GP_Monthly"
            WHERE metric_type = 'CSM Revenue'
              AND product_category NOT IN ('Total')
              AND DATE_TRUNC('month', month_year) = (SELECT latest_month FROM latest)
            GROUP BY product_category
        ),
        prior_month AS (
            SELECT product_category, SUM(value) as rev
            FROM finance."CSM_Rev_GP_Monthly"
            WHERE metric_type = 'CSM Revenue'
              AND product_category NOT IN ('Total')
              AND DATE_TRUNC('month', month_year) = (SELECT latest_month - INTERVAL '12 months' FROM latest)
            GROUP BY product_category
        )
        SELECT c.product_category,
               c.rev as current_rev,
               p.rev as prior_rev,
               CASE WHEN p.rev > 0 THEN ((c.rev - p.rev) / p.rev * 100) ELSE NULL END as growth_pct
        FROM current_month c
        JOIN prior_month p ON c.product_category = p.product_category
        WHERE p.rev > 0
        ORDER BY growth_pct DESC NULLS LAST
    """)

    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {
            "answer": "Unable to calculate growth rates — not enough historical data.",
            "data": {},
            "chartType": "none"
        }

    labels = []
    values = []
    for row in rows:
        labels.append(row[0])
        values.append(round(float(row[3]), 1) if row[3] else 0)

    fastest = rows[0]
    answer = (
        f"🚀 The fastest-growing product category is **{fastest[0]}** "
        f"with **{fastest[3]:+.1f}% YoY growth** "
        f"(from {_fmt_money(fastest[2])} to {_fmt_money(fastest[1])} per month)."
    )

    if len(rows) > 1:
        answer += "\n\nAll category growth rates (YoY):"
        for row in rows:
            emoji = "📈" if row[3] and row[3] > 0 else "📉"
            answer += f"\n• {emoji} {row[0]}: {row[3]:+.1f}%"

    return {
        "answer": answer,
        "data": {
            "labels": labels,
            "values": values
        },
        "chartType": "bar"
    }


def _handle_gross_margin(question):
    """Handle: 'What is our gross margin?'"""
    conn = get_connection()
    cur = conn.cursor()

    # Get last 6 months of revenue and gross profit
    cur.execute("""
        SELECT
            month_year,
            SUM(CASE WHEN metric_type = 'CSM Revenue' THEN value ELSE 0 END) as revenue,
            SUM(CASE WHEN metric_type = 'CSM Gross Profit' THEN value ELSE 0 END) as gross_profit
        FROM finance."CSM_Rev_GP_Monthly"
        WHERE product_category = 'Total'
          AND month_year >= (SELECT MAX(month_year) - INTERVAL '5 months' FROM finance."CSM_Rev_GP_Monthly")
        GROUP BY month_year
        ORDER BY month_year
    """)

    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {
            "answer": "No margin data available.",
            "data": {},
            "chartType": "none"
        }

    labels = []
    margins = []
    revenues = []
    gross_profits = []

    for row in rows:
        rev = float(row[1])
        gp = float(row[2])
        margin = (gp / rev * 100) if rev else 0
        labels.append(row[0].strftime("%b %Y"))
        margins.append(round(margin, 1))
        revenues.append(round(rev, 2))
        gross_profits.append(round(gp, 2))

    latest_margin = margins[-1]
    avg_margin = sum(margins) / len(margins)

    answer = (
        f"Current gross margin is **{latest_margin:.1f}%** "
        f"(latest month: {labels[-1]}). "
        f"The 6-month average is **{avg_margin:.1f}%**."
    )
    answer += f"\n\nLatest month breakdown:"
    answer += f"\n• Revenue: {_fmt_money(revenues[-1])}"
    answer += f"\n• Gross Profit: {_fmt_money(gross_profits[-1])}"

    return {
        "answer": answer,
        "data": {
            "labels": labels,
            "values": margins
        },
        "chartType": "line"
    }


def _handle_quarter_comparison(question):
    """Handle: 'Compare Q1 2025 vs Q1 2026 revenue'"""
    # Find all QN YYYY patterns
    matches = re.findall(r'q(\d)\s*(\d{2,4})', question.lower())
    if len(matches) < 2:
        return None

    quarters = []
    for m in matches[:2]:
        q = int(m[0])
        yr = int(m[1])
        if yr < 100:
            yr += 2000
        quarters.append((q, yr))

    conn = get_connection()
    cur = conn.cursor()

    results = []
    for q, yr in quarters:
        start_month = (q - 1) * 3 + 1
        end_month = start_month + 2

        cur.execute("""
            SELECT SUM(value)
            FROM finance."CSM_Rev_GP_Monthly"
            WHERE metric_type = 'CSM Revenue'
              AND product_category = 'Total'
              AND EXTRACT(YEAR FROM month_year) = %s
              AND EXTRACT(MONTH FROM month_year) BETWEEN %s AND %s
        """, (yr, start_month, end_month))

        row = cur.fetchone()
        rev = float(row[0]) if row[0] else 0
        results.append({"quarter": f"Q{q} {yr}", "revenue": round(rev, 2)})

    conn.close()

    if not results[0]["revenue"] and not results[1]["revenue"]:
        return {
            "answer": f"No revenue data found for {results[0]['quarter']} or {results[1]['quarter']}.",
            "data": {},
            "chartType": "none"
        }

    diff = results[1]["revenue"] - results[0]["revenue"]
    pct = (diff / results[0]["revenue"] * 100) if results[0]["revenue"] else 0

    direction = "📈 increased" if diff > 0 else "📉 decreased"
    answer = (
        f"**{results[0]['quarter']}** revenue: {_fmt_money(results[0]['revenue'])}\n"
        f"**{results[1]['quarter']}** revenue: {_fmt_money(results[1]['revenue'])}\n\n"
        f"Revenue {direction} by **{_fmt_money(abs(diff))}** ({abs(pct):.1f}%)."
    )

    return {
        "answer": answer,
        "data": {
            "labels": [r["quarter"] for r in results],
            "values": [r["revenue"] for r in results]
        },
        "chartType": "bar"
    }


def _handle_revenue_by_product(question):
    """Handle: 'Revenue by product', 'product breakdown', 'revenue by category'"""
    yr = _parse_year(question)
    if yr is None:
        yr = date.today().year

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT product_category, SUM(value) as revenue
        FROM finance."CSM_Rev_GP_Monthly"
        WHERE metric_type = 'CSM Revenue'
          AND product_category NOT IN ('Total')
          AND EXTRACT(YEAR FROM month_year) = %s
        GROUP BY product_category
        ORDER BY revenue DESC
    """, (yr,))

    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {
            "answer": f"No product revenue data for {yr}.",
            "data": {},
            "chartType": "none"
        }

    labels = []
    values = []
    total = 0
    for row in rows:
        labels.append(row[0])
        rev = round(float(row[1]), 2)
        values.append(rev)
        total += rev

    answer = f"**Revenue by Product Category ({yr})**\nTotal: {_fmt_money(total)}\n"
    for i, label in enumerate(labels):
        pct = (values[i] / total * 100) if total else 0
        answer += f"\n• {label}: {_fmt_money(values[i])} ({pct:.1f}%)"

    return {
        "answer": answer,
        "data": {
            "labels": labels,
            "values": values
        },
        "chartType": "pie"
    }


def _handle_customer_count(question):
    """Handle: 'How many customers?' / 'customer count'"""
    m, y = _parse_month_year(question)

    conn = get_connection()
    cur = conn.cursor()

    if m and y:
        month_start = f"{y}-{m:02d}-01"
        cur.execute("""
            SELECT COUNT(DISTINCT user_id)
            FROM public.mission_control_monthly_revenue
            WHERE DATE_TRUNC('month', month) = %s::date
              AND month_revenue > 0
        """, (month_start,))
        row = cur.fetchone()
        count = int(row[0]) if row[0] else 0
        label = f"{month_name[m]} {y}"
        answer = f"There were **{count:,}** active customers in **{label}**."
    else:
        # Latest month
        cur.execute("""
            SELECT DATE_TRUNC('month', month) as mon, COUNT(DISTINCT user_id) as cnt
            FROM public.mission_control_monthly_revenue
            WHERE month_revenue > 0
              AND month >= NOW() - INTERVAL '12 months'
            GROUP BY 1
            ORDER BY 1
        """)
        rows = cur.fetchall()

        labels = [r[0].strftime("%b %Y") for r in rows]
        values = [int(r[1]) for r in rows]

        if rows:
            latest = values[-1]
            answer = f"Latest active customer count: **{latest:,}** ({labels[-1]})."
        else:
            answer = "No customer data available."
            labels, values = [], []

    conn.close()

    return {
        "answer": answer,
        "data": {
            "labels": labels if 'labels' in dir() else [],
            "values": values if 'values' in dir() else []
        },
        "chartType": "line" if not (m and y) else "none"
    }


def _handle_year_revenue(question):
    """Handle: 'Total revenue for 2025' / '2025 revenue'"""
    yr = _parse_year(question)
    if yr is None:
        return None

    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT SUM(value)
        FROM finance."CSM_Rev_GP_Monthly"
        WHERE metric_type = 'CSM Revenue'
          AND product_category = 'Total'
          AND EXTRACT(YEAR FROM month_year) = %s
    """, (yr,))

    row = cur.fetchone()
    rev = float(row[0]) if row[0] else 0

    # Get monthly breakdown for chart
    cur.execute("""
        SELECT month_year, SUM(value)
        FROM finance."CSM_Rev_GP_Monthly"
        WHERE metric_type = 'CSM Revenue'
          AND product_category = 'Total'
          AND EXTRACT(YEAR FROM month_year) = %s
        GROUP BY month_year
        ORDER BY month_year
    """, (yr,))

    rows = cur.fetchall()
    conn.close()

    labels = [r[0].strftime("%b") for r in rows]
    values = [round(float(r[1]), 2) for r in rows]

    answer = f"Total revenue for **{yr}** is **{_fmt_money(rev)}** ({len(rows)} months of data)."

    return {
        "answer": answer,
        "data": {
            "labels": labels,
            "values": values
        },
        "chartType": "bar"
    }


def _handle_top_customers(question):
    """Handle: 'Top customers' / 'biggest customers'"""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT user_id, SUM(month_revenue) as total_rev
        FROM public.mission_control_monthly_revenue
        WHERE month >= NOW() - INTERVAL '3 months'
          AND month_revenue > 0
        GROUP BY user_id
        ORDER BY total_rev DESC
        LIMIT 10
    """)

    rows = cur.fetchall()
    conn.close()

    if not rows:
        return {
            "answer": "No customer revenue data available.",
            "data": {},
            "chartType": "none"
        }

    labels = [f"Customer {r[0]}" for r in rows]
    values = [round(float(r[1]), 2) for r in rows]

    answer = "**Top 10 Customers by Revenue** (last 3 months):\n"
    for i, row in enumerate(rows):
        answer += f"\n{i+1}. Customer {row[0]}: {_fmt_money(row[1])}"

    return {
        "answer": answer,
        "data": {
            "labels": labels,
            "values": values
        },
        "chartType": "bar"
    }


def _handle_mom_growth(question):
    """Handle: 'month over month growth' / 'MoM growth'"""
    conn = get_connection()
    cur = conn.cursor()

    cur.execute("""
        SELECT DATE_TRUNC('month', month) as mon, SUM(month_revenue)
        FROM public.mission_control_monthly_revenue
        WHERE month >= NOW() - INTERVAL '13 months'
        GROUP BY 1 ORDER BY 1
    """)

    rows = cur.fetchall()
    conn.close()

    if len(rows) < 2:
        return {
            "answer": "Not enough data to calculate month-over-month growth.",
            "data": {},
            "chartType": "none"
        }

    labels = []
    values = []
    for i in range(1, len(rows)):
        prev_rev = float(rows[i-1][1])
        curr_rev = float(rows[i][1])
        growth = ((curr_rev - prev_rev) / prev_rev * 100) if prev_rev else 0
        labels.append(rows[i][0].strftime("%b %Y"))
        values.append(round(growth, 1))

    avg_growth = sum(values) / len(values) if values else 0
    latest_growth = values[-1] if values else 0

    answer = (
        f"**Month-over-Month Revenue Growth**\n"
        f"Latest: **{_fmt_pct(latest_growth)}**\n"
        f"12-month average: **{_fmt_pct(avg_growth)}**"
    )

    return {
        "answer": answer,
        "data": {
            "labels": labels,
            "values": values
        },
        "chartType": "bar"
    }


# ---------------------------------------------------------------------------
# Pattern Router
# ---------------------------------------------------------------------------

PATTERNS = [
    # Quarter comparison — must come before generic revenue
    (r'compare\s+q\d|q\d\s*\d{2,4}\s*vs\s*q\d|q\d.*vs.*q\d', _handle_quarter_comparison),

    # Revenue for a specific month
    (r'(?:total\s+)?revenue\s+(?:for\s+|in\s+)?(?:last\s+month|this\s+month|jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)', _handle_total_revenue_month),
    (r'(?:last|this)\s+month.*revenue', _handle_total_revenue_month),
    (r'revenue.*(?:last|this)\s+month', _handle_total_revenue_month),

    # Revenue trend
    (r'revenue\s+trend|trend.*revenue|show.*revenue.*(?:last|past)\s+\d+\s+months?', _handle_revenue_trend),
    (r'(?:last|past)\s+\d+\s+months?\s+revenue', _handle_revenue_trend),

    # Fastest growing
    (r'fastest\s+grow|growing\s+fastest|highest\s+growth|top\s+grow', _handle_fastest_growing_category),

    # Gross margin
    (r'gross\s+margin|margin|gross\s+profit\s+%|gp\s+margin|profitability', _handle_gross_margin),

    # Revenue by product / category
    (r'revenue\s+by\s+(?:product|category)|product\s+(?:breakdown|mix|split|revenue)|category\s+(?:breakdown|revenue)', _handle_revenue_by_product),

    # Customer count
    (r'(?:how\s+many|number\s+of|count)\s+customers?|customer\s+count|active\s+customers', _handle_customer_count),

    # Year revenue
    (r'(?:total\s+)?revenue\s+(?:for\s+)?20\d{2}|20\d{2}\s+revenue|annual\s+revenue', _handle_year_revenue),

    # Top customers
    (r'top\s+customers?|biggest\s+customers?|largest\s+customers?', _handle_top_customers),

    # MoM growth
    (r'mom\s+growth|month\s+over\s+month|monthly\s+growth|growth\s+rate', _handle_mom_growth),
]


def process_question(question: str) -> dict:
    """
    Process a natural language financial question and return a structured answer.

    Returns:
        {
            "answer": str,          # Human-readable answer (markdown)
            "data": dict,           # Structured data (labels, values, etc.)
            "chartType": str        # "bar" | "line" | "pie" | "none"
        }
    """
    if not question or not question.strip():
        return {
            "answer": "Please ask a question about our financial data.",
            "data": {},
            "chartType": "none"
        }

    q_lower = question.lower().strip()

    # Try each pattern
    for pattern, handler in PATTERNS:
        if re.search(pattern, q_lower):
            try:
                result = handler(question)
                if result is not None:
                    return result
            except Exception as e:
                return {
                    "answer": f"Sorry, I encountered an error processing that question: {str(e)}",
                    "data": {},
                    "chartType": "none"
                }

    # Fallback: help message
    return {
        "answer": (
            "I'm not sure how to answer that. Here are some things I can help with:\n\n"
            "📊 **Revenue Questions**\n"
            "• \"What was total revenue last month?\"\n"
            "• \"Show me revenue trend for the last 6 months\"\n"
            "• \"Total revenue for 2025\"\n"
            "• \"Compare Q1 2025 vs Q1 2026 revenue\"\n\n"
            "📈 **Growth & Analysis**\n"
            "• \"Which product category is growing fastest?\"\n"
            "• \"Revenue by product category\"\n"
            "• \"Month over month growth\"\n\n"
            "💰 **Margins & Profitability**\n"
            "• \"What's our gross margin?\"\n\n"
            "👥 **Customers**\n"
            "• \"How many active customers?\"\n"
            "• \"Top customers by revenue\""
        ),
        "data": {},
        "chartType": "none"
    }


# ---------------------------------------------------------------------------
# CLI test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1:
        q = " ".join(sys.argv[1:])
    else:
        q = "What was total revenue last month?"

    result = process_question(q)
    print(json.dumps(result, indent=2))
