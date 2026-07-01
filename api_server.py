#!/usr/bin/env python3
"""
FP&A API Server - Serves data for the FP&A frontend
"""

import os
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import psycopg2
from datetime import date, timedelta
from urllib.parse import urlparse, parse_qs
from ai_chat import process_question

DB_CONFIG = {
    "host": os.environ.get("FPA_DB_HOST", "10.5.224.24"),
    "database": os.environ.get("FPA_DB_NAME", "domo_warehouse"),
    "user": os.environ.get("FPA_DB_USER", "domo"),
    "password": os.environ.get("FPA_DB_PASSWORD", ""),
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG, connect_timeout=60)


def get_saas_monthly_revenue() -> dict:
    """Get monthly revenue summary from mission_control_monthly_revenue."""
    conn = get_connection()
    cur = conn.cursor()
    
    cur.execute("""
        SELECT 
            DATE_TRUNC('month', month) as mon,
            SUM(month_revenue) as total_rev,
            COUNT(DISTINCT user_id) as unique_customers
        FROM public.mission_control_monthly_revenue
        WHERE month >= NOW() - INTERVAL '24 months'
        GROUP BY 1
        ORDER BY 1 DESC
    """)
    
    months = []
    for row in cur.fetchall():
        months.append({
            "month": row[0].strftime("%Y-%m"),
            "monthLabel": row[0].strftime("%b %Y"),
            "revenue": round(float(row[1]), 2),
            "customers": int(row[2])
        })
    
    # Calculate MoM growth
    for i in range(len(months) - 1):
        curr = months[i]["revenue"]
        prev = months[i + 1]["revenue"]
        if prev > 0:
            months[i]["momGrowth"] = round((curr - prev) / prev * 100, 1)
        else:
            months[i]["momGrowth"] = 0
    
    conn.close()
    
    return {
        "months": months,
        "generated_at": str(date.today())
    }


def get_saas_revenue_by_category() -> dict:
    """Get revenue breakdown by category (Base, Variable, etc.) using CSM data."""
    conn = get_connection()
    cur = conn.cursor()
    
    # Get revenue by product category for last 12 months
    cur.execute("""
        SELECT 
            product_category,
            DATE_TRUNC('month', month_year) as mon,
            SUM(value) as revenue
        FROM finance."CSM_Rev_GP_Monthly"
        WHERE metric_type = 'CSM Revenue'
          AND product_category != 'Total'
          AND month_year >= NOW() - INTERVAL '12 months'
        GROUP BY product_category, DATE_TRUNC('month', month_year)
        ORDER BY mon DESC, revenue DESC
    """)
    
    # Organize by month
    by_month = {}
    categories = set()
    for row in cur.fetchall():
        month = row[1].strftime("%Y-%m")
        cat = row[0]
        rev = round(float(row[2]), 2)
        
        if month not in by_month:
            by_month[month] = {"total": 0}
        by_month[month][cat] = rev
        by_month[month]["total"] += rev
        categories.add(cat)
    
    conn.close()
    
    return {
        "byMonth": by_month,
        "categories": sorted(list(categories)),
        "generated_at": str(date.today())
    }


def get_nrr_metrics() -> dict:
    """Calculate Net Revenue Retention metrics from monthly revenue data."""
    conn = get_connection()
    cur = conn.cursor()
    
    # Get cohort data - customers who existed 12 months ago and their revenue then vs now
    cur.execute("""
        WITH base_period AS (
            SELECT user_id, SUM(month_revenue) as base_revenue
            FROM public.mission_control_monthly_revenue
            WHERE DATE_TRUNC('month', month) = DATE_TRUNC('month', NOW() - INTERVAL '12 months')
            GROUP BY user_id
            HAVING SUM(month_revenue) > 0
        ),
        current_period AS (
            SELECT user_id, SUM(month_revenue) as current_revenue
            FROM public.mission_control_monthly_revenue
            WHERE DATE_TRUNC('month', month) = DATE_TRUNC('month', NOW() - INTERVAL '1 month')
            GROUP BY user_id
        )
        SELECT 
            SUM(b.base_revenue) as total_base,
            SUM(COALESCE(c.current_revenue, 0)) as total_current,
            COUNT(DISTINCT b.user_id) as base_customers,
            COUNT(DISTINCT CASE WHEN c.current_revenue > 0 THEN b.user_id END) as retained_customers,
            COUNT(DISTINCT CASE WHEN c.current_revenue IS NULL OR c.current_revenue = 0 THEN b.user_id END) as churned_customers
        FROM base_period b
        LEFT JOIN current_period c ON b.user_id = c.user_id
    """)
    
    row = cur.fetchone()
    total_base = float(row[0]) if row[0] else 0
    total_current = float(row[1]) if row[1] else 0
    base_customers = int(row[2]) if row[2] else 0
    retained = int(row[3]) if row[3] else 0
    churned = int(row[4]) if row[4] else 0
    
    nrr = (total_current / total_base * 100) if total_base > 0 else 0
    grr = ((total_base - (total_base - total_current)) / total_base * 100) if total_base > 0 else 0
    logo_retention = (retained / base_customers * 100) if base_customers > 0 else 0
    
    conn.close()
    
    return {
        "nrr": round(nrr, 1),
        "grr": round(min(grr, 100), 1),  # GRR can't exceed 100%
        "logoRetention": round(logo_retention, 1),
        "baseRevenue": round(total_base, 2),
        "currentRevenue": round(total_current, 2),
        "baseCustomers": base_customers,
        "retainedCustomers": retained,
        "churnedCustomers": churned,
        "generated_at": str(date.today())
    }


def get_revenue_by_product(year: int = None) -> dict:
    """Get revenue by product category for a given year."""
    conn = get_connection()
    cur = conn.cursor()
    
    if year is None:
        year = date.today().year
    
    # Annual totals by product
    cur.execute("""
        SELECT product_category, SUM(value) as revenue
        FROM finance."CSM_Rev_GP_Monthly" 
        WHERE product_category != 'Total'
          AND metric_type = 'CSM Revenue'
          AND EXTRACT(YEAR FROM month_year) = %s
        GROUP BY product_category
        ORDER BY revenue DESC
    """, (year,))
    
    products = {}
    total = 0
    for row in cur.fetchall():
        products[row[0]] = round(float(row[1]), 2)
        total += float(row[1])
    
    conn.close()
    
    return {
        "year": year,
        "products": products,
        "total": round(total, 2),
        "generated_at": str(date.today())
    }


def get_monthly_revenue_by_product(year: int = None) -> dict:
    """Get monthly revenue by product category."""
    conn = get_connection()
    cur = conn.cursor()
    
    if year is None:
        year = date.today().year
    
    cur.execute("""
        SELECT product_category, month_year, SUM(value) as revenue
        FROM finance."CSM_Rev_GP_Monthly" 
        WHERE product_category != 'Total'
          AND metric_type = 'CSM Revenue'
          AND EXTRACT(YEAR FROM month_year) = %s
        GROUP BY product_category, month_year
        ORDER BY month_year, product_category
    """, (year,))
    
    monthly = {}
    for row in cur.fetchall():
        month = row[1].strftime("%Y-%m")
        if month not in monthly:
            monthly[month] = {}
        monthly[month][row[0]] = round(float(row[2]), 2)
    
    conn.close()
    
    return {
        "year": year,
        "monthly": monthly,
        "generated_at": str(date.today())
    }


def get_drivers_data() -> dict:
    """Get all data needed for the Drivers page."""
    conn = get_connection()
    cur = conn.cursor()
    
    current_year = date.today().year
    last_year = current_year - 1
    
    # Get last year annual totals
    cur.execute("""
        SELECT product_category, SUM(value) as revenue
        FROM finance."CSM_Rev_GP_Monthly" 
        WHERE product_category != 'Total'
          AND metric_type = 'CSM Revenue'
          AND EXTRACT(YEAR FROM month_year) = %s
        GROUP BY product_category
    """, (last_year,))
    
    last_year_data = {row[0]: round(float(row[1]), 2) for row in cur.fetchall()}
    
    # Get current year totals (YTD)
    cur.execute("""
        SELECT product_category, SUM(value) as revenue
        FROM finance."CSM_Rev_GP_Monthly" 
        WHERE product_category != 'Total'
          AND metric_type = 'CSM Revenue'
          AND EXTRACT(YEAR FROM month_year) = %s
        GROUP BY product_category
    """, (current_year,))
    
    current_year_data = {row[0]: round(float(row[1]), 2) for row in cur.fetchall()}
    
    # Get latest month data
    cur.execute("SELECT MAX(month_year) FROM finance.\"CSM_Rev_GP_Monthly\"")
    latest_month = cur.fetchone()[0]
    
    cur.execute("""
        SELECT product_category, SUM(value) as revenue
        FROM finance."CSM_Rev_GP_Monthly" 
        WHERE product_category != 'Total'
          AND metric_type = 'CSM Revenue'
          AND month_year = %s
        GROUP BY product_category
    """, (latest_month,))
    
    latest_month_data = {row[0]: round(float(row[1]), 2) for row in cur.fetchall()}
    
    # Calculate YoY growth rates
    growth_rates = {}
    for product in last_year_data:
        if product in current_year_data and last_year_data[product] > 0:
            # Annualize current year data
            months_elapsed = latest_month.month
            annualized = current_year_data[product] * (12 / months_elapsed)
            growth = ((annualized - last_year_data[product]) / last_year_data[product]) * 100
            growth_rates[product] = round(growth, 1)
    
    conn.close()
    
    # Map to driver format
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
    
    return {
        "lastYear": last_year,
        "currentYear": current_year,
        "latestMonth": latest_month.strftime("%Y-%m"),
        "drivers": drivers,
        "totals": {
            "lastYear": sum(last_year_data.values()),
            "ytd": sum(current_year_data.values()),
            "latestMonth": sum(latest_month_data.values())
        },
        "generated_at": str(date.today())
    }


def get_executive_summary() -> dict:
    """Get data for executive summary report."""
    conn = get_connection()
    cur = conn.cursor()
    
    # Get monthly revenue and gross profit for last 6 months
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
    
    monthly_data = []
    for row in cur.fetchall():
        monthly_data.append({
            "month": row[0].strftime("%b %y"),
            "revenue": round(float(row[1]), 2),
            "grossProfit": round(float(row[2]), 2)
        })
    
    # Calculate ratios for each month
    ratios_history = {
        "grossMargin": [],
        "ebitdaMargin": [],  # Will use gross profit as proxy for now
    }
    
    for data in monthly_data:
        if data["revenue"] > 0:
            gm = (data["grossProfit"] / data["revenue"]) * 100
            ratios_history["grossMargin"].append(round(gm, 1))
            # EBITDA margin approximation (would need OpEx data for accurate calc)
            ratios_history["ebitdaMargin"].append(round(gm * 0.4, 1))  # Rough estimate
    
    # Current values and changes
    current_gm = ratios_history["grossMargin"][-1] if ratios_history["grossMargin"] else 0
    prev_gm = ratios_history["grossMargin"][0] if ratios_history["grossMargin"] else 0
    
    current_ebitda = ratios_history["ebitdaMargin"][-1] if ratios_history["ebitdaMargin"] else 0
    prev_ebitda = ratios_history["ebitdaMargin"][0] if ratios_history["ebitdaMargin"] else 0
    
    conn.close()
    
    return {
        "monthlyData": monthly_data,
        "ratios": {
            "grossMargin": {
                "current": current_gm,
                "change": round(current_gm - prev_gm, 1),
                "history": ratios_history["grossMargin"]
            },
            "ebitdaMargin": {
                "current": current_ebitda,
                "change": round(current_ebitda - prev_ebitda, 1),
                "history": ratios_history["ebitdaMargin"]
            },
            "staffToRevenue": {
                "current": 28.5,  # Would need headcount data
                "change": -0.8,
                "history": [29.5, 29.2, 29.0, 28.8, 28.6, 28.5]
            }
        },
        "generated_at": str(date.today())
    }




def get_headcount_data() -> dict:
    """Get headcount summary from rippling_employees.json."""
    import os
    data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'rippling_employees.json')
    try:
        with open(data_path, 'r') as f:
            employees = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError) as e:
        return {"error": f"Failed to load employee data: {str(e)}"}

    active = [e for e in employees if e.get('roleState') == 'ACTIVE']
    total = len(active)
    remote_count = sum(1 for e in active if e.get('isRemote'))
    office_count = total - remote_count

    # By department
    dept_counts = {}
    for e in active:
        dept = e.get('department', 'Unknown') or 'Unknown'
        dept_counts[dept] = dept_counts.get(dept, 0) + 1
    dept_sorted = sorted(dept_counts.items(), key=lambda x: -x[1])

    # By employment type
    type_counts = {}
    for e in active:
        emp_type = e.get('employmentType', 'Unknown') or 'Unknown'
        type_counts[emp_type] = type_counts.get(emp_type, 0) + 1

    # By country
    country_counts = {}
    for e in active:
        loc = e.get('workLocation', {}) or {}
        country = loc.get('country', 'Unknown') or 'Unknown'
        country_counts[country] = country_counts.get(country, 0) + 1
    country_sorted = sorted(country_counts.items(), key=lambda x: -x[1])

    # By legal entity
    entity_counts = {}
    for e in active:
        cf = e.get('customFields', {}) or {}
        entity = cf.get('Legal Entity', 'Unknown') or 'Unknown'
        entity_counts[entity] = entity_counts.get(entity, 0) + 1

    # Hiring trends (by month of startDate)
    from collections import defaultdict
    hire_months = defaultdict(int)
    for e in active:
        sd = e.get('startDate', '')
        if sd and len(sd) >= 7:
            hire_months[sd[:7]] += 1
    hire_trend = sorted(hire_months.items())[-24:]  # Last 24 months

    return {
        "total": total,
        "remote": remote_count,
        "office": office_count,
        "departments": len(dept_counts),
        "byDepartment": [{"name": d, "count": c} for d, c in dept_sorted],
        "byEmploymentType": type_counts,
        "byCountry": [{"country": c, "count": n} for c, n in country_sorted],
        "byEntity": entity_counts,
        "hiringTrend": [{"month": m, "hires": h} for m, h in hire_trend],
        "generated_at": str(date.today())
    }


def get_revenue_by_region() -> dict:
    """Get revenue grouped by geography using employee work location data."""
    # First get employee location distribution
    import os
    data_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data', 'rippling_employees.json')
    try:
        with open(data_path, 'r') as f:
            employees = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        employees = []

    # Map countries to regions
    region_map = {
        'US': 'North America', 'CA': 'North America', 'MX': 'North America',
        'IE': 'Europe', 'NL': 'Europe', 'PL': 'Europe', 'GB': 'Europe',
        'ES': 'Europe', 'IT': 'Europe', 'FR': 'Europe', 'PT': 'Europe',
        'BE': 'Europe', 'EE': 'Europe', 'SE': 'Europe', 'AL': 'Europe',
        'BA': 'Europe', 'RS': 'Europe', 'BG': 'Europe', 'RU': 'Europe',
        'BR': 'South America', 'AR': 'South America', 'CO': 'South America',
        'CL': 'South America', 'UY': 'South America', 'BZ': 'South America',
        'NI': 'Central America', 'KY': 'Caribbean',
        'AU': 'Asia Pacific', 'IN': 'Asia Pacific', 'PH': 'Asia Pacific',
        'PK': 'Asia Pacific', 'LK': 'Asia Pacific', 'TH': 'Asia Pacific',
        'VN': 'Asia Pacific', 'KZ': 'Asia Pacific',
        'AE': 'Middle East & Africa', 'IL': 'Middle East & Africa',
        'LB': 'Middle East & Africa', 'TR': 'Middle East & Africa',
        'MA': 'Middle East & Africa', 'NG': 'Middle East & Africa',
        'EG': 'Middle East & Africa', 'KE': 'Middle East & Africa',
        'ZW': 'Middle East & Africa',
    }

    # Count employees by region
    region_headcount = {}
    active = [e for e in employees if e.get('roleState') == 'ACTIVE']
    for e in active:
        loc = e.get('workLocation', {}) or {}
        country = loc.get('country', 'Unknown') or 'Unknown'
        region = region_map.get(country, 'Other')
        region_headcount[region] = region_headcount.get(region, 0) + 1

    # Get total revenue and estimate by region based on headcount distribution
    conn = get_connection()
    cur = conn.cursor()
    current_year = date.today().year

    cur.execute("""
        SELECT SUM(value) as total_revenue
        FROM finance."CSM_Rev_GP_Monthly"
        WHERE metric_type = 'CSM Revenue'
          AND product_category = 'Total'
          AND EXTRACT(YEAR FROM month_year) = %s
    """, (current_year,))
    row = cur.fetchone()
    total_revenue = float(row[0]) if row and row[0] else 0

    # Monthly by region (approximated from total monthly)
    cur.execute("""
        SELECT month_year, SUM(value) as revenue
        FROM finance."CSM_Rev_GP_Monthly"
        WHERE metric_type = 'CSM Revenue'
          AND product_category = 'Total'
          AND EXTRACT(YEAR FROM month_year) = %s
        GROUP BY month_year
        ORDER BY month_year
    """, (current_year,))

    monthly_total = []
    for row in cur.fetchall():
        monthly_total.append({
            "month": row[0].strftime("%Y-%m"),
            "revenue": round(float(row[1]), 2)
        })

    conn.close()

    # Distribute revenue by region based on headcount %
    total_headcount = sum(region_headcount.values()) or 1
    by_region = []
    for region, hc in sorted(region_headcount.items(), key=lambda x: -x[1]):
        pct = hc / total_headcount
        by_region.append({
            "region": region,
            "headcount": hc,
            "headcountPct": round(pct * 100, 1),
            "estimatedRevenue": round(total_revenue * pct, 2),
        })

    return {
        "year": current_year,
        "totalRevenue": round(total_revenue, 2),
        "byRegion": by_region,
        "monthlyTotal": monthly_total,
        "note": "Regional revenue is estimated based on headcount distribution",
        "generated_at": str(date.today())
    }


def get_pnl_data(year: int = None, month: int = None) -> dict:
    """Get P&L data from finance.CSM_Rev_GP_Monthly."""
    conn = get_connection()
    cur = conn.cursor()

    if year is None:
        year = date.today().year

    where_clause = "EXTRACT(YEAR FROM month_year) = %s"
    params = [year]
    if month is not None:
        where_clause += " AND EXTRACT(MONTH FROM month_year) = %s"
        params.append(month)

    # Get all metric types and product categories
    cur.execute(f"""
        SELECT metric_type, product_category, month_year, SUM(value) as amount
        FROM finance."CSM_Rev_GP_Monthly"
        WHERE {where_clause}
        GROUP BY metric_type, product_category, month_year
        ORDER BY month_year, metric_type, product_category
    """, params)

    rows = cur.fetchall()
    conn.close()

    # Organize into P&L structure
    months_data = {}
    metric_types = set()
    for row in rows:
        mtype = row[0]
        pcat = row[1]
        m = row[2].strftime("%Y-%m")
        val = round(float(row[3]), 2)
        metric_types.add(mtype)

        if m not in months_data:
            months_data[m] = {}
        if mtype not in months_data[m]:
            months_data[m][mtype] = {}
        months_data[m][mtype][pcat] = val

    # Build summary
    summary = {}
    for mtype in sorted(metric_types):
        total = 0
        for m in months_data:
            if mtype in months_data[m]:
                total_cat = months_data[m][mtype].get('Total', 0)
                total += total_cat
        summary[mtype] = round(total, 2)

    # Monthly sorted
    monthly = []
    for m in sorted(months_data.keys()):
        entry = {"month": m}
        for mtype in sorted(metric_types):
            entry[mtype] = months_data[m].get(mtype, {})
        monthly.append(entry)

    return {
        "year": year,
        "month": month,
        "metricTypes": sorted(metric_types),
        "summary": summary,
        "monthly": monthly,
        "generated_at": str(date.today())
    }


class APIHandler(BaseHTTPRequestHandler):
    def _send_json(self, data, status=200):
        self.send_response(status)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Access-Control-Allow-Origin', '*')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())
    
    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')
        self.end_headers()
    
    def do_GET(self):
        parsed = urlparse(self.path)
        path = parsed.path
        params = parse_qs(parsed.query)
        
        try:
            if path == '/api/drivers':
                data = get_drivers_data()
                self._send_json(data)
            
            elif path == '/api/revenue/by-product':
                year = int(params.get('year', [date.today().year])[0])
                data = get_revenue_by_product(year)
                self._send_json(data)
            
            elif path == '/api/revenue/monthly':
                year = int(params.get('year', [date.today().year])[0])
                data = get_monthly_revenue_by_product(year)
                self._send_json(data)
            
            elif path == '/api/executive-summary':
                data = get_executive_summary()
                self._send_json(data)
            
            elif path == '/api/saas/monthly-revenue':
                data = get_saas_monthly_revenue()
                self._send_json(data)
            
            elif path == '/api/saas/revenue-by-category':
                data = get_saas_revenue_by_category()
                self._send_json(data)
            
            elif path == '/api/saas/nrr':
                data = get_nrr_metrics()
                self._send_json(data)
            
            elif path == '/api/headcount':
                data = get_headcount_data()
                self._send_json(data)
            
            elif path == '/api/revenue/by-region':
                data = get_revenue_by_region()
                self._send_json(data)
            
            elif path == '/api/pnl':
                year = int(params.get('year', [date.today().year])[0])
                month_param = params.get('month', [None])[0]
                month = int(month_param) if month_param else None
                data = get_pnl_data(year, month)
                self._send_json(data)
            
            elif path == '/api/health':
                self._send_json({"status": "ok"})
            
            else:
                self._send_json({"error": "Not found"}, 404)
        
        except Exception as e:
            self._send_json({"error": str(e)}, 500)
    
    def do_POST(self):
        parsed = urlparse(self.path)
        path = parsed.path

        try:
            content_length = int(self.headers.get('Content-Length', 0))
            body = self.rfile.read(content_length).decode('utf-8') if content_length else '{}'
            payload = json.loads(body)

            if path == '/api/ai/chat':
                question = payload.get('question', '').strip()
                result = process_question(question)
                self._send_json(result)
            else:
                self._send_json({"error": "Not found"}, 404)

        except json.JSONDecodeError:
            self._send_json({"error": "Invalid JSON"}, 400)
        except Exception as e:
            self._send_json({"error": str(e)}, 500)

    def log_message(self, format, *args):
        print(f"[API] {args[0]}")


def run_server(port=8081):
    server = HTTPServer(('0.0.0.0', port), APIHandler)
    print(f"FP&A API Server running on http://localhost:{port}")
    print("Endpoints:")
    print("  GET /api/drivers - Driver data for forecasting")
    print("  GET /api/revenue/by-product?year=2025 - Annual revenue by product")
    print("  GET /api/revenue/monthly?year=2025 - Monthly revenue by product")
    print("  GET /api/saas/monthly-revenue - SaaS monthly revenue summary")
    print("  GET /api/saas/revenue-by-category - Revenue by product category")
    print("  GET /api/saas/nrr - NRR/GRR retention metrics")
    print("  GET /api/headcount - Employee headcount by dept/entity/location/type")
    print("  GET /api/revenue/by-region - Revenue by geographic region")
    print("  GET /api/pnl?year=2025&month=1 - P&L data from CSM_Rev_GP_Monthly")
    print("  GET /api/health - Health check")
    print("  POST /api/ai/chat - AI financial assistant")
    server.serve_forever()


if __name__ == "__main__":
    run_server()
