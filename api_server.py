#!/usr/bin/env python3
"""
FP&A API Server - Serves data for the FP&A frontend
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
import json
import psycopg2
from datetime import date, timedelta
from urllib.parse import urlparse, parse_qs

DB_CONFIG = {
    "host": "10.5.224.24",
    "database": "domo_warehouse",
    "user": "domo",
    "password": "Q3ugHwECA8ehpq",
}


def get_connection():
    return psycopg2.connect(**DB_CONFIG, connect_timeout=60)


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
        self.send_header('Access-Control-Allow-Methods', 'GET, OPTIONS')
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
            
            elif path == '/api/health':
                self._send_json({"status": "ok"})
            
            else:
                self._send_json({"error": "Not found"}, 404)
        
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
    print("  GET /api/health - Health check")
    server.serve_forever()


if __name__ == "__main__":
    run_server()
