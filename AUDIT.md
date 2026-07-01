# Telnyx FP&A Platform — Full Audit
*Date: 2026-07-01 | Auditor: Vortex FinCore*

## Summary
- **28 HTML pages** total
- **Only 4 pages** have live API connections
- **24 pages** are hardcoded/static (UI shells with sample data)
- **7 API endpoints** exist, but only 4 are wired to pages
- **Inspiration:** Inflectiv Intelligence (financial modelling SaaS for CFOs)

## Page-by-Page Status

### ✅ FUNCTIONAL (API-connected) — 4 pages
| Page | API Endpoint | Status |
|------|-------------|--------|
| `executive_summary.html` | `/api/executive-summary` | ✅ Fetches live data |
| `pnl_drivers.html` | `/api/drivers` | ✅ Fetches live data with fallback |
| `saas_metrics.html` | `/api/saas/monthly-revenue`, `/api/saas/revenue-by-category` | ✅ Fetches live data |
| `pnl.html` | `/api/pnl` | ⚠️ Has fetch reference but TODO — endpoint doesn't exist |

### 🟡 HARDCODED (UI built, data static) — 15 pages
| Page | Lines | Charts | Notes |
|------|-------|--------|-------|
| `dashboard.html` | 203 | 2 | Revenue overview — hardcoded 2025/2026 data |
| `revenue_product.html` | 181 | 3 | Revenue by product — hardcoded |
| `revenue_region.html` | 177 | 2 | Revenue by region — hardcoded |
| `revenue_segment.html` | 174 | 2 | Revenue by segment — hardcoded |
| `headcount.html` | 187 | 2 | Headcount trends — hardcoded |
| `balance_sheet.html` | 351 | 2 | Balance sheet view — hardcoded |
| `bs_aged_debtors.html` | 202 | 2 | Aged debtors — hardcoded |
| `bs_aged_creditors.html` | 323 | 2 | Aged creditors — hardcoded |
| `bs_fixed_assets.html` | 349 | 2 | Fixed assets — hardcoded |
| `bs_loans.html` | 314 | 1 | Loans — hardcoded |
| `cash_flow.html` | 204 | 1 | Cash flow overview — hardcoded |
| `cf_13week.html` | 329 | 1 | 13-week cash forecast — hardcoded |
| `cf_direct.html` | 547 | 0 | Direct cash flow — hardcoded tables |
| `cf_indirect.html` | 505 | 2 | Indirect cash flow — hardcoded |
| `pnl_forecast.html` | 474 | 1 | P&L forecast — hardcoded |

### 🔴 SCAFFOLDING (UI layout only, no real data/logic) — 9 pages
| Page | Lines | Notes |
|------|-------|-------|
| `index.html` | 192 | Home/landing — navigation hub only |
| `pnl_import.html` | 284 | CSV/file import UI — no backend |
| `pnl_miniforecasts.html` | 410 | Mini forecast cards — no data engine |
| `bs_import.html` | 236 | BS import — no backend |
| `bs_accruals.html` | 260 | Accruals — no data |
| `bs_prepay.html` | 284 | Prepayments — no data |
| `data_connections.html` | 458 | Data source config — no backend |
| `reports.html` | 224 | Report builder — no backend |
| `settings.html` | 201 | Settings page — no backend |

## API Endpoints Status

### Existing (in api_server.py)
| Endpoint | Connected to Page? | Data Source |
|----------|-------------------|-------------|
| `/api/drivers` | ✅ pnl_drivers.html | `finance.CSM_Rev_GP_Monthly` |
| `/api/revenue/by-product` | ❌ Not connected | `finance.CSM_Rev_GP_Monthly` |
| `/api/revenue/monthly` | ❌ Not connected | `finance.CSM_Rev_GP_Monthly` |
| `/api/executive-summary` | ✅ executive_summary.html | `finance.CSM_Rev_GP_Monthly` |
| `/api/saas/monthly-revenue` | ✅ saas_metrics.html | `mission_control_monthly_revenue` |
| `/api/saas/revenue-by-category` | ✅ saas_metrics.html | `finance.CSM_Rev_GP_Monthly` |
| `/api/saas/nrr` | ❌ Not connected | `mission_control_monthly_revenue` |

### Missing (needed but don't exist)
- `/api/pnl` — P&L data by period
- `/api/balance-sheet` — BS data
- `/api/cash-flow` — Cash flow data
- `/api/headcount` — Headcount from Rippling
- `/api/revenue/by-region` — Revenue by geography
- `/api/revenue/by-segment` — Revenue by customer segment
- `/api/forecast` — Forecast engine
- `/api/variance` — Variance analysis
- `/api/kpi` — KPI dashboard metrics

## Security Issues 🚨
1. **`data/rippling_employees.json`** — Contains raw PII (names, DOBs, addresses, photos, compensation). Must be removed/gitignored.
2. **Hardcoded DB credentials** in `api_server.py` — Should use env vars.
3. **No authentication** — Anyone with URL can access.

## Architecture Gaps
1. No frontend framework — plain HTML limits interactivity
2. No state management — each page is isolated
3. No AI/chat capabilities
4. No scenario modelling
5. No export/board pack generation
6. No user roles/permissions
7. No data refresh mechanism (manual only)

## Feature Roadmap (Inflectiv-Inspired)

### Phase 1 — Fix Foundations
- [ ] Move credentials to env vars
- [ ] Remove PII data file
- [ ] Wire existing API endpoints to their pages
- [ ] Add missing API endpoints for existing pages

### Phase 2 — Core FP&A
- [ ] P&L by department with variance analysis
- [ ] Revenue & cost drivers engine
- [ ] Headcount planning with Rippling integration
- [ ] Cash flow forecasting (13-week + long-term)
- [ ] Balance sheet with full BS logic
- [ ] KPI dashboard with configurable metrics

### Phase 3 — AI & Intelligence
- [ ] AI chat assistant (ask questions about financials)
- [ ] Natural language querying ("What drove revenue growth in Q2?")
- [ ] Automated variance commentary
- [ ] Forecast generation from drivers
- [ ] Anomaly detection & alerts

### Phase 4 — Reporting & Polish
- [ ] Board pack generator
- [ ] Scenario modelling (best/base/worst)
- [ ] PDF/PowerPoint export
- [ ] User auth & role-based access
- [ ] Data source management UI
