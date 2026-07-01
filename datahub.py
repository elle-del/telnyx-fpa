#!/usr/bin/env python3
"""
Data Hub — Universal data platform layer for the FP&A system.

Uses SQLite for storage (separate from the read-only PostgreSQL domo_warehouse).
Supports: data source configs, datasets, field mappings, imported data,
calculated fields, and department hierarchy with rollups.
"""

import ast
import csv
import io
import json
import math
import operator
import os
import re
import sqlite3
import uuid
from datetime import datetime, date
from typing import Any, Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# Safe formula evaluator
# ---------------------------------------------------------------------------

# Allowed binary operators
_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.FloorDiv: operator.floordiv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
    ast.USub: operator.neg,
    ast.UAdd: operator.pos,
}

# Allowed aggregate / helper functions
_SAFE_FUNCS = {
    "SUM": sum,
    "AVG": lambda xs: sum(xs) / len(xs) if xs else 0,
    "COUNT": len,
    "MIN": min,
    "MAX": max,
    "ABS": abs,
    "ROUND": round,
    "IF": lambda cond, t, f: t if cond else f,
}


class FormulaError(Exception):
    """Raised when a formula cannot be parsed or evaluated."""


def _safe_eval_node(node, row: dict, all_rows: Optional[List[dict]] = None):
    """Recursively evaluate an AST node against *row* (single record)
    and optionally *all_rows* (for aggregates)."""

    if isinstance(node, ast.Expression):
        return _safe_eval_node(node.body, row, all_rows)

    # Numeric / string literals
    if isinstance(node, ast.Constant):
        if isinstance(node.value, (int, float, str, bool)):
            return node.value
        raise FormulaError(f"Unsupported constant type: {type(node.value)}")

    # Unary ops (e.g. -amount)
    if isinstance(node, ast.UnaryOp):
        op_fn = _OPS.get(type(node.op))
        if op_fn is None:
            raise FormulaError(f"Unsupported unary op: {type(node.op).__name__}")
        return op_fn(_safe_eval_node(node.operand, row, all_rows))

    # Binary ops (e.g. revenue - cogs)
    if isinstance(node, ast.BinOp):
        op_fn = _OPS.get(type(node.op))
        if op_fn is None:
            raise FormulaError(f"Unsupported op: {type(node.op).__name__}")
        left = _safe_eval_node(node.left, row, all_rows)
        right = _safe_eval_node(node.right, row, all_rows)
        if op_fn is operator.truediv and right == 0:
            return 0  # safe div-by-zero
        return op_fn(left, right)

    # Comparison ops (for IF conditions)
    if isinstance(node, ast.Compare):
        left = _safe_eval_node(node.left, row, all_rows)
        for op_node, comparator in zip(node.ops, node.comparators):
            right = _safe_eval_node(comparator, row, all_rows)
            if isinstance(op_node, ast.Gt):
                if not (left > right):
                    return False
            elif isinstance(op_node, ast.GtE):
                if not (left >= right):
                    return False
            elif isinstance(op_node, ast.Lt):
                if not (left < right):
                    return False
            elif isinstance(op_node, ast.LtE):
                if not (left <= right):
                    return False
            elif isinstance(op_node, ast.Eq):
                if not (left == right):
                    return False
            elif isinstance(op_node, ast.NotEq):
                if not (left != right):
                    return False
            else:
                raise FormulaError(f"Unsupported comparison: {type(op_node).__name__}")
            left = right
        return True

    # Boolean ops (and / or)
    if isinstance(node, ast.BoolOp):
        values = [_safe_eval_node(v, row, all_rows) for v in node.values]
        if isinstance(node.op, ast.And):
            return all(values)
        if isinstance(node.op, ast.Or):
            return any(values)

    # Name -> field reference
    if isinstance(node, ast.Name):
        name = node.id
        if name in row:
            val = row[name]
            if isinstance(val, (int, float)):
                return val
            if isinstance(val, str):
                try:
                    return float(val)
                except (ValueError, TypeError):
                    return val
            return val
        raise FormulaError(f"Unknown field: {name}")

    # Attribute access for dotted field names (e.g. dept.total)
    if isinstance(node, ast.Attribute):
        parts = []
        n = node
        while isinstance(n, ast.Attribute):
            parts.append(n.attr)
            n = n.value
        if isinstance(n, ast.Name):
            parts.append(n.id)
        parts.reverse()
        field_name = ".".join(parts)
        if field_name in row:
            val = row[field_name]
            try:
                return float(val)
            except (ValueError, TypeError):
                return val
        raise FormulaError(f"Unknown field: {field_name}")

    # Function calls: SUM(field), AVG(field), IF(cond, t, f), etc.
    if isinstance(node, ast.Call):
        if not isinstance(node.func, ast.Name):
            raise FormulaError("Only simple function calls allowed")
        fname = node.func.id.upper()
        if fname not in _SAFE_FUNCS:
            raise FormulaError(f"Unknown function: {fname}")

        fn = _SAFE_FUNCS[fname]

        # Aggregate functions: SUM(field_name), AVG(field_name)
        if fname in ("SUM", "AVG", "COUNT", "MIN", "MAX") and all_rows is not None:
            if fname == "COUNT" and len(node.args) == 0:
                return fn(all_rows)
            if len(node.args) != 1:
                raise FormulaError(f"{fname} expects 1 argument")
            arg = node.args[0]
            if isinstance(arg, ast.Name):
                field = arg.id
                values = []
                for r in all_rows:
                    v = r.get(field, 0)
                    try:
                        values.append(float(v))
                    except (ValueError, TypeError):
                        pass
                return fn(values) if values else 0
            else:
                values = []
                for r in all_rows:
                    try:
                        values.append(float(_safe_eval_node(arg, r, None)))
                    except (FormulaError, ValueError, TypeError):
                        pass
                return fn(values) if values else 0

        # Non-aggregate functions: evaluate args against current row
        args = [_safe_eval_node(a, row, all_rows) for a in node.args]
        return fn(*args)

    raise FormulaError(f"Unsupported expression: {ast.dump(node)}")


def evaluate_formula(formula: str, row: dict, all_rows: Optional[List[dict]] = None) -> Any:
    """Safely evaluate a formula string against a data row."""
    try:
        tree = ast.parse(formula.strip(), mode="eval")
    except SyntaxError as exc:
        raise FormulaError(f"Invalid formula syntax: {exc}")
    return _safe_eval_node(tree, row, all_rows)


# ---------------------------------------------------------------------------
# DataHub class
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.utcnow().isoformat() + "Z"


def _new_id() -> str:
    return uuid.uuid4().hex[:12]


class DataHub:
    """Manages the Data Hub SQLite database."""

    def __init__(self, db_path: Optional[str] = None):
        if db_path is None:
            if os.path.isdir("/app"):
                db_path = "/app/datahub.db"
            else:
                db_path = os.path.join(
                    os.path.dirname(os.path.abspath(__file__)), "datahub.db"
                )
        self.db_path = db_path
        self._conn: Optional[sqlite3.Connection] = None

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
            self._conn.row_factory = sqlite3.Row
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
        return self._conn

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def init_db(self):
        """Create all Data Hub tables if they don't exist."""
        conn = self._get_conn()
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS data_sources (
                id          TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                type        TEXT NOT NULL CHECK(type IN (
                                'csv','excel','anaplan','rippling',
                                'campfire','api','postgres')),
                connection_config TEXT DEFAULT '{}',
                created_at  TEXT NOT NULL,
                updated_at  TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS datasets (
                id                TEXT PRIMARY KEY,
                name              TEXT NOT NULL,
                description       TEXT DEFAULT '',
                source_id         TEXT REFERENCES data_sources(id) ON DELETE SET NULL,
                import_mode       TEXT DEFAULT 'replace_all'
                                      CHECK(import_mode IN (
                                          'replace_all','append','update_current')),
                date_field        TEXT DEFAULT '',
                archive_monthly   INTEGER DEFAULT 0,
                schema_definition TEXT DEFAULT '[]',
                created_at        TEXT NOT NULL,
                updated_at        TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS field_mappings (
                id           TEXT PRIMARY KEY,
                dataset_id   TEXT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
                source_field TEXT NOT NULL,
                target_field TEXT NOT NULL,
                data_type    TEXT DEFAULT 'text'
                                 CHECK(data_type IN ('text','number','date','currency')),
                transform    TEXT DEFAULT '{}',
                is_active    INTEGER DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS dataset_rows (
                id          TEXT PRIMARY KEY,
                dataset_id  TEXT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
                period      TEXT DEFAULT '',
                data        TEXT NOT NULL DEFAULT '{}',
                imported_at TEXT NOT NULL,
                source_file TEXT DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_rows_dataset
                ON dataset_rows(dataset_id);
            CREATE INDEX IF NOT EXISTS idx_rows_period
                ON dataset_rows(dataset_id, period);

            CREATE TABLE IF NOT EXISTS calculated_fields (
                id           TEXT PRIMARY KEY,
                dataset_id   TEXT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
                name         TEXT NOT NULL,
                formula      TEXT NOT NULL,
                formula_type TEXT DEFAULT 'simple_math'
                                 CHECK(formula_type IN (
                                     'simple_math','aggregate','lookup','conditional')),
                depends_on   TEXT DEFAULT '[]',
                description  TEXT DEFAULT '',
                created_at   TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS department_hierarchy (
                id            TEXT PRIMARY KEY,
                name          TEXT NOT NULL,
                parent_id     TEXT REFERENCES department_hierarchy(id) ON DELETE SET NULL,
                level         INTEGER DEFAULT 0,
                mapping_rules TEXT DEFAULT '{}',
                created_at    TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS department_mappings (
                id            TEXT PRIMARY KEY,
                department_id TEXT NOT NULL REFERENCES department_hierarchy(id) ON DELETE CASCADE,
                dataset_id    TEXT NOT NULL REFERENCES datasets(id) ON DELETE CASCADE,
                field_name    TEXT NOT NULL,
                field_value   TEXT NOT NULL
            );
        """)
        conn.commit()

    # -------------------------------------------------------------------
    # Data Sources CRUD
    # -------------------------------------------------------------------

    def create_source(self, name: str, type_: str,
                      connection_config: Optional[dict] = None) -> dict:
        conn = self._get_conn()
        now = _now_iso()
        sid = _new_id()
        conn.execute(
            "INSERT INTO data_sources (id, name, type, connection_config, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (sid, name, type_, json.dumps(connection_config or {}), now, now),
        )
        conn.commit()
        return self.get_source(sid)

    def list_sources(self) -> List[dict]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM data_sources ORDER BY created_at DESC"
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_source(self, source_id: str) -> Optional[dict]:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM data_sources WHERE id = ?", (source_id,)
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def delete_source(self, source_id: str) -> bool:
        conn = self._get_conn()
        cur = conn.execute(
            "DELETE FROM data_sources WHERE id = ?", (source_id,)
        )
        conn.commit()
        return cur.rowcount > 0

    # -------------------------------------------------------------------
    # Datasets CRUD
    # -------------------------------------------------------------------

    def create_dataset(self, name: str, description: str = "",
                       source_id: Optional[str] = None,
                       import_mode: str = "replace_all",
                       date_field: str = "",
                       archive_monthly: bool = False,
                       schema_definition: Optional[list] = None) -> dict:
        conn = self._get_conn()
        now = _now_iso()
        did = _new_id()
        conn.execute(
            "INSERT INTO datasets "
            "(id, name, description, source_id, import_mode, date_field, "
            " archive_monthly, schema_definition, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (did, name, description, source_id, import_mode, date_field,
             1 if archive_monthly else 0,
             json.dumps(schema_definition or []), now, now),
        )
        conn.commit()
        return self.get_dataset(did)

    def list_datasets(self) -> List[dict]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM datasets ORDER BY created_at DESC"
        ).fetchall()
        result = []
        for r in rows:
            d = self._row_to_dict(r)
            cnt = conn.execute(
                "SELECT COUNT(*) FROM dataset_rows WHERE dataset_id = ?",
                (d["id"],)
            ).fetchone()[0]
            d["row_count"] = cnt
            result.append(d)
        return result

    def get_dataset(self, dataset_id: str, preview_limit: int = 50) -> Optional[dict]:
        conn = self._get_conn()
        row = conn.execute(
            "SELECT * FROM datasets WHERE id = ?", (dataset_id,)
        ).fetchone()
        if not row:
            return None
        d = self._row_to_dict(row)
        d["row_count"] = conn.execute(
            "SELECT COUNT(*) FROM dataset_rows WHERE dataset_id = ?",
            (dataset_id,)
        ).fetchone()[0]
        preview_rows = conn.execute(
            "SELECT data FROM dataset_rows WHERE dataset_id = ? "
            "ORDER BY imported_at DESC LIMIT ?",
            (dataset_id, preview_limit)
        ).fetchall()
        d["preview"] = [json.loads(r[0]) for r in preview_rows]
        return d

    def delete_dataset(self, dataset_id: str) -> bool:
        conn = self._get_conn()
        cur = conn.execute(
            "DELETE FROM datasets WHERE id = ?", (dataset_id,)
        )
        conn.commit()
        return cur.rowcount > 0

    # -------------------------------------------------------------------
    # Field Mappings
    # -------------------------------------------------------------------

    def get_mappings(self, dataset_id: str) -> List[dict]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM field_mappings WHERE dataset_id = ? ORDER BY source_field",
            (dataset_id,)
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def save_mappings(self, dataset_id: str,
                      mappings: List[dict]) -> List[dict]:
        """Replace all mappings for a dataset."""
        conn = self._get_conn()
        conn.execute(
            "DELETE FROM field_mappings WHERE dataset_id = ?", (dataset_id,)
        )
        for m in mappings:
            mid = _new_id()
            conn.execute(
                "INSERT INTO field_mappings "
                "(id, dataset_id, source_field, target_field, data_type, transform, is_active) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (mid, dataset_id, m["source_field"], m["target_field"],
                 m.get("data_type", "text"), json.dumps(m.get("transform", {})),
                 1 if m.get("is_active", True) else 0),
            )
        conn.commit()
        return self.get_mappings(dataset_id)

    def update_mapping(self, dataset_id: str, mapping_id: str,
                       updates: dict) -> Optional[dict]:
        conn = self._get_conn()
        existing = conn.execute(
            "SELECT * FROM field_mappings WHERE id = ? AND dataset_id = ?",
            (mapping_id, dataset_id)
        ).fetchone()
        if not existing:
            return None
        sets = []
        params = []
        for col in ("source_field", "target_field", "data_type", "is_active"):
            if col in updates:
                sets.append(f"{col} = ?")
                val = updates[col]
                if col == "is_active":
                    val = 1 if val else 0
                params.append(val)
        if "transform" in updates:
            sets.append("transform = ?")
            params.append(json.dumps(updates["transform"]))
        if sets:
            params.append(mapping_id)
            conn.execute(
                f"UPDATE field_mappings SET {', '.join(sets)} WHERE id = ?",
                params,
            )
            conn.commit()
        row = conn.execute(
            "SELECT * FROM field_mappings WHERE id = ?", (mapping_id,)
        ).fetchone()
        return self._row_to_dict(row) if row else None

    # -------------------------------------------------------------------
    # CSV Import
    # -------------------------------------------------------------------

    def preview_csv(self, file_content: str, has_header: bool = True,
                    max_rows: int = 10) -> dict:
        """Parse CSV content and return column names + sample rows."""
        reader = csv.reader(io.StringIO(file_content))
        rows_raw = list(reader)
        if not rows_raw:
            return {"columns": [], "sample_rows": [], "total_rows": 0}
        if has_header:
            columns = rows_raw[0]
            data_rows = rows_raw[1:]
        else:
            columns = [f"col_{i}" for i in range(len(rows_raw[0]))]
            data_rows = rows_raw
        sample = []
        for row in data_rows[:max_rows]:
            record = {}
            for i, col in enumerate(columns):
                record[col] = row[i] if i < len(row) else ""
            sample.append(record)
        return {
            "columns": columns,
            "sample_rows": sample,
            "total_rows": len(data_rows),
        }

    def import_csv(self, dataset_id: str, file_content: str,
                   has_header: bool = True,
                   source_file: str = "",
                   field_mappings: Optional[List[dict]] = None) -> dict:
        """Parse CSV, apply field mappings, store rows."""
        conn = self._get_conn()
        ds = conn.execute(
            "SELECT * FROM datasets WHERE id = ?", (dataset_id,)
        ).fetchone()
        if not ds:
            raise ValueError(f"Dataset {dataset_id} not found")

        import_mode = ds["import_mode"]
        date_field = ds["date_field"]

        reader = csv.reader(io.StringIO(file_content))
        rows_raw = list(reader)
        if not rows_raw:
            return {"imported": 0, "errors": ["Empty CSV"]}

        if has_header:
            columns = rows_raw[0]
            data_rows = rows_raw[1:]
        else:
            columns = [f"col_{i}" for i in range(len(rows_raw[0]))]
            data_rows = rows_raw

        if field_mappings is None:
            db_mappings = conn.execute(
                "SELECT * FROM field_mappings WHERE dataset_id = ? AND is_active = 1",
                (dataset_id,)
            ).fetchall()
            mapping_dict = {}
            type_dict = {}
            for m in db_mappings:
                mapping_dict[m["source_field"]] = m["target_field"]
                type_dict[m["target_field"]] = m["data_type"]
        else:
            mapping_dict = {m["source_field"]: m["target_field"] for m in field_mappings}
            type_dict = {m["target_field"]: m.get("data_type", "text") for m in field_mappings}

        if import_mode == "replace_all":
            conn.execute(
                "DELETE FROM dataset_rows WHERE dataset_id = ?", (dataset_id,)
            )

        now = _now_iso()
        imported = 0
        errors = []

        for row_idx, row in enumerate(data_rows):
            try:
                record = {}
                for i, col in enumerate(columns):
                    val = row[i] if i < len(row) else ""
                    target = mapping_dict.get(col, col)
                    dtype = type_dict.get(target, "text")
                    record[target] = self._coerce_value(val, dtype)
                period = ""
                if date_field and date_field in record:
                    period = self._extract_period(record[date_field])
                rid = _new_id()
                conn.execute(
                    "INSERT INTO dataset_rows (id, dataset_id, period, data, imported_at, source_file) "
                    "VALUES (?, ?, ?, ?, ?, ?)",
                    (rid, dataset_id, period, json.dumps(record), now, source_file),
                )
                imported += 1
            except Exception as e:
                errors.append(f"Row {row_idx + 1}: {str(e)}")
                if len(errors) >= 100:
                    errors.append("... (too many errors, stopping)")
                    break

        schema = []
        for col in columns:
            target = mapping_dict.get(col, col)
            dtype = type_dict.get(target, "text")
            schema.append({"field": target, "source": col, "type": dtype})
        conn.execute(
            "UPDATE datasets SET schema_definition = ?, updated_at = ? WHERE id = ?",
            (json.dumps(schema), now, dataset_id),
        )
        conn.commit()
        return {
            "dataset_id": dataset_id,
            "imported": imported,
            "total_rows": len(data_rows),
            "errors": errors,
            "source_file": source_file,
        }

    # -------------------------------------------------------------------
    # Calculated Fields
    # -------------------------------------------------------------------

    def list_calculated_fields(self, dataset_id: str) -> List[dict]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM calculated_fields WHERE dataset_id = ? ORDER BY created_at",
            (dataset_id,)
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def create_calculated_field(self, dataset_id: str, name: str,
                                formula: str,
                                formula_type: str = "simple_math",
                                depends_on: Optional[list] = None,
                                description: str = "") -> dict:
        conn = self._get_conn()
        cid = _new_id()
        now = _now_iso()
        conn.execute(
            "INSERT INTO calculated_fields "
            "(id, dataset_id, name, formula, formula_type, depends_on, description, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (cid, dataset_id, name, formula, formula_type,
             json.dumps(depends_on or []), description, now),
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM calculated_fields WHERE id = ?", (cid,)
        ).fetchone()
        return self._row_to_dict(row)

    def update_calculated_field(self, dataset_id: str, calc_id: str,
                                updates: dict) -> Optional[dict]:
        conn = self._get_conn()
        existing = conn.execute(
            "SELECT * FROM calculated_fields WHERE id = ? AND dataset_id = ?",
            (calc_id, dataset_id)
        ).fetchone()
        if not existing:
            return None
        sets = []
        params = []
        for col in ("name", "formula", "formula_type", "description"):
            if col in updates:
                sets.append(f"{col} = ?")
                params.append(updates[col])
        if "depends_on" in updates:
            sets.append("depends_on = ?")
            params.append(json.dumps(updates["depends_on"]))
        if sets:
            params.append(calc_id)
            conn.execute(
                f"UPDATE calculated_fields SET {', '.join(sets)} WHERE id = ?",
                params,
            )
            conn.commit()
        row = conn.execute(
            "SELECT * FROM calculated_fields WHERE id = ?", (calc_id,)
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def delete_calculated_field(self, dataset_id: str, calc_id: str) -> bool:
        conn = self._get_conn()
        cur = conn.execute(
            "DELETE FROM calculated_fields WHERE id = ? AND dataset_id = ?",
            (calc_id, dataset_id),
        )
        conn.commit()
        return cur.rowcount > 0

    def evaluate_calculated_fields(self, dataset_id: str,
                                   row_data: dict,
                                   all_rows: Optional[List[dict]] = None) -> dict:
        """Compute all calculated fields for a dataset."""
        calcs = self.list_calculated_fields(dataset_id)
        result = {}
        extended = dict(row_data)
        for cf in calcs:
            try:
                val = evaluate_formula(cf["formula"], extended, all_rows)
                result[cf["name"]] = val
                extended[cf["name"]] = val
            except FormulaError:
                result[cf["name"]] = None
        return result

    # -------------------------------------------------------------------
    # Department Hierarchy
    # -------------------------------------------------------------------

    def list_departments(self) -> List[dict]:
        conn = self._get_conn()
        rows = conn.execute(
            "SELECT * FROM department_hierarchy ORDER BY level, name"
        ).fetchall()
        return [self._row_to_dict(r) for r in rows]

    def get_department_tree(self) -> List[dict]:
        all_depts = self.list_departments()
        by_id = {d["id"]: {**d, "children": []} for d in all_depts}
        roots = []
        for d in all_depts:
            node = by_id[d["id"]]
            pid = d.get("parent_id")
            if pid and pid in by_id:
                by_id[pid]["children"].append(node)
            else:
                roots.append(node)
        return roots

    def create_department(self, name: str,
                          parent_id: Optional[str] = None,
                          level: int = 0,
                          mapping_rules: Optional[dict] = None) -> dict:
        conn = self._get_conn()
        did = _new_id()
        now = _now_iso()
        conn.execute(
            "INSERT INTO department_hierarchy (id, name, parent_id, level, mapping_rules, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (did, name, parent_id, level, json.dumps(mapping_rules or {}), now),
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM department_hierarchy WHERE id = ?", (did,)
        ).fetchone()
        return self._row_to_dict(row)

    def update_department(self, dept_id: str, updates: dict) -> Optional[dict]:
        conn = self._get_conn()
        existing = conn.execute(
            "SELECT * FROM department_hierarchy WHERE id = ?", (dept_id,)
        ).fetchone()
        if not existing:
            return None
        sets = []
        params = []
        for col in ("name", "parent_id", "level"):
            if col in updates:
                sets.append(f"{col} = ?")
                params.append(updates[col])
        if "mapping_rules" in updates:
            sets.append("mapping_rules = ?")
            params.append(json.dumps(updates["mapping_rules"]))
        if sets:
            params.append(dept_id)
            conn.execute(
                f"UPDATE department_hierarchy SET {', '.join(sets)} WHERE id = ?",
                params,
            )
            conn.commit()
        row = conn.execute(
            "SELECT * FROM department_hierarchy WHERE id = ?", (dept_id,)
        ).fetchone()
        return self._row_to_dict(row) if row else None

    def delete_department(self, dept_id: str) -> bool:
        conn = self._get_conn()
        cur = conn.execute(
            "DELETE FROM department_hierarchy WHERE id = ?", (dept_id,)
        )
        conn.commit()
        return cur.rowcount > 0

    def add_department_mapping(self, department_id: str, dataset_id: str,
                               field_name: str, field_value: str) -> dict:
        conn = self._get_conn()
        mid = _new_id()
        conn.execute(
            "INSERT INTO department_mappings (id, department_id, dataset_id, field_name, field_value) "
            "VALUES (?, ?, ?, ?, ?)",
            (mid, department_id, dataset_id, field_name, field_value),
        )
        conn.commit()
        row = conn.execute(
            "SELECT * FROM department_mappings WHERE id = ?", (mid,)
        ).fetchone()
        return self._row_to_dict(row)

    def get_department_rollup(self, department_id: str, dataset_id: str,
                              period: Optional[str] = None) -> dict:
        """Aggregate data for a department and all its children."""
        conn = self._get_conn()
        dept_ids = self._get_descendant_ids(department_id)
        placeholders = ",".join("?" for _ in dept_ids)
        dm_rows = conn.execute(
            f"SELECT field_name, field_value FROM department_mappings "
            f"WHERE department_id IN ({placeholders}) AND dataset_id = ?",
            (*dept_ids, dataset_id),
        ).fetchall()

        base_query = "SELECT data FROM dataset_rows WHERE dataset_id = ?"
        base_params: list = [dataset_id]
        if period:
            base_query += " AND period = ?"
            base_params.append(period)

        all_rows_raw = conn.execute(base_query, base_params).fetchall()
        all_rows = [json.loads(r[0]) for r in all_rows_raw]

        mapping_filters = [(r["field_name"], r["field_value"]) for r in dm_rows]
        if mapping_filters:
            matched = []
            for row in all_rows:
                for fname, fval in mapping_filters:
                    if str(row.get(fname, "")) == fval:
                        matched.append(row)
                        break
            filtered = matched
        else:
            filtered = all_rows

        aggregated: Dict[str, float] = {}
        for row in filtered:
            for k, v in row.items():
                try:
                    num = float(v)
                    aggregated[k] = aggregated.get(k, 0) + num
                except (ValueError, TypeError):
                    pass

        return {
            "department_id": department_id,
            "dataset_id": dataset_id,
            "period": period,
            "row_count": len(filtered),
            "aggregated": aggregated,
        }

    def _get_descendant_ids(self, dept_id: str) -> List[str]:
        conn = self._get_conn()
        result = [dept_id]
        queue = [dept_id]
        while queue:
            current = queue.pop(0)
            children = conn.execute(
                "SELECT id FROM department_hierarchy WHERE parent_id = ?",
                (current,)
            ).fetchall()
            for c in children:
                result.append(c[0])
                queue.append(c[0])
        return result

    # -------------------------------------------------------------------
    # Data Query
    # -------------------------------------------------------------------

    def query_data(self, dataset_id: str,
                   filters: Optional[Dict[str, Any]] = None,
                   group_by: Optional[List[str]] = None,
                   include_calculated: bool = True,
                   period: Optional[str] = None,
                   limit: int = 1000,
                   offset: int = 0) -> dict:
        """Flexible data query with filtering, grouping, calculated fields."""
        conn = self._get_conn()
        query = "SELECT data, period FROM dataset_rows WHERE dataset_id = ?"
        params: list = [dataset_id]
        if period:
            query += " AND period = ?"
            params.append(period)
        query += " ORDER BY imported_at DESC"

        raw = conn.execute(query, params).fetchall()
        rows = []
        for r in raw:
            d = json.loads(r[0])
            d["_period"] = r[1]
            rows.append(d)

        if filters:
            rows = self._apply_filters(rows, filters)

        if include_calculated:
            calcs = self.list_calculated_fields(dataset_id)
            if calcs:
                for row in rows:
                    extended = dict(row)
                    for cf in calcs:
                        try:
                            val = evaluate_formula(cf["formula"], extended, rows)
                            row[cf["name"]] = val
                            extended[cf["name"]] = val
                        except FormulaError:
                            row[cf["name"]] = None

        total = len(rows)

        if group_by:
            rows = self._group_rows(rows, group_by)
            total = len(rows)

        paginated = rows[offset: offset + limit]
        return {
            "dataset_id": dataset_id,
            "total": total,
            "offset": offset,
            "limit": limit,
            "rows": paginated,
        }

    def _apply_filters(self, rows: List[dict],
                       filters: Dict[str, Any]) -> List[dict]:
        result = []
        for row in rows:
            match = True
            for field, condition in filters.items():
                val = row.get(field)
                if isinstance(condition, dict):
                    op = condition.get("op", "eq")
                    cmp_val = condition.get("value")
                    try:
                        val_f = float(val) if val is not None else None
                        cmp_f = float(cmp_val) if cmp_val is not None else None
                    except (ValueError, TypeError):
                        val_f = None
                        cmp_f = None
                    if op == "eq" and str(val) != str(cmp_val):
                        match = False
                    elif op == "neq" and str(val) == str(cmp_val):
                        match = False
                    elif op == "gt" and (val_f is None or cmp_f is None or val_f <= cmp_f):
                        match = False
                    elif op == "gte" and (val_f is None or cmp_f is None or val_f < cmp_f):
                        match = False
                    elif op == "lt" and (val_f is None or cmp_f is None or val_f >= cmp_f):
                        match = False
                    elif op == "lte" and (val_f is None or cmp_f is None or val_f > cmp_f):
                        match = False
                    elif op == "contains" and str(cmp_val) not in str(val):
                        match = False
                    elif op == "in" and str(val) not in [str(v) for v in (cmp_val if isinstance(cmp_val, list) else [cmp_val])]:
                        match = False
                else:
                    if str(val) != str(condition):
                        match = False
                if not match:
                    break
            if match:
                result.append(row)
        return result

    def _group_rows(self, rows: List[dict],
                    group_by: List[str]) -> List[dict]:
        groups: Dict[str, dict] = {}
        for row in rows:
            key = tuple(str(row.get(g, "")) for g in group_by)
            key_str = "|".join(key)
            if key_str not in groups:
                groups[key_str] = {g: row.get(g, "") for g in group_by}
                groups[key_str]["_count"] = 0
            groups[key_str]["_count"] += 1
            for k, v in row.items():
                if k in group_by or k.startswith("_"):
                    continue
                try:
                    num = float(v)
                    groups[key_str][k] = groups[key_str].get(k, 0) + num
                except (ValueError, TypeError):
                    if k not in groups[key_str]:
                        groups[key_str][k] = v
        return list(groups.values())

    # -------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------

    @staticmethod
    def _row_to_dict(row: sqlite3.Row) -> dict:
        d = dict(row)
        for key in ("connection_config", "schema_definition", "transform",
                     "depends_on", "mapping_rules", "data"):
            if key in d and isinstance(d[key], str):
                try:
                    d[key] = json.loads(d[key])
                except (json.JSONDecodeError, TypeError):
                    pass
        for key in ("is_active", "archive_monthly"):
            if key in d:
                d[key] = bool(d[key])
        return d

    @staticmethod
    def _coerce_value(val: str, dtype: str) -> Any:
        if not val or val.strip() == "":
            if dtype in ("number", "currency"):
                return 0
            return ""
        val = val.strip()
        if dtype == "number":
            cleaned = re.sub(r"[,$\xe2\x82\xac\xc2\xa3\xc2\xa5]", "", val)
            try:
                return float(cleaned)
            except ValueError:
                return val
        if dtype == "currency":
            cleaned = re.sub(r"[,$\xe2\x82\xac\xc2\xa3\xc2\xa5\s]", "", val)
            try:
                return round(float(cleaned), 2)
            except ValueError:
                return val
        if dtype == "date":
            for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y",
                        "%Y-%m-%dT%H:%M:%S", "%b %d, %Y", "%B %d, %Y",
                        "%Y-%m"):
                try:
                    return datetime.strptime(val, fmt).strftime("%Y-%m-%d")
                except ValueError:
                    continue
            return val
        return val

    @staticmethod
    def _extract_period(date_val) -> str:
        if isinstance(date_val, str):
            if re.match(r"^\d{4}-\d{2}$", date_val):
                return date_val
            m = re.match(r"(\d{4})-(\d{2})", date_val)
            if m:
                return f"{m.group(1)}-{m.group(2)}"
            for fmt in ("%m/%d/%Y", "%d/%m/%Y", "%Y-%m-%dT%H:%M:%S"):
                try:
                    dt = datetime.strptime(date_val, fmt)
                    return dt.strftime("%Y-%m")
                except ValueError:
                    continue
        return ""
