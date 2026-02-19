from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
import threading

import pandas as pd
from fastmcp import FastMCP
from starlette.responses import JSONResponse, PlainTextResponse


mcp = FastMCP("csv-analyst")

_lock = threading.Lock()


class _State:
    def __init__(self) -> None:
        self.csv_path: Optional[str] = None
        self.df: Optional[pd.DataFrame] = None
        self.last_result: Optional[pd.DataFrame] = None
        self.base_dir: Path = self._default_base_dir()

    def _default_base_dir(self) -> Path:
        env_dir = os.getenv("CSV_MCP_BASEDIR")
        if env_dir:
            return Path(env_dir).expanduser().resolve()
        return Path.home().resolve()


STATE = _State()


def _resolve_csv_path(user_path: str) -> Path:
    """
    Resolve and validate a CSV file path.

    Security model:
    - By default, only allow paths under STATE.base_dir (defaults to user home).
    - You can widen/narrow by setting CSV_MCP_BASEDIR env var before starting the server.
    """
    p = Path(user_path).expanduser()
    if not p.is_absolute():
        p = (STATE.base_dir / p).resolve()
    else:
        p = p.resolve()

    if not str(p).lower().endswith(".csv"):
        raise ValueError("Only .csv files are allowed.")

    if not p.exists() or not p.is_file():
        raise FileNotFoundError(f"CSV file not found: {p}")

    base = STATE.base_dir
    try:
        p.relative_to(base)
    except Exception:
        raise PermissionError(
            f"Access denied. File must be under base directory: {base}. "
            f"Set CSV_MCP_BASEDIR to change the allowed root."
        )

    return p


def _ensure_loaded() -> pd.DataFrame:
    if STATE.df is None:
        raise RuntimeError("No CSV loaded. Call load_csv first.")
    return STATE.df


def _df_to_rows(df: pd.DataFrame, limit: int) -> List[Dict[str, Any]]:
    if limit <= 0:
        return []
    out = df.head(limit)
    return out.to_dict(orient="records")


def _basic_profile(df: pd.DataFrame) -> Dict[str, Any]:
    return {
        "row_count": int(df.shape[0]),
        "column_count": int(df.shape[1]),
        "columns": list(df.columns),
        "dtypes": {c: str(df[c].dtype) for c in df.columns},
        "missing_by_column": {c: int(df[c].isna().sum()) for c in df.columns},
    }


@mcp.tool()
def set_base_directory(path: str) -> Dict[str, Any]:
    """
    Set the allowed base directory for CSV access.
    This restricts which files can be opened for safety.

    Example:
      set_base_directory("C:\\Users\\pchitnbh\\Documents\\data")
    """
    with _lock:
        base = Path(path).expanduser().resolve()
        if not base.exists() or not base.is_dir():
            raise FileNotFoundError(f"Directory not found: {base}")
        STATE.base_dir = base
        return {"ok": True, "base_dir": str(STATE.base_dir)}


@mcp.tool()
def get_base_directory() -> Dict[str, Any]:
    """
    Returns the current allowed base directory for CSV access.
    """
    with _lock:
        return {"base_dir": str(STATE.base_dir)}


@mcp.tool()
def load_csv(
    path: str,
    delimiter: str = ",",
    encoding: Optional[str] = None,
    sample_rows: int = 10,
) -> Dict[str, Any]:
    """
    Load a CSV file from your computer.

    Notes:
    - For safety, the file must be under the configured base directory.
    - If you want a different root, call set_base_directory or set CSV_MCP_BASEDIR before starting.

    Returns:
    - Basic profile + a small preview.
    """
    with _lock:
        csv_path = _resolve_csv_path(path)

        read_kwargs: Dict[str, Any] = {"sep": delimiter}
        if encoding:
            read_kwargs["encoding"] = encoding

        df = pd.read_csv(csv_path, **read_kwargs)

        STATE.csv_path = str(csv_path)
        STATE.df = df
        STATE.last_result = None

        return {
            "ok": True,
            "csv_path": STATE.csv_path,
            "profile": _basic_profile(df),
            "preview": _df_to_rows(df, sample_rows),
        }


@mcp.tool()
def get_schema() -> Dict[str, Any]:
    """
    Return CSV schema details: columns, types, and missing counts.
    """
    with _lock:
        df = _ensure_loaded()
        return {"ok": True, "csv_path": STATE.csv_path, "profile": _basic_profile(df)}


@mcp.tool()
def preview(rows: int = 20, columns: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Preview rows from the loaded CSV.

    Args:
      rows: number of rows to return
      columns: optional list of columns to include

    Returns:
      List of row objects.
    """
    with _lock:
        df = _ensure_loaded()
        out = df
        if columns:
            missing = [c for c in columns if c not in df.columns]
            if missing:
                return {"ok": False, "error": f"Unknown columns: {missing}"}
            out = df[columns]

        STATE.last_result = out.copy()
        return {"ok": True, "rows": _df_to_rows(out, rows)}


@mcp.tool()
def describe_numeric() -> Dict[str, Any]:
    """
    Return summary stats for numeric columns only.
    """
    with _lock:
        df = _ensure_loaded()
        desc = df.describe(include="number").transpose()
        STATE.last_result = desc.reset_index().rename(columns={"index": "column"})
        return {"ok": True, "summary": STATE.last_result.to_dict(orient="records")}


@mcp.tool()
def value_counts(column: str, limit: int = 25, normalize: bool = False, dropna: bool = True) -> Dict[str, Any]:
    """
    Value counts for a single column.

    Great for:
    - "What are the top categories in X?"
    - "How many records per status?"

    Returns:
      List of {value, count, proportion?}
    """
    with _lock:
        df = _ensure_loaded()
        if column not in df.columns:
            return {"ok": False, "error": f"Unknown column: {column}"}

        vc = df[column].value_counts(dropna=dropna, normalize=normalize).head(limit)
        result = []
        for idx, val in vc.items():
            result.append({"value": idx if pd.notna(idx) else None, "metric": float(val) if normalize else int(val)})

        STATE.last_result = pd.DataFrame(result)
        return {"ok": True, "column": column, "results": result}


@mcp.tool()
def filter_rows(
    filters: List[Dict[str, Any]],
    logic: str = "and",
    limit: int = 200,
) -> Dict[str, Any]:
    """
    Filter rows using structured filters (safe).

    Each filter is a dict:
      {
        "column": "Status",
        "op": "==|!=|>|>=|<|<=|contains|startswith|endswith|in",
        "value": "Open",
        "case_sensitive": false
      }

    logic: "and" or "or"

    Returns:
      Filtered rows, and row_count.
    """
    with _lock:
        df = _ensure_loaded()

        if logic not in ["and", "or"]:
            return {"ok": False, "error": "logic must be 'and' or 'or'"}

        mask = None
        for f in filters:
            col = f.get("column")
            op = f.get("op")
            val = f.get("value")
            case_sensitive = bool(f.get("case_sensitive", False))

            if not col or col not in df.columns:
                return {"ok": False, "error": f"Unknown or missing column in filter: {col}"}

            series = df[col]

            if op in ["==", "!=", ">", ">=", "<", "<="]:
                ops = {
                    "==": series.eq,
                    "!=": series.ne,
                    ">": series.gt,
                    ">=": series.ge,
                    "<": series.lt,
                    "<=": series.le,
                }
                try:
                    m = ops[op](val)
                except Exception as e:
                    return {"ok": False, "error": f"Comparison failed for {col} {op} {val}: {e}"}

            elif op in ["contains", "startswith", "endswith"]:
                s = series.astype("string")
                needle = str(val)
                if not case_sensitive:
                    s = s.str.lower()
                    needle = needle.lower()

                if op == "contains":
                    m = s.str.contains(needle, na=False)
                elif op == "startswith":
                    m = s.str.startswith(needle, na=False)
                else:
                    m = s.str.endswith(needle, na=False)

            elif op == "in":
                if not isinstance(val, list):
                    return {"ok": False, "error": "For op='in', value must be a list."}
                m = series.isin(val)

            else:
                return {"ok": False, "error": f"Unsupported op: {op}"}

            if mask is None:
                mask = m
            else:
                mask = (mask & m) if logic == "and" else (mask | m)

        if mask is None:
            out = df
        else:
            out = df[mask]

        STATE.last_result = out.copy()
        return {"ok": True, "row_count": int(out.shape[0]), "rows": _df_to_rows(out, limit)}


@mcp.tool()
def groupby_aggregate(
    group_columns: List[str],
    aggregations: List[Dict[str, Any]],
    limit: int = 200,
) -> Dict[str, Any]:
    """
    Group and aggregate.

    group_columns: list of columns to group by
    aggregations: list of dicts like:
      {"column": "Amount", "agg": "sum|mean|min|max|count|nunique"}

    Example:
      groupby_aggregate(
        ["Department"],
        [{"column":"WorkerId","agg":"nunique"}]
      )
    """
    with _lock:
        df = _ensure_loaded()

        missing = [c for c in group_columns if c not in df.columns]
        if missing:
            return {"ok": False, "error": f"Unknown group columns: {missing}"}

        agg_map: Dict[str, List[str]] = {}
        for a in aggregations:
            col = a.get("column")
            fn = a.get("agg")
            if not col or col not in df.columns:
                return {"ok": False, "error": f"Unknown agg column: {col}"}
            if fn not in ["sum", "mean", "min", "max", "count", "nunique"]:
                return {"ok": False, "error": f"Unsupported agg: {fn}"}
            agg_map.setdefault(col, []).append(fn)

        grouped = df.groupby(group_columns, dropna=False).agg(agg_map)
        grouped.columns = ["_".join([c, f]) for c, f in grouped.columns]
        grouped = grouped.reset_index()

        STATE.last_result = grouped
        return {"ok": True, "row_count": int(grouped.shape[0]), "rows": _df_to_rows(grouped, limit)}


@mcp.tool()
def export_last_result(output_path: str) -> Dict[str, Any]:
    """
    Export the last result (from preview, filter_rows, groupby_aggregate, etc.) to a CSV.

    For safety, output must be written under the base directory.
    """
    with _lock:
        if STATE.last_result is None:
            return {"ok": False, "error": "No last_result available. Run a tool that produces a result first."}

        out = Path(output_path).expanduser()
        if not out.is_absolute():
            out = (STATE.base_dir / out).resolve()
        else:
            out = out.resolve()

        base = STATE.base_dir
        try:
            out.relative_to(base)
        except Exception:
            return {"ok": False, "error": f"Access denied. Output must be under base directory: {base}"}

        out.parent.mkdir(parents=True, exist_ok=True)
        STATE.last_result.to_csv(out, index=False)

        return {"ok": True, "output_path": str(out), "rows_written": int(STATE.last_result.shape[0])}


@mcp.custom_route("/", methods=["GET"])
async def root(_request):
    return PlainTextResponse(
        "csv-analyst MCP server is running.\n"
        "MCP endpoint: /mcp\n"
        "Health: /health\n",
        status_code=200,
    )


@mcp.custom_route("/health", methods=["GET"])
async def health(_request):
    return JSONResponse({"status": "healthy", "service": "csv-analyst"})


app = mcp.http_app()