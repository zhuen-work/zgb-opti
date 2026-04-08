"""Parse MT5 SpreadsheetML optimization reports into structured records."""
from __future__ import annotations

import re
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from pathlib import Path

_SS = "urn:schemas-microsoft-com:office:spreadsheet"
_T = lambda n: f"{{{_SS}}}{n}"  # noqa: E731

# Fixed MT5 column names → normalized schema names
METRIC_RENAME: dict[str, str] = {
    "Pass": "pass_id",
    "Result": "result",
    "Profit": "net_profit",
    "Expected Payoff": "expected_payoff",
    "Profit Factor": "profit_factor",
    "Recovery Factor": "recovery_factor",
    "Sharpe Ratio": "sharpe_ratio",
    "Custom": "custom_criterion",
    "Equity DD %": "drawdown_pct",
    "Trades": "trades",
}

_INT_COLS = {"pass_id", "trades"}


def _build_col_map(headers: list[str]) -> dict[str, str]:
    col_map: dict[str, str] = {}
    for h in headers:
        if h in METRIC_RENAME:
            col_map[h] = METRIC_RENAME[h]
        else:
            col_map[h] = f"param_{h}"
    return col_map


def _cell_value(cell: ET.Element) -> float | str | None:
    data = cell.find(_T("Data"))
    if data is None or data.text is None:
        return None
    if data.get(f"{{{_SS}}}Type") == "Number":
        try:
            return float(data.text)
        except ValueError:
            pass
    return data.text


def parse_optimization_xml(
    xml_path: str | Path,
    job_id: str,
) -> tuple[list[dict], list[str]]:
    """Parse MT5 SpreadsheetML optimization report.

    Returns (records, warnings) where records is a list of dicts,
    one per optimization pass.
    """
    p = Path(xml_path)
    warns: list[str] = []

    try:
        tree = ET.parse(p)
    except ET.ParseError as e:
        raise ValueError(f"XML parse error in {p}: {e}") from e

    root = tree.getroot()
    table = root.find(f".//{_T('Worksheet')}/{_T('Table')}")
    if table is None:
        raise ValueError(f"No Table found in {p}")

    rows = table.findall(_T("Row"))
    if not rows:
        raise ValueError(f"Empty table in {p}")

    # Header row
    headers: list[str] = []
    for cell in rows[0].findall(_T("Cell")):
        data = cell.find(_T("Data"))
        headers.append(data.text.strip() if data is not None and data.text else "")

    if not headers:
        raise ValueError(f"No headers in {p}")

    col_map = _build_col_map(headers)

    unknown = [
        h for h in headers
        if h and h not in METRIC_RENAME and not h.startswith("Inp")
    ]
    if unknown:
        warns.append(f"Unrecognised columns treated as params: {unknown}")

    # Data rows
    records: list[dict] = []
    for row in rows[1:]:
        cells = row.findall(_T("Cell"))
        record: dict = {"job_id": job_id, "source_xml": str(p.resolve().as_posix())}
        for i, cell in enumerate(cells):
            if i >= len(headers):
                break
            h = headers[i]
            if not h:
                continue
            record[col_map[h]] = _cell_value(cell)
        records.append(record)

    if not records:
        warns.append("No data rows found")

    return records, warns


def write_passes(
    records: list[dict],
    output_dir: str | Path,
) -> tuple[Path, Path]:
    """Write records to passes.parquet and passes.csv. Returns (parquet_path, csv_path)."""
    try:
        import pandas as pd
    except ImportError as e:
        raise ImportError(
            "pandas is required for Phase 3. Run: pip install pandas pyarrow"
        ) from e

    d = Path(output_dir)
    d.mkdir(parents=True, exist_ok=True)

    df = pd.DataFrame(records)

    # Coerce numeric columns; force pass_id and trades to nullable int
    for col in df.columns:
        if col not in ("job_id", "source_xml"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
    for col in _INT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    parquet_path = d / "passes.parquet"
    csv_path = d / "passes.csv"

    df.to_parquet(parquet_path, index=False)
    df.to_csv(csv_path, index=False)

    return parquet_path, csv_path


def parse_and_write(
    xml_path: str | Path,
    output_dir: str | Path,
    job_id: str,
) -> tuple[Path, Path, int, list[str]]:
    """Parse XML, write outputs. Returns (parquet_path, csv_path, n_rows, warnings)."""
    records, warns = parse_optimization_xml(xml_path, job_id)
    parquet_path, csv_path = write_passes(records, output_dir)
    return parquet_path, csv_path, len(records), warns


# ---------------------------------------------------------------------------
# Forward test report parser (MT5 single backtest HTML)
# ---------------------------------------------------------------------------

# MT5 HTML report summary label → normalized key (lowercase, stripped of trailing colon/space)
# Actual MT5 labels as observed in Strategy Tester HTML reports (Build 5660+)
_FWD_LABEL_MAP: dict[str, str] = {
    # Primary labels (MT5 English)
    "total net profit": "net_profit",
    "profit factor": "profit_factor",
    "expected payoff": "expected_payoff",
    "equity drawdown relative": "drawdown_pct",
    "total trades": "trades",
    # Fallback / older MT5 labels
    "net profit": "net_profit",
    "relative drawdown": "drawdown_pct",
    "balance drawdown relative": "drawdown_pct",
    "trades": "trades",
}

# Metrics where we want the percentage portion (may appear as "15.34% (123.45)")
_PCT_METRICS = {"drawdown_pct"}


class _TdExtractor(HTMLParser):
    """Collect text content from every <td> element."""

    def __init__(self) -> None:
        super().__init__()
        self._in_td = False
        self._buf: list[str] = []
        self.cells: list[str] = []

    def handle_starttag(self, tag: str, attrs: list) -> None:
        if tag == "td":
            self._in_td = True
            self._buf = []

    def handle_endtag(self, tag: str) -> None:
        if tag == "td":
            self._in_td = False
            self.cells.append("".join(self._buf).strip())

    def handle_data(self, data: str) -> None:
        if self._in_td:
            self._buf.append(data)


def _parse_mt5_number(s: str, want_pct: bool = False) -> float | None:
    """Parse MT5 numeric string.  Handles spaces/nbsp as thousands separators,
    optional % suffix.  If want_pct, extract the first %-bearing number."""
    s = s.replace("\xa0", " ").replace(",", "")
    if want_pct:
        # Try to find a number immediately before '%'
        m = re.search(r"(-?\d[\d .]*)\s*%", s)
        if m:
            s = m.group(1)
    # Remove spaces between digits (thousands separator)
    s = re.sub(r"(?<=\d) (?=\d)", "", s)
    m2 = re.search(r"-?\d+\.?\d*", s)
    if m2 is None:
        return None
    try:
        return float(m2.group())
    except ValueError:
        return None


def parse_forward_report(
    report_path: str | Path,
    job_id: str,
) -> tuple[dict, list[str]]:
    """Parse MT5 single backtest HTML report.

    Returns (metrics_dict, warnings).  metrics_dict always contains 'job_id'.
    Missing metrics produce a warning but do not raise.
    """
    p = Path(report_path)
    warns: list[str] = []

    raw = p.read_bytes()
    if raw[:2] == b"\xff\xfe":
        text = raw.decode("utf-16-le", errors="replace").lstrip("\ufeff")
    elif raw[:2] == b"\xfe\xff":
        text = raw.decode("utf-16-be", errors="replace").lstrip("\ufeff")
    else:
        text = raw.decode("utf-8", errors="replace")

    ext = p.suffix.lower()
    metrics: dict = {"job_id": job_id}

    if ext == ".xml" or (text.lstrip().startswith("<?xml") or "<Workbook" in text[:500]):
        warns.append(f"Unexpected XML format for forward report at {p}; metrics not extracted")
        return metrics, warns

    extractor = _TdExtractor()
    extractor.feed(text)
    cells = extractor.cells

    # Scan sequential cells: when a cell matches a known label, next cell is value
    for i, cell in enumerate(cells):
        label = cell.lower().rstrip(": ").strip()
        if label not in _FWD_LABEL_MAP:
            continue
        if i + 1 >= len(cells):
            continue
        key = _FWD_LABEL_MAP[label]
        if key in metrics:  # take first occurrence only
            continue
        val_str = cells[i + 1]
        want_pct = key in _PCT_METRICS
        val = _parse_mt5_number(val_str, want_pct=want_pct)
        if val is not None:
            metrics[key] = val

    # final_equity: MT5 trade history ends with the ending balance in the last cell
    # (the "end of test" summary row's Balance column is always the last table cell)
    non_empty = [c for c in cells if c.strip()]
    if non_empty:
        last_val = _parse_mt5_number(non_empty[-1])
        if last_val is not None:
            metrics["final_equity"] = last_val

    missing = {"net_profit", "profit_factor", "drawdown_pct", "trades"} - set(metrics)
    if missing:
        warns.append(f"Missing forward metrics: {sorted(missing)}")

    return metrics, warns
