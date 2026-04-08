from datetime import date
from pathlib import Path

from zgb_opti.models import OptimizationJob, StudyConfig


def load_base_ini(path: str | Path) -> str:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Base ini file not found: {p}")
    return p.read_text(encoding="utf-8")


def _load_tester_inputs(set_path: str | Path) -> str:
    """Read a MT5 .set file and return clean parameter lines for [TesterInputs].

    Strips comment lines (;) and blank lines.  Handles both UTF-8 and UTF-16 LE
    encodings (MT5 saves set files as UTF-16 LE).
    """
    p = Path(set_path)
    if not p.is_absolute():
        p = p.resolve()
    if not p.exists():
        raise FileNotFoundError(f"Set file not found: {p}")
    raw = p.read_bytes()
    # Detect UTF-16 LE BOM (FF FE)
    if raw[:2] == b"\xff\xfe":
        text = raw.decode("utf-16-le", errors="replace").lstrip("\ufeff")
    else:
        text = raw.decode("utf-8", errors="replace")
    lines = []
    for line in text.splitlines():
        stripped = line.strip().replace("\x00", "")
        if not stripped or stripped.startswith(";"):
            continue
        # MT5 .set files store 6-part format: value||default||step||min||max||flag
        # MT5 [TesterInputs] ini format requires 5-part: value||min||step||max||flag
        if "=" in stripped and "||" in stripped:
            name, _, rest = stripped.partition("=")
            parts = rest.split("||")
            if len(parts) == 6:
                stripped = f"{name}={parts[0]}||{parts[3]}||{parts[2]}||{parts[4]}||{parts[5]}"
        lines.append(stripped)
    return "\n".join(lines)


def build_ini_content(base_ini_text: str, job: OptimizationJob, config: StudyConfig) -> str:
    from_date = job.train_start.strftime("%Y.%m.%d")
    to_date = job.train_end.strftime("%Y.%m.%d")

    # Use a relative report name so MT5 writes to its own MQL5/Profiles/Tester/
    # directory — absolute paths are silently ignored in some MT5 builds.
    report_name = job.job_id

    tester_inputs = _load_tester_inputs(config.base_set_path)

    return (
        "[Common]\n"
        f"Login={config.mt5_login}\n"
        f"Server={config.mt5_server}\n"
        "ProxyEnable=0\n"
        "CertInstall=0\n"
        "NewsEnable=0\n"
        "\n"
        "[Charts]\n"
        "\n"
        "[Experts]\n"
        "\n"
        "[Tester]\n"
        f"Expert={config.ea_path}\n"
        f"Symbol={job.symbol}\n"
        f"Period={job.timeframe}\n"
        f"Model={config.optimization_model}\n"
        "Optimization=1\n"
        f"Deposit={config.deposit}\n"
        f"Spread={config.spread}\n"
        f"FromDate={from_date}\n"
        f"ToDate={to_date}\n"
        "ForwardMode=0\n"
        "Visual=0\n"
        "TesterStart=1\n"
        "ReplaceReport=1\n"
        "ShutdownTerminal=1\n"
        f"Report={report_name}\n"
        "\n"
        "[TesterInputs]\n"
        f"{tester_inputs}\n"
    )


def write_ini_file(job: OptimizationJob, ini_content: str) -> Path:
    path = Path(job.ini_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(ini_content, encoding="utf-8")
    return path


def generate_ini_files_for_jobs(
    jobs: list[OptimizationJob],
    config: StudyConfig,
    base_ini_path: str | Path,
) -> list[Path]:
    base_text = load_base_ini(base_ini_path)
    written: list[Path] = []
    for job in jobs:
        content = build_ini_content(base_text, job, config)
        written.append(write_ini_file(job, content))
    return written


def build_forward_ini_content(
    param_values: dict[str, str],
    forward_start: date,
    forward_end: date,
    config: StudyConfig,
    fwd_id: str,
    symbol: str,
    timeframe: str,
) -> str:
    """Build MT5 tester ini for a single forward backtest (Optimization=0).

    param_values: {param_name: formatted_value} — candidate parameter set.
    fwd_id: report name written to MT5's Profiles/Tester directory.
    """
    from_date = forward_start.strftime("%Y.%m.%d")
    to_date = forward_end.strftime("%Y.%m.%d")
    tester_inputs = "\n".join(f"{k}={v}" for k, v in param_values.items())

    return (
        "[Common]\n"
        f"Login={config.mt5_login}\n"
        f"Server={config.mt5_server}\n"
        "ProxyEnable=0\n"
        "CertInstall=0\n"
        "NewsEnable=0\n"
        "\n"
        "[Charts]\n"
        "\n"
        "[Experts]\n"
        "\n"
        "[Tester]\n"
        f"Expert={config.ea_path}\n"
        f"Symbol={symbol}\n"
        f"Period={timeframe}\n"
        f"Model={config.backtest_model}\n"
        "Optimization=0\n"
        f"Deposit={config.deposit}\n"
        f"Spread={config.spread}\n"
        f"FromDate={from_date}\n"
        f"ToDate={to_date}\n"
        "ForwardMode=0\n"
        "Visual=0\n"
        "TesterStart=1\n"
        "ReplaceReport=1\n"
        "ShutdownTerminal=1\n"
        f"Report={fwd_id}\n"
        "\n"
        "[TesterInputs]\n"
        f"{tester_inputs}\n"
    )
