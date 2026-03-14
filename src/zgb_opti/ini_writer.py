from pathlib import Path

from zgb_opti.models import OptimizationJob, StudyConfig


def load_base_ini(path: str | Path) -> str:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Base ini file not found: {p}")
    return p.read_text(encoding="utf-8")


def build_ini_content(base_ini_text: str, job: OptimizationJob, config: StudyConfig) -> str:
    from_date = job.train_start.strftime("%Y.%m.%d")
    to_date = job.train_end.strftime("%Y.%m.%d")

    return (
        "[Common]\n"
        "Login=0\n"
        "Password=\n"
        "Server=\n"
        "ProxyEnable=0\n"
        "CertInstall=0\n"
        "NewsEnable=0\n"
        "\n"
        "[Charts]\n"
        "ProfileLast=\n"
        "MaxBars=50000\n"
        "\n"
        "[Experts]\n"
        "AllowLiveTrading=0\n"
        "AllowDllImport=1\n"
        "Enabled=1\n"
        "\n"
        "[Tester]\n"
        f"Expert={config.ea_path}\n"
        f"ExpertParameters={config.base_set_path}\n"
        f"Symbol={job.symbol}\n"
        f"Period={job.timeframe}\n"
        "Model=0\n"
        "ExecutionMode=0\n"
        "Optimization=1\n"
        f"FromDate={from_date}\n"
        f"ToDate={to_date}\n"
        "ForwardMode=0\n"
        "ReplaceReport=1\n"
        "ShutdownTerminal=1\n"
        f"Report={job.job_id}\n"
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
