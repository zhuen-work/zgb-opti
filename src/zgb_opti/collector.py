import json
import os
import shutil
from pathlib import Path

from zgb_opti.models import ManifestRow


def default_manifest_path() -> Path:
    return Path("data/manifests/job_runs.jsonl")


def ensure_output_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _find_mt5_profiles_tester(mt5_terminal_path: str) -> Path | None:
    """Return MQL5/Profiles/Tester dir for this MT5 install, or None if not found.

    MT5 stores per-installation data under
    %APPDATA%/MetaQuotes/Terminal/{HASH}/ where {HASH} is derived from the
    terminal path.  We locate the right directory by reading each origin.txt.
    """
    appdata = Path(os.environ.get("APPDATA", ""))
    mt5_base = appdata / "MetaQuotes" / "Terminal"
    if not mt5_base.exists():
        return None
    # origin.txt stores the terminal install directory; config stores the exe path
    terminal_dir = Path(mt5_terminal_path).resolve().parent
    for origin_file in mt5_base.glob("*/origin.txt"):
        try:
            raw = origin_file.read_bytes()
            # origin.txt is UTF-16 LE (with or without BOM)
            if raw[:2] == b"\xff\xfe":
                text = raw.decode("utf-16-le", errors="replace").lstrip("\ufeff")
            else:
                text = raw.decode("utf-8", errors="replace")
            text = text.replace("\x00", "").strip()
            if Path(text).resolve() == terminal_dir:
                profiles_dir = origin_file.parent / "MQL5" / "Profiles" / "Tester"
                if profiles_dir.exists():
                    return profiles_dir
        except Exception:
            continue
    return None


def find_report_artifact(
    output_dir: str | Path,
    job_id: str,
    mt5_terminal_path: str | None = None,
) -> Path | None:
    # Primary: check our own output directory (used when Report= is absolute)
    d = Path(output_dir)
    for ext in (".xml", ".html", ".htm"):
        candidate = d / f"{job_id}{ext}"
        if candidate.exists():
            return candidate

    # Fallback: check MT5's data directories (Report= writes to terminal data root)
    if mt5_terminal_path:
        profiles_dir = _find_mt5_profiles_tester(mt5_terminal_path)
        if profiles_dir:
            # MT5 writes Report={name} to the terminal data root (3 levels above Profiles/Tester)
            terminal_data_root = profiles_dir.parent.parent.parent
            for ext in (".xml", ".html", ".htm"):
                candidate = terminal_data_root / f"{job_id}{ext}"
                if candidate.exists():
                    return candidate

            # Also check Profiles/Tester itself for the job_id-named file
            for ext in (".xml", ".html", ".htm"):
                candidate = profiles_dir / f"{job_id}{ext}"
                if candidate.exists():
                    return candidate

    return None


def copy_report_artifact(report_path: str | Path, output_dir: str | Path) -> Path:
    import time
    src = Path(report_path)
    dst_dir = Path(output_dir)
    dst_dir.mkdir(parents=True, exist_ok=True)
    dst = dst_dir / src.name
    for attempt in range(10):
        try:
            shutil.copy2(src, dst)
            return dst.resolve()
        except PermissionError:
            if attempt == 9:
                raise
            time.sleep(2)
    return dst.resolve()


def append_manifest_row(manifest_path: str | Path, row: ManifestRow) -> None:
    path = Path(manifest_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = row.model_dump(mode="json")
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(data, separators=(",", ":")) + "\n")
