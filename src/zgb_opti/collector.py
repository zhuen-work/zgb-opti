import json
from pathlib import Path

from zgb_opti.models import ManifestRow


def default_manifest_path() -> Path:
    return Path("data/manifests/job_runs.jsonl")


def ensure_output_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def append_manifest_row(manifest_path: str | Path, row: ManifestRow) -> None:
    path = Path(manifest_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = row.model_dump(mode="json")
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(data, separators=(",", ":")) + "\n")
