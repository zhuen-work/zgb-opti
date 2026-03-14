from pathlib import Path

import yaml
from pydantic import ValidationError

from zgb_opti.models import StudyConfig


def load_study_config(path: str | Path) -> StudyConfig:
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")
    try:
        raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    except yaml.YAMLError as e:
        raise ValueError(f"Failed to parse YAML in {path}: {e}") from e
    try:
        return StudyConfig(**raw)
    except ValidationError as e:
        raise ValueError(f"Invalid study config in {path}:\n{e}") from e
