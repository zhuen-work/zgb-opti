from datetime import date, datetime
from typing import Optional

from pydantic import BaseModel, field_validator, model_validator


class StudyConfig(BaseModel):
    study_name: str
    symbol: str
    timeframe: str
    ea_name: str
    ea_path: str
    base_set_path: str
    mt5_terminal_path: str
    mt5_login: int
    mt5_server: str
    output_root: str
    windows_weeks: list[int]
    study_start: date
    study_end: date
    step_weeks: int
    deposit: int = 1_000_000
    forward_weeks: int = 1
    optimization_model: int = 1   # MT5 Model for optimization passes (1=Control Points, fast)
    backtest_model: int = 0       # MT5 Model for forward/backtest validation (0=Every Tick, realistic)
    spread: int = 45              # Spread override in points applied to all tester runs

    @field_validator("windows_weeks")
    @classmethod
    def windows_weeks_nonempty_and_positive(cls, v: list[int]) -> list[int]:
        if not v:
            raise ValueError("windows_weeks must not be empty")
        if any(w <= 0 for w in v):
            raise ValueError("all windows_weeks values must be positive integers")
        return v

    @field_validator("step_weeks")
    @classmethod
    def step_weeks_at_least_one(cls, v: int) -> int:
        if v < 1:
            raise ValueError("step_weeks must be >= 1")
        return v

    @model_validator(mode="after")
    def study_end_after_start(self) -> "StudyConfig":
        if self.study_end <= self.study_start:
            raise ValueError("study_end must be after study_start")
        return self


class OptimizationJob(BaseModel):
    job_id: str
    window_weeks: int
    train_start: date
    train_end: date
    symbol: str
    timeframe: str
    ea_name: str
    ini_path: str
    output_dir: str
    status: str = "pending"


class ManifestRow(BaseModel):
    job_id: str
    status: str
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None
    window_weeks: int
    train_start: date
    train_end: date
    ini_path: str
    output_dir: str
    notes: Optional[str] = None
