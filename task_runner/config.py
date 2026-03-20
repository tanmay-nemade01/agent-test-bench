"""Configuration and environment helpers for Task Runner."""

import os
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
TASKS_DIR = BASE_DIR / "tasks"
AGENT_OUTPUT_DIR = BASE_DIR / ".agent_generated"


def load_env_file(env_path: Path):
    """Load simple KEY=VALUE pairs from .env into process env if missing."""
    if not env_path.exists():
        return
    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


load_env_file(BASE_DIR / ".env")

TASK_FOLDERS = sorted(
    [
        d.name
        for d in TASKS_DIR.iterdir()
        if d.is_dir()
        and (d / "environment" / "Dockerfile").exists()
        and (d / "solution" / "solve.sh").exists()
        and (d / "tests" / "test.sh").exists()
        and (d / "tests" / "test_outputs.py").exists()
    ]
) if TASKS_DIR.exists() else []
