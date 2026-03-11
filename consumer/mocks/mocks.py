import os
from pathlib import Path


def get_mocks_path(is_local_path: bool = True) -> str:
    """
    Resolve the path to the mock Bronze data used for this exercise.

    Priority:
    1. Environment variable `MOCK_DATA_PATH` (can be local or Azure-style path).
    2. Local `consumer/mocks/` folder (project-relative).
    """
    if is_local_path:
        project_root = Path(__file__).resolve().parents[2]
        local_mocks = project_root / "consumer" / "mocks" / "bronze_2026_3_2"
        return str(local_mocks)
    else:
        env_path = os.getenv("MOCK_DATA_PATH")
        if env_path:
            return env_path
        else:
            raise "Path not found"
