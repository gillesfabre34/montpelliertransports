import os
from pathlib import Path
from dotenv import load_dotenv


def get_mocks_path(is_local_path: bool = True) -> str:
    """
    Resolve the path to the mock Bronze data used for this exercise.

    Priority:
    1. Environment variable `MOCK_DATA_PATH` (can be local or Azure-style path).
    2. Local `consumer/mocks/` folder (project-relative).
    """
    if is_local_path:
        project_root = Path(__file__).resolve().parents[2]
        load_dotenv(project_root / ".env")
        mocks_folder_name = os.getenv("LOCAL_MOCK_DATA_FOLDER_NAME")
        local_mocks = project_root / "consumer" / "mocks" / mocks_folder_name / "raw"
        return str(local_mocks)
    else:
        env_path = os.getenv("MOCK_DATA_PATH")
        if env_path:
            return env_path
        else:
            raise "Path not found"


if __name__ == "__main__":
    print("\npath = ", get_mocks_path())
