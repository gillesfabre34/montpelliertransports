import os
from rich import print
from azure.storage.blob import BlobServiceClient
from dotenv import load_dotenv
from pathlib import Path

_project_root = Path(__file__).resolve().parent.parent
load_dotenv(_project_root / ".env")

def get_mocks():
    AZURE_ACCOUNT_URL = os.getenv("AZURE_ACCOUNT_URL")
    print(f"AZURE_ACCOUNT_URL", AZURE_ACCOUNT_URL)
    # service = BlobServiceClient()
    return {}

if __name__ == "__main__":
    get_mocks()
    print(f"get_mocks done")