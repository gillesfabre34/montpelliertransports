from rich import print
from pathlib import Path
import pandas as pd


def merge_parquets(folder_name: str) -> None:
    print("folder_name", folder_name)
    project_root = Path(__file__).resolve().parents[1]
    raw_folder = project_root / 'consumer/mocks/' / folder_name / 'raw'
    print("raw_folder", raw_folder)
    parquet_files = sorted(raw_folder.glob('*.parquet'))
    dfs = [pd.read_parquet(file) for file in parquet_files]
    combined_df = pd.concat(dfs, ignore_index=True)
    output_file_path = project_root / 'consumer/mocks/' / folder_name / f'{folder_name}.parquet'
    print("output_file_path", output_file_path)
    combined_df.to_parquet(output_file_path, index=False)


if __name__ == '__main__':
    merge_parquets('bronze_2026_3_2')