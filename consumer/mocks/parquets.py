from rich import print
from pathlib import Path
import pandas as pd


def merge_parquets(folder_path: str) -> None:
    print("folder_path", folder_path)
    project_root = Path(__file__).resolve().parents[2]
    parquets_folder = project_root / folder_path
    parquet_files = sorted(parquets_folder.glob('*.parquet'))
    dfs = [pd.read_parquet(file) for file in parquet_files]
    combined_df = pd.concat(dfs, ignore_index=True)
    output_file_name = folder_path.split('/')[-1] + '.parquet'
    output_file_path = project_root / 'consumer/mocks/' / output_file_name
    print("output_file_path", output_file_path)
    combined_df.to_parquet(output_file_path, index=False)


if __name__ == '__main__':
    merge_parquets('consumer/mocks/bronze_2026_3_2')