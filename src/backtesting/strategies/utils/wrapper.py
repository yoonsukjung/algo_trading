
import glob
import os
import pandas as pd
from config import result_path_strategy1


start_directory = result_path_strategy1

csv_files = glob.glob(os.path.join(start_directory, "*", "*", "*.csv"))

merged_df = pd.DataFrame()

for csv_file in csv_files:
    print(f"Found CSV file: {csv_file}")

    df = pd.read_csv(csv_file)

    merged_df = pd.concat([merged_df, df])


output_file = os.path.join(start_directory, 'merged_result.csv')
merged_df.to_csv(output_file, index=True)

print(f"병합된 CSV 파일이 {output_file}에 저장되었습니다.")