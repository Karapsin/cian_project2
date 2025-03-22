
import pandas as pd
import hashlib
import os

def get_df_hash(df):
    sorted_df = df.sort_values(by=df.columns.tolist())
    memory_df_obj = sorted_df.reset_index(drop=True).to_csv(index=False).encode('utf-8')
    return hashlib.sha256(memory_df_obj).hexdigest()

def split_csv(input_file, output_folder, output_filename, max_file_size_mb=40, chunksize = 1):
    
    
    max_file_size_bytes = max_file_size_mb * 1024 * 1024
    
    reader = pd.read_csv(input_file, chunksize=chunksize)
    
    file_count = 0
    current_chunk = pd.DataFrame()
    
    for chunk in reader:
        current_chunk = pd.concat([current_chunk, chunk])

        current_chunk_size_bytes = current_chunk.memory_usage(deep=True).sum()
        if current_chunk_size_bytes >= max_file_size_bytes:
            file_path = os.path.join(output_folder, f"{output_filename}_{file_count}.csv")
            current_chunk.to_csv(file_path, index=False)
            file_count += 1
            current_chunk = pd.DataFrame()

    # Save any remaining data in the last chunk
    if not current_chunk.empty:
        file_path = os.path.join(output_folder, f"{output_filename}_{file_count}.csv")
        current_chunk.to_csv(file_path, index=False)


def read_splitted_csv(input_folder, usecols = None):

    csv_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]
    dataframes = []
    for file in csv_files:
        file_path = os.path.join(input_folder, file)
        df = pd.read_csv(file_path, usecols=usecols)
        dataframes.append(df)
    
    return pd.concat(dataframes, ignore_index=True)
