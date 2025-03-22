
import pandas as pd
import hashlib
import os

def get_object_size(input_object):
    return input_object.memory_usage(deep=True).sum()

def mb_to_bytes(mb):
    return mb * 1024 * 1024

def check_mb_constraint(input_object, mb):
    return mb_to_bytes(mb) <= get_object_size(input_object)

def get_df_hash(df):
    sorted_df = df.sort_values(by=df.columns.tolist())
    memory_df_obj = sorted_df.reset_index(drop=True).to_csv(index=False).encode('utf-8')
    return hashlib.sha256(memory_df_obj).hexdigest()

def split_csv(input_file, output_folder, output_filename, max_file_size_mb=40, chunksize = 1):

    
    reader = pd.read_csv(input_file, chunksize=chunksize)
    
    file_count = 0
    current_chunk = pd.DataFrame()
    
    for chunk in reader:
        current_chunk = pd.concat([current_chunk, chunk])

        # if mb constraint is satisfied, we are going further
        if check_mb_constraint(current_chunk, max_file_size_mb):
            continue

        file_path = os.path.join(output_folder, f"{output_filename}_{file_count}.csv")
        current_chunk.to_csv(file_path, index=False)

        file_count += 1
        current_chunk = pd.DataFrame()

    # Save any remaining data in the last chunk
    if not current_chunk.empty:
        file_path = os.path.join(output_folder, f"{output_filename}_{file_count}.csv")
        current_chunk.to_csv(file_path, index=False)


def read_splitted_csv(input_folder, usecols = None):

    csv_files = [file for file in os.listdir(input_folder) if file.endswith('.csv')]
    dataframes = []
    for file in csv_files:
        file_path = os.path.join(input_folder, file)
        df = pd.read_csv(file_path, usecols=usecols)

        dataframes.append(df)
    
    return pd.concat(dataframes, ignore_index=True)


def get_set_from_splitted_csv(input_folder, column):

    csv_files = [file for file in os.listdir(input_folder) if file.endswith('.csv')]
    output_set = set()
    for file in csv_files:
        file_path = os.path.join(input_folder, file)
        current_set = set(pd.read_csv(file_path, usecols=column)[column])

        output_set.update(current_set)
    
    return output_set


def df_to_splitted_csv(df, input_folder, name_pattern, max_file_size_mb):
    save_path = os.path.join(input_folder, "{name_pattern}.csv")
    df.to_csv(save_path, index = False)
    split_csv(save_path, input_folder, name_pattern, max_file_size_mb, 1000)
    os.remove(save_path)

def append_df_to_splitted_csv(df, input_folder, max_file_size_mb = 40, name_pattern = 'default_pattern'):

    csv_files = [f for f in os.listdir(input_folder) if f.endswith('.csv')]
    if not csv_files:
        df_to_splitted_csv(df, input_folder, name_pattern, max_file_size_mb)

        return 'finished'

    # get file_name pattern 
    latest_file_num = max(int(file.split('_')[-1].split('.csv')[0]) for file in csv_files)
    latest_file_path = os.path.join(input_folder, f"{name_pattern}_{latest_file_num}.csv")

    # append some data to the last csv if possible
    latest_df = pd.read_csv(latest_file_path)
    available_space = mb_to_bytes(max_file_size_mb) - get_object_size(latest_df)
    
    df_rows = df.shape[0]
    finish_index = 0

    if available_space > 0:
        while get_object_size(df[:finish_index]) < available_space and finish_index < df_rows:
            finish_index += 1

        pd.concat([latest_df, df[:finish_index]], ignore_index=True).to_csv(latest_file_path, index=False)
        df = df.iloc[finish_index:]

    if finish_index == df_rows:
        return 'finished'

    df = df.iloc[finish_index:]

    finish_index = 0
    next_file_num = latest_file_num + 1
    while True:
        df_rows = df.shape[0]
        while get_object_size(df[:finish_index]) < mb_to_bytes(max_file_size_mb) and finish_index < df_rows:
            finish_index += 1

        output_file_path = os.path.join(input_folder, f"{name_pattern}_{next_file_num}.csv")
        df[:finish_index].to_csv(output_file_path, index=False)

        if finish_index == df_rows:
            break

        df = df.iloc[finish_index:]
        next_file_num += 1

    return 'finished'
