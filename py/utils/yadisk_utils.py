# thanks chat gpt for sponsoring that script 
import os
import shutil
import yadisk
import random

ya_client = yadisk.YaDisk(token = os.environ['YANDEX_DISK_TOKEN'])

def upload_single_file(root, file, local_path, remote_path):
    local_file_path = os.path.join(root, file)
    relative_path = os.path.relpath(local_file_path, local_path)
    remote_file_path = os.path.join(remote_path, relative_path)

    # Define the directory path where the file will be uploaded
    remote_dir_path = os.path.dirname(remote_file_path)

    # Check if remote directory exists, create if it doesn't
    if not ya_client.exists(remote_dir_path):
        ya_client.mkdir(remote_dir_path)

    # Upload file
    if not ya_client.exists(remote_file_path):
        ya_client.upload(local_file_path, remote_file_path)

def upload_to_yadisk(local_path, remote_path):

    if os.path.isdir(local_path):
        for root, dirs, files in os.walk(local_path):
            shuffled_files = files
            random.shuffle(shuffled_files)
            for file in shuffled_files:
                upload_single_file(root, file, local_path, remote_path)

    else:
        # Handle single-file upload
        if not ya_client.exists(remote_path):
            # If it's a single file, ensure parent directory exists
            remote_dir_path = os.path.dirname(remote_path)
            if not ya_client.exists(remote_dir_path):
                ya_client.mkdir(remote_dir_path)

            if not ya_client.exists(remote_path):
                ya_client.upload(local_path, remote_path)

def transfer_single_missing_file(file, root, local_dir_path, remote_dir_path):
    local_file_path = os.path.join(root, file)
    relative_path = os.path.relpath(local_file_path, local_dir_path)
    remote_file_path = os.path.join(remote_dir_path, relative_path)
    
    # Check if the file exists in the remote path
    if not ya_client.exists(remote_file_path):
        remote_file_dir = os.path.dirname(remote_file_path)
        # Ensure the directory exists on the remote
        if not ya_client.exists(remote_file_dir):
            ya_client.mkdir(remote_file_dir)
            
        # Upload the missing file
        print(f'Uploading missing file {local_file_path} to {remote_file_path}')
        ya_client.upload(local_file_path, remote_file_path)


def transfer_missing_files(local_dir_path, remote_dir_path):

    # Walk through the local directory structure
    for root, dirs, files in os.walk(local_dir_path):
        shuffled_files = files
        random.shuffle(shuffled_files)
        for file in shuffled_files:
           transfer_single_missing_file(file, root, local_dir_path, remote_dir_path)



def transfer_single_dir(item, local_base_dir, remote_base_dir):
   
    local_dir_path = os.path.join(local_base_dir, item)

    # Check if item is a directory
    if os.path.isdir(local_dir_path):
        remote_dir_path = os.path.join(remote_base_dir, item)
        
        # Check if corresponding remote directory exists
        if not ya_client.exists(remote_dir_path):
            print(f'Transferring {local_dir_path} to {remote_dir_path}')
            # Use the existing upload function to transfer the directory
            upload_to_yadisk(local_dir_path, remote_dir_path)
        else:
            print(f'The directory {remote_dir_path} already exists on the remote side. Checking contents...')
            # Check and transfer missing files
            transfer_missing_files(local_dir_path, remote_dir_path)
        
        print(f"removing dir {local_dir_path}")
        shutil.rmtree(local_dir_path)


def transfer_missing_directories_and_files(local_base_dir, remote_base_dir, dirs_num = 25):
    
    dirs_list = os.listdir(local_base_dir)
    if len(dirs_list) == 0:
        print('nothing to transfer')
        return

    if len(dirs_list) > dirs_num:
        sampled_items = random.sample(dirs_list, dirs_num)
    else:
        sampled_items = dirs_list

    # Transfer each of the sampled items using list comprehension
    [transfer_single_dir(item, local_base_dir, remote_base_dir) for item in sampled_items]


def copy_cloud_directory(cloud_path, destination_path):

    # List all items in the source directory
    items = ya_client.listdir(cloud_path)
    for item in items:
        item_source_path = f"{cloud_path}/{item['name']}"
        item_destination_path = f"{destination_path}/{item['name']}"

        # If the item is a directory, make a recursive call
        if item['type'] == 'dir':
            if not ya_client.exists(item_destination_path):
                ya_client.mkdir(item_destination_path)

            copy_cloud_directory(item_source_path, item_destination_path)
        else:
            # Copy file
            ya_client.copy(item_source_path, item_destination_path, overwrite=True)
    
    print(f"Copy of directory {cloud_path} to {destination_path} completed successfully.")
        

def force_create_empty_cloud_dir(cloud_path):
    if ya_client.exists(cloud_path):
        ya_client.remove(cloud_path, permanently=True)
        
    ya_client.mkdir(cloud_path)
