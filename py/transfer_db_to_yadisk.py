from py.utils.yadisk_utils import( 
    copy_cloud_directory,
    force_create_empty_cloud_dir, 
    upload_to_yadisk
)

cloud_path = "database/db_backup"
local_path = "/home/danila/cian_project/database_backup"



print("Saving previous backup")
force_create_empty_cloud_dir("database/old_db_backup")
copy_cloud_directory(cloud_path, "database/old_db_backup")

print("Removing old copy")
force_create_empty_cloud_dir(cloud_path)

print("starting transfer")
upload_to_yadisk(local_path, cloud_path)
