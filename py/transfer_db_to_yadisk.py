from py.utils.utils import get_current_date
from py.utils.yadisk_utils import( 
    force_create_empty_cloud_dir, 
    transfer_missing_directories_and_files
)


cloud_path = f"database/db_backup_{get_current_date().replace('-', '_')}"
local_path = "/home/kardinal/projects/cian/database_backup"


print("Removing old copy")
force_create_empty_cloud_dir(cloud_path)

print("starting transfer")
transfer_missing_directories_and_files(local_path, cloud_path)
