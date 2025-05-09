from py.utils.utils import get_current_date
from py.utils.yadisk_utils import( 
    force_create_empty_cloud_dir, 
    upload_to_yadisk
)


cloud_path = f"database/db_backup_{get_current_date().replace('-', '_')}"
local_path = "/home/danila/cian_project/database_backup"


print("Removing old copy")
force_create_empty_cloud_dir(cloud_path)

print("starting transfer")
upload_to_yadisk(local_path, cloud_path)
