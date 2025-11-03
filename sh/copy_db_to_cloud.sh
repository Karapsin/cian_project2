#!/bin/bash
echo "Removing old backup directory... (if exists)"
rm -rf /home/kardinal/projects/cian/database_backup

echo "Creating a new backup directory..."
mkdir -p /home/kardinal/projects/cian/database_backup

echo "Copying database from Docker container..."
sudo /home/kardinal/projects/cian/sh/utils/copy_cian_database.sh
sleep 300

echo "Starting transfer..."
cd /home/kardinal/projects/cian
if [ -z "$VIRTUAL_ENV" ]; then
    if [ -d "venv" ]; then
        echo "Activating virtual environment..."
        . venv/bin/activate
    else
        echo "Virtual environment not found. Exiting restart script."
        exit 1
    fi
else
    echo "Already in a virtual environment."
fi

echo "Running Python script to transfer the database to Yandex Disk..."
python3 -m py.transfer_db_to_yadisk
echo "Transfer completed."
echo "Done :)"