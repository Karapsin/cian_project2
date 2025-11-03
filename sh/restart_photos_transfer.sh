#!/bin/bash
cd /home/kardinal/projects/cian

# Step 0: Activate the virtual environment if not already active
if [ -z "$VIRTUAL_ENV" ]; then
    if [ -d "venv" ]; then
        echo "Activating virtual environment..."
        source .venv/bin/activate
    else
        echo "Virtual environment not found. Exiting restart script."
        exit 1
    fi
else
    echo "Already in a virtual environment."
fi

# Step 1: Check if the Python script is already running
echo "checking if offer parsing is running"
if pgrep -f "python3 -m py.transfer_photos_to_yadisk" > /dev/null; then
    echo "The Python script is already running. Exiting restart script."
    echo "---------------------------------------------"
    exit 0
fi

# Step 2: Run the Python script
echo "Running the Python script..."
echo "---------------------------------------------"
python3 -m py.transfer_photos_to_yadisk
