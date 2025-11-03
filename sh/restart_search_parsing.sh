#!/bin/bash
cd /home/kardinal/projects/cian


# Step 0: Activate the virtual environment if not already active
if [ -z "$VIRTUAL_ENV" ]; then
    if [ -d "venv" ]; then
        echo "Activating virtual environment..."
        source venv/bin/activate
    else
        echo "Virtual environment not found. Exiting restart script."
        exit 1
    fi
else
    echo "Already in a virtual environment."
fi

echo "venv active"
# Step 1: Check if the MongoDB Docker container is running; if not, start it
if [ "$(docker inspect -f '{{.State.Running}}' mongo)" != "true" ]; then
    echo "Starting MongoDB Docker container..."
    # Using sudo without password prompt due to sudoers config
    sudo docker start mongo
else
    echo "MongoDB Docker container is already running."
fi

echo "MongoDB container started"


# Step 2: Check if the Python script is already running
echo "checking if offer parsing is running"
if pgrep -f "python3 -m py.run_search_parsing" > /dev/null; then
    echo "The Python script is already running. Exiting restart script."
    echo "---------------------------------------------"
    exit 0
fi

# Step 3: Run the Python script
echo "Running the Python script..."
echo "---------------------------------------------"
python3 -m py.run_search_parsing
