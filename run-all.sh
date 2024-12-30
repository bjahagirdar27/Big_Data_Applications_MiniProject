#!/bin/bash

scripts=(
    "data_overview.py"
    "handle_missing_values.py"
    "descriptive_statistics.py"
    "feature_engineering.py"
    "aggregation.py"
)

echo "Automation started at $(date)"

for script in "${scripts[@]}"
do
    echo "Running $script..."
    spark-submit $script
    if [ $? -ne 0 ]; then
        echo "Error occurred while running $script. Exiting..."
        exit 1
    fi
    echo "$script completed successfully."
done

echo "All scripts executed successfully at $(date)"
