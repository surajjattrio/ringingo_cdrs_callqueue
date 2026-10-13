#!/bin/bash

# Directory containing the files
DIR="${1:-.}"

# Check if directory exists
if [ ! -d "$DIR" ]; then
    echo "Error: Directory '$DIR' does not exist."
    exit 1
fi

echo "Processing directory: $DIR"

find "$DIR" -maxdepth 1 -type f -name "*.wav" | while read -r file; do
    filename=$(basename "$file")
    dirname=$(dirname "$file")

    # Remove everything up to and including the first underscore
    newname="${filename#*_}"

    # Skip if no underscore exists
    if [ "$filename" = "$newname" ]; then
        echo "Skipping (no underscore): $filename"
        continue
    fi

    # Check if destination already exists
    if [ -e "$dirname/$newname" ]; then
        echo "Skipping (target exists): $newname"
        continue
    fi

    mv "$file" "$dirname/$newname"
    echo "Renamed: $filename -> $newname"
done

echo "Done."
