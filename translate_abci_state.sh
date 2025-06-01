#!/bin/bash

CURRENT_DATE=$(date +"%Y%m%d")

SOURCE_DIR="$HOME/hl/data/periodic_abci_states"
TARGET_FILE="$HOME/hl-scripts/builder-codes-parser/data.json"

LATEST_FILE=$(ls -t "$SOURCE_DIR/$CURRENT_DATE"/*.rmp 2>/dev/null | head -n1)

if [ -z "$LATEST_FILE" ]; then
    echo "No .rmp files found in $SOURCE_DIR/$CURRENT_DATE"
    exit 1
fi

"$HOME/hl-node" --chain Mainnet translate-abci-state "$LATEST_FILE" "$TARGET_FILE"

if [ $? -eq 0 ]; then
    echo "Successfully translated ABCI state to $TARGET_FILE"
else
    echo "Translation failed"
    exit 1
fi
