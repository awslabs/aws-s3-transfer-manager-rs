#!/bin/bash

# Helper script to generate a changelog markdown file for a new release
# Usage: ./new_release.sh <new-tag-version> <release-notes>
# Example: ./new_release.sh v1.2.0 "Fixed critical bugs and improved performance"
# This script will generate a markdown file in the .changelog directory
# with the release notes and changes since the last tag.
# It will also check if the new tag already exists in the repository
# and display the commands to create a new tag and push to GitHub

# Get the directory where the script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"

# Change to the script directory
cd "$SCRIPT_DIR" || {
    echo "Error: Could not change to script directory: $SCRIPT_DIR"
    exit 1
}

# Check arguments
if [ $# -lt 2 ]; then
    echo "Usage: $0 <new-tag-version> <release-notes>"
    echo "Example: $0 v1.2.0 \"Fixed critical bugs and improved performance\""
    exit 1
fi

NEW_TAG=$1
RELEASE_NOTES=$2

# Check if we're in a git repository
if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    echo "Error: Not in a git repository"
    exit 1
fi

# Check if the new tag already exists
if git show-ref --tags | grep -q "refs/tags/$NEW_TAG"; then
    echo "Error: Tag '$NEW_TAG' already exists. Choose a different tag name."
    exit 1
fi

# Extract version without the 'v' prefix for Cargo.toml
CARGO_VERSION="${NEW_TAG#v}"
# Path to Cargo.toml
CARGO_TOML_PATH="$SCRIPT_DIR/aws-s3-transfer-manager/Cargo.toml"

# Update version in Cargo.toml
echo "Updating version in Cargo.toml to $CARGO_VERSION..."
sed -i.bak "s/^version = \"[0-9]*\.[0-9]*\.[0-9]*\"/version = \"$CARGO_VERSION\"/" "$CARGO_TOML_PATH"
if [ $? -ne 0 ]; then
    echo "Error: Failed to update version in Cargo.toml"
    exit 1
fi
# Remove the backup file created by sed
rm -f "$CARGO_TOML_PATH.bak"

# Fetch the latest tags
echo "Fetching latest tags..."
git fetch --tags --quiet

# Get the most recent tag
LAST_TAG=$(git describe --tags --abbrev=0 2>/dev/null)

if [ -z "$LAST_TAG" ]; then
    COMMIT_RANGE="HEAD"
else
    COMMIT_RANGE="$LAST_TAG..HEAD"
fi

# Get commits since the last tag
COMMIT_LOGS=$(git log $COMMIT_RANGE --pretty=format:"- %h: %s (%an)")
COMMIT_COUNT=$(git log $COMMIT_RANGE --pretty=format:"%h" | wc -l | xargs)

# Generate release date
RELEASE_DATE=$(date "+%Y-%m-%d")

# Create markdown filename
MD_FILENAME=".changelog/release_${NEW_TAG}.md"

# Generate markdown content
cat >"$MD_FILENAME" <<EOF
# Release $NEW_TAG

**Release Date:** $RELEASE_DATE

## Release Notes
$RELEASE_NOTES

## Changes since $LAST_TAG
$([[ -z "$LAST_TAG" ]] && echo "Initial release" || echo "$COMMIT_LOGS")

EOF

echo "==============================================="
echo "Release markdown generated: $MD_FILENAME"
echo "New tag version: $NEW_TAG"
echo "Previous tag: $([[ -z "$LAST_TAG" ]] && echo "None (initial release)" || echo "$LAST_TAG")"
echo "Commit count: $COMMIT_COUNT"
echo "==============================================="
echo "To create the tag and push to GitHub, run:"
echo "git tag $NEW_TAG"
echo "git push origin $NEW_TAG"
echo "==============================================="
