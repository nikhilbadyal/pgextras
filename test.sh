#!/bin/bash

# Exit on any error
set -e

# Step 1: Ensure you're on the default branch
echo "Switching to the main branch..."
git checkout master

# Step 2: Create a backup branch
echo "Creating a backup branch for safety..."
git branch backup-branch

# Step 3: Create a new commit squashing all the previous ones
echo "Squashing all commits into one..."

# Reset the branch to the root commit (detached HEAD state)
first_commit=$(git rev-list --max-parents=0 HEAD)
git reset --soft "$first_commit"

# Create a new commit with all the changes
git commit -m "Squashed all previous commits into one"

echo "All commits have been squashed into one. A backup branch named 'backup-branch' has been created."
