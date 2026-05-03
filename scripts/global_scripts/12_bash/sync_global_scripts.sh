#!/bin/bash
# sync_global_scripts.sh
#
# This script synchronizes the global_scripts directories across all company projects.
# It ensures that all instances of global_scripts have the latest code from the main branch.
#
# Author: Claude
# Date: April 1, 2025

# Base path for all precision marketing projects
BASE_PATH="$HOME/Library/CloudStorage/Dropbox/precision_marketing"

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
PARAMS_FILE="$SCRIPT_DIR/../00_principles/parameters.yaml"

# Initialize companies array with defaults
COMPANIES=("KitchenMAMA" "WISER" "MAMBA")

# Check if parameters.yaml exists
if [ -f "$PARAMS_FILE" ]; then
  echo "Loading companies from parameters.yaml..."
  
  # Reset companies array
  COMPANIES=()
  
  # First try to find companies in proper YAML list format
  if grep -q "^companies:" "$PARAMS_FILE"; then
    # Extract companies from YAML list format (stop at first non-indented line)
    while read -r line; do
      # Detect the end of the companies section
      if [[ ! "$line" =~ ^[[:space:]] && ! "$line" =~ ^- && ! "$line" =~ ^$ ]]; then
        break
      fi
      
      # Extract company name by removing leading whitespace, dash, and additional whitespace
      if [[ "$line" =~ ^[[:space:]]*-[[:space:]]* ]]; then
        company=$(echo "$line" | sed -E 's/^[[:space:]]*-[[:space:]]*(.*)[[:space:]]*$/\1/')
        
        # Add to array if not empty and not a comment
        if [ -n "$company" ] && [[ ! "$company" =~ ^# ]]; then
          COMPANIES+=("$company")
        fi
      fi
    done < <(grep -A20 "^companies:" "$PARAMS_FILE" | tail -n +2)
  fi
  
  # If no companies found in YAML list, try the COMPANY_LIST format
  if [ ${#COMPANIES[@]} -eq 0 ]; then
    while read -r company; do
      if [ -n "$company" ]; then
        COMPANIES+=("$company")
      fi
    done < <(grep "^COMPANY_LIST:" "$PARAMS_FILE" | cut -d ":" -f2 | sed 's/^ *//')
  fi
  
  # If still no companies found, revert to defaults
  if [ ${#COMPANIES[@]} -eq 0 ]; then
    COMPANIES=("KitchenMAMA" "WISER" "MAMBA")
    echo "No companies found in parameters file, using defaults: ${COMPANIES[*]}"
  else
    echo "Loaded companies from parameters.yaml: ${COMPANIES[*]}"
  fi
else
  echo "Parameters file not found, using default companies: ${COMPANIES[*]}"
fi

# Save the current directory to return to it later
ORIGINAL_DIR=$(pwd)

echo "==============================================="
echo "Global Scripts Synchronization Tool"
echo "==============================================="
echo "This script ensures all company projects have the same version of global_scripts."
echo

# First check if any directories have uncommitted changes
echo "Checking for uncommitted changes..."
UNCOMMITTED_FOUND=false

for company in "${COMPANIES[@]}"; do
  GLOBAL_SCRIPTS_PATH="$BASE_PATH/precision_marketing_$company/precision_marketing_app/update_scripts/global_scripts"
  
  if [ -d "$GLOBAL_SCRIPTS_PATH" ]; then
    cd "$GLOBAL_SCRIPTS_PATH"
    
    if [[ -n $(git status --porcelain) ]]; then
      echo "⚠️ Uncommitted changes found in $company project"
      git status --short
      UNCOMMITTED_FOUND=true
      echo
    fi
  else
    echo "⚠️ Directory not found: $GLOBAL_SCRIPTS_PATH"
  fi
done

if [ "$UNCOMMITTED_FOUND" = true ]; then
  read -p "Uncommitted changes were found. Continue anyway? (y/n): " CONTINUE
  if [[ $CONTINUE != "y" && $CONTINUE != "Y" ]]; then
    echo "Synchronization cancelled. Please commit or stash your changes."
    cd "$ORIGINAL_DIR"
    exit 1
  fi
  echo
fi

# Ask if user wants to commit local changes
if [ "$UNCOMMITTED_FOUND" = true ]; then
  read -p "Would you like to commit your local changes before synchronizing? (y/n): " COMMIT_CHANGES
  if [[ $COMMIT_CHANGES == "y" || $COMMIT_CHANGES == "Y" ]]; then
    echo
    echo "Committing local changes..."
    
    for company in "${COMPANIES[@]}"; do
      GLOBAL_SCRIPTS_PATH="$BASE_PATH/precision_marketing_$company/precision_marketing_app/update_scripts/global_scripts"
      
      if [ -d "$GLOBAL_SCRIPTS_PATH" ]; then
        cd "$GLOBAL_SCRIPTS_PATH"
        
        if [[ -n $(git status --porcelain) ]]; then
          echo "Committing changes in $company project..."
          git add .
          git commit -m "Auto commit by sync_global_scripts.sh before synchronization"
          echo "✅ Changes committed in $company project"
          echo
        fi
      fi
    done
  fi
fi

# Pull latest changes in all company projects
echo "Synchronizing global_scripts directories..."
echo

for company in "${COMPANIES[@]}"; do
  GLOBAL_SCRIPTS_PATH="$BASE_PATH/precision_marketing_$company/precision_marketing_app/update_scripts/global_scripts"
  
  if [ -d "$GLOBAL_SCRIPTS_PATH" ]; then
    echo "Syncing $company project..."
    cd "$GLOBAL_SCRIPTS_PATH"
    
    # Check if this is a git repository
    if [ -d "$(git rev-parse --git-dir 2>/dev/null)" ]; then
      # Fetch the latest changes
      git fetch origin
      
      # Get current branch
      CURRENT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
      
      # Check if we're behind the remote
      if git rev-list --count HEAD..origin/$CURRENT_BRANCH > /dev/null 2>&1; then
        LOCAL_BEHIND=$(git rev-list --count HEAD..origin/$CURRENT_BRANCH)
      else
        LOCAL_BEHIND=0
      fi
      
      if [ "$LOCAL_BEHIND" -eq 0 ]; then
        echo "✅ $company is already up to date"
      else
        echo "⏳ Updating $company ($LOCAL_BEHIND commits behind)..."
        
        # Stash any local changes
        if [[ -n $(git status --porcelain) ]]; then
          echo "  Stashing local changes..."
          git stash save "Automatically stashed by sync_global_scripts.sh"
          STASHED=true
        else
          STASHED=false
        fi
        
        # Pull the latest changes
        echo "  Pulling latest changes..."
        PULL_RESULT=$(git pull origin $CURRENT_BRANCH 2>&1)
        PULL_STATUS=$?
        
        # Apply stashed changes if needed
        if [ "$STASHED" = true ]; then
          echo "  Restoring local changes..."
          git stash pop
          STASH_STATUS=$?
          if [ $STASH_STATUS -ne 0 ]; then
            echo "  ⚠️ There were conflicts when restoring your local changes."
            echo "  Please resolve them manually. Your changes are in the stash."
          fi
        fi
        
        if [ $PULL_STATUS -eq 0 ]; then
          echo "✅ $company updated successfully"
        else
          echo "❌ Failed to update $company: $PULL_RESULT"
        fi
      fi
    else
      echo "❌ Not a git repository: $GLOBAL_SCRIPTS_PATH"
    fi
    echo
  else
    echo "⚠️ Directory not found: $GLOBAL_SCRIPTS_PATH"
    echo
  fi
done

# Check if any directories were missing and offer to create them
MISSING_FOUND=false
MISSING_COMPANIES=()

for company in "${COMPANIES[@]}"; do
  GLOBAL_SCRIPTS_PATH="$BASE_PATH/precision_marketing_$company/precision_marketing_app/update_scripts/global_scripts"
  
  if [ ! -d "$GLOBAL_SCRIPTS_PATH" ]; then
    MISSING_FOUND=true
    MISSING_COMPANIES+=("$company")
  fi
done

if [ "$MISSING_FOUND" = true ]; then
  echo "==============================================="
  echo "Missing Repositories"
  echo "==============================================="
  echo "The following companies do not have global_scripts directories:"
  for company in "${MISSING_COMPANIES[@]}"; do
    echo "  - $company"
  done
  echo
  
  read -p "Would you like to create missing directories and clone from an existing repo? (y/n): " CREATE_MISSING
  if [[ $CREATE_MISSING == "y" || $CREATE_MISSING == "Y" ]]; then
    # Find a valid source repository
    SOURCE_COMPANY=""
    for company in "${COMPANIES[@]}"; do
      GLOBAL_SCRIPTS_PATH="$BASE_PATH/precision_marketing_$company/precision_marketing_app/update_scripts/global_scripts"
      
      if [ -d "$GLOBAL_SCRIPTS_PATH" ]; then
        SOURCE_COMPANY="$company"
        break
      fi
    done
    
    if [ -n "$SOURCE_COMPANY" ]; then
      SOURCE_PATH="$BASE_PATH/precision_marketing_$SOURCE_COMPANY/precision_marketing_app/update_scripts/global_scripts"
      
      echo "Using $SOURCE_COMPANY as the source for missing repositories..."
      
      for company in "${MISSING_COMPANIES[@]}"; do
        TARGET_DIR="$BASE_PATH/precision_marketing_$company/precision_marketing_app/update_scripts"
        GLOBAL_SCRIPTS_PATH="$TARGET_DIR/global_scripts"
        
        echo "Creating global_scripts for $company..."
        
        # Create parent directories if needed
        if [ ! -d "$TARGET_DIR" ]; then
          mkdir -p "$TARGET_DIR"
        fi
        
        # Clone the repository
        cd "$TARGET_DIR"
        if cp -R "$SOURCE_PATH" "$GLOBAL_SCRIPTS_PATH"; then
          echo "✅ Created global_scripts directory for $company"
          
          # Initialize git repository if the source was a git repository
          if [ -d "$SOURCE_PATH/.git" ]; then
            cd "$GLOBAL_SCRIPTS_PATH"
            git init
            git add .
            git commit -m "Initial commit for global_scripts"
            echo "✅ Initialized git repository for $company"
          fi
        else
          echo "❌ Failed to create global_scripts directory for $company"
        fi
        echo
      done
    else
      echo "❌ No valid source repository found. Cannot create missing directories."
    fi
  fi
fi

echo "==============================================="
echo "Synchronization Completed"
echo "==============================================="

# Return to the original directory
cd "$ORIGINAL_DIR"