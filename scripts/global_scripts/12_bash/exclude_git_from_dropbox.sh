#!/bin/bash

# Script to mark .git directories as "Ignore" in Dropbox
# This script should be run from the global_scripts directory

# Get the current script directory and navigate to the repository root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &>/dev/null && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." &>/dev/null && pwd)"  # This is the global_scripts directory

# Try to detect the precision_marketing root path
# Move up 3 levels from global_scripts (update_scripts → precision_marketing_app → precision_marketing_COMPANY)
PRECISION_MARKETING_ROOT="$(cd "$REPO_DIR/../../.." &>/dev/null && pwd)"

echo "Repository directory: $REPO_DIR"
echo "Precision marketing root path: $PRECISION_MARKETING_ROOT"

# First handle the current repository
if [ -d "$REPO_DIR/.git" ]; then
  echo "Processing $REPO_DIR/.git"
  
  # If the .git is already a symbolic link, skip it
  if [ -L "$REPO_DIR/.git" ]; then
    echo "  ✓ Already a symbolic link, skipping"
  else
    # Rename .git to .git.nosync and create symbolic link
    echo "  ⊕ Renaming to .git.nosync and creating symbolic link"
    mv "$REPO_DIR/.git" "$REPO_DIR/.git.nosync"
    ln -s ".git.nosync" "$REPO_DIR/.git"
    
    # Create a .dropbox.ignore file in the repository
    echo "  ✎ Creating .dropbox.ignore in repository"
    echo ".git/" > "$REPO_DIR/.dropbox.ignore"
    
    # Add Dropbox conflict patterns to .gitignore if it exists
    if [ -f "$REPO_DIR/.gitignore" ]; then
      echo "  ✎ Adding Dropbox conflict patterns to .gitignore"
      grep -q "Dropbox conflict files" "$REPO_DIR/.gitignore" || cat >> "$REPO_DIR/.gitignore" << EOF

# Dropbox conflict files
*conflicted copy*
*conflict*
*複本*
*(*'s conflicted copy*
EOF
    else
      # Create .gitignore if it doesn't exist
      echo "  ✎ Creating .gitignore with Dropbox conflict patterns"
      cat > "$REPO_DIR/.gitignore" << EOF
# Dropbox conflict files
*conflicted copy*
*conflict*
*複本*
*(*'s conflicted copy*
EOF
    fi
    
    echo "  ✓ Done processing $REPO_DIR/.git"
  fi
else
  echo "⚠️ No .git directory found in $REPO_DIR"
fi

# Ask if user wants to process other repositories in the precision_marketing directory
echo ""
echo "Do you want to process other Git repositories in the precision_marketing directory? (y/n)"
read -p "> " answer

if [[ "$answer" =~ ^[Yy] ]]; then
  # Find all .git directories in the precision_marketing repository
  find "$PRECISION_MARKETING_ROOT" -type d -name ".git" | while read git_dir; do
    # Skip the current repository that we've already processed
    if [ "$git_dir" == "$REPO_DIR/.git" ]; then
      continue
    fi
    
    # Get the parent directory
    parent_dir=$(dirname "$git_dir")
    echo "Processing $git_dir"
    
    # If the .git is a symbolic link, skip it
    if [ -L "$git_dir" ]; then
      echo "  ✓ Already a symbolic link, skipping"
      continue
    fi
    
    # Rename .git to .git.nosync and create symbolic link
    echo "  ⊕ Renaming to .git.nosync and creating symbolic link"
    mv "$git_dir" "${git_dir}.nosync"
    ln -s "${git_dir}.nosync" "$git_dir"
    
    # Create a .dropbox.ignore file in the parent directory
    echo "  ✎ Creating .dropbox.ignore in parent directory"
    echo ".git/" > "$parent_dir/.dropbox.ignore"
    
    # Add Dropbox conflict patterns to .gitignore if it exists
    if [ -f "$parent_dir/.gitignore" ]; then
      echo "  ✎ Adding Dropbox conflict patterns to .gitignore"
      grep -q "Dropbox conflict files" "$parent_dir/.gitignore" || cat >> "$parent_dir/.gitignore" << EOF

# Dropbox conflict files
*conflicted copy*
*conflict*
*複本*
*(*'s conflicted copy*
EOF
    fi
    
    echo "  ✓ Done processing $git_dir"
    echo ""
  done
  
  echo "✅ Finished processing all .git directories!"
else
  echo "✅ Finished processing only the current repository."
fi

echo "📝 Note: It may take a few minutes for Dropbox to recognize these changes."
echo "🔍 Check your Dropbox status to see if it's syncing or if sync is paused."

# Verify git still works for the current repository
echo "Verifying Git functionality..."
cd "$REPO_DIR"
if git status &>/dev/null; then
  echo "✅ Git is working correctly!"
else
  echo "❌ Git status failed. You may need to fix the symbolic link."
fi