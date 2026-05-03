#!/bin/bash
#
# Script to verify Git-Dropbox integration is set up correctly
# Created by Claude for the precision marketing project

set -eo pipefail

# Get the current directory (should be the repository root)
REPO_DIR=$(pwd)
echo "Checking Git-Dropbox integration in: $REPO_DIR"
echo "---------------------------------------------"

# Check 1: Verify .git is a symlink to .git.nosync
echo "✓ Checking .git symlink..."
if [ -L "$REPO_DIR/.git" ]; then
  GIT_LINK=$(readlink "$REPO_DIR/.git")
  if [ "$GIT_LINK" = ".git.nosync" ]; then
    echo "  ✅ PASS: .git is correctly symlinked to .git.nosync"
  else
    echo "  ❌ FAIL: .git is a symlink but points to '$GIT_LINK' instead of '.git.nosync'"
  fi
else
  echo "  ❌ FAIL: .git is not a symlink. This may cause Dropbox sync conflicts."
fi

# Check 2: Verify .git.nosync exists and is a directory (not a symlink)
echo "✓ Checking .git.nosync structure..."
if [ -d "$REPO_DIR/.git.nosync" ] && [ ! -L "$REPO_DIR/.git.nosync" ]; then
  echo "  ✅ PASS: .git.nosync is a proper directory"
else
  if [ ! -d "$REPO_DIR/.git.nosync" ]; then
    echo "  ❌ FAIL: .git.nosync directory is missing"
  else
    echo "  ❌ FAIL: .git.nosync is a symlink, which may cause recursive reference issues"
  fi
fi

# Check 3: Check for recursive symlinks within .git.nosync
echo "✓ Checking for recursive symlinks..."
if [ -d "$REPO_DIR/.git.nosync" ]; then
  RECURSIVE_LINKS=$(find "$REPO_DIR/.git.nosync" -type l -name ".git*" | wc -l)
  if [ "$RECURSIVE_LINKS" -eq 0 ]; then
    echo "  ✅ PASS: No recursive symlinks found in .git.nosync"
  else
    echo "  ❌ FAIL: Found $RECURSIVE_LINKS recursive symlinks in .git.nosync"
    find "$REPO_DIR/.git.nosync" -type l -name ".git*" | while read link; do
      echo "      - $link -> $(readlink "$link")"
    done
  fi
fi

# Check 4: Verify .dropbox.ignore exists and contains .git/
echo "✓ Checking .dropbox.ignore configuration..."
if [ -f "$REPO_DIR/.dropbox.ignore" ]; then
  if grep -q ".git/" "$REPO_DIR/.dropbox.ignore"; then
    echo "  ✅ PASS: .dropbox.ignore exists and contains .git/"
  else
    echo "  ❌ FAIL: .dropbox.ignore exists but does not contain .git/"
  fi
else
  echo "  ❌ FAIL: .dropbox.ignore file is missing"
fi

# Check 5: Verify .gitignore includes Dropbox conflict patterns
echo "✓ Checking .gitignore for Dropbox conflict patterns..."
if [ -f "$REPO_DIR/.gitignore" ]; then
  if grep -q "Dropbox conflict files" "$REPO_DIR/.gitignore"; then
    echo "  ✅ PASS: .gitignore includes Dropbox conflict patterns"
  else
    echo "  ⚠️ WARNING: .gitignore may not have Dropbox conflict patterns"
  fi
else
  echo "  ⚠️ WARNING: .gitignore file is missing"
fi

# Check 6: Verify Git is working
echo "✓ Verifying Git functionality..."
if git status &>/dev/null; then
  echo "  ✅ PASS: Git commands are working properly"
else
  echo "  ❌ FAIL: Git commands are not working"
fi

# Check 7: Verify the Git repository has a remote
echo "✓ Checking remote configuration..."
if git remote -v 2>/dev/null | grep -q origin; then
  REMOTE_URL=$(git remote get-url origin 2>/dev/null)
  echo "  ✅ PASS: Remote 'origin' is configured to $REMOTE_URL"
else
  echo "  ⚠️ WARNING: No 'origin' remote configured"
fi

# Summary
echo "---------------------------------------------"
echo "Git-Dropbox integration check complete."
FAILURES=$(grep -c "FAIL" <<< "$(grep "❌" <<< "$0" 2>/dev/null || echo "")")
WARNINGS=$(grep -c "WARNING" <<< "$(grep "⚠️" <<< "$0" 2>/dev/null || echo "")")
PASSES=$(grep -c "PASS" <<< "$(grep "✅" <<< "$0" 2>/dev/null || echo "")")

if [ "$FAILURES" -gt 0 ]; then
  echo "⚠️ Found $FAILURES failures that need to be addressed."
  echo "   Use the recovery procedure in git_dropbox_integration_guide.md to fix these issues."
elif [ "$WARNINGS" -gt 0 ]; then
  echo "⚠️ Found $WARNINGS warnings that should be reviewed."
  echo "   Consider running exclude_git_from_dropbox_noninteractive.sh to update your setup."
else
  echo "✅ All checks passed! Your Git-Dropbox integration is correctly configured."
fi

echo "Done."