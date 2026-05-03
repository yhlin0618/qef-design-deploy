# Git-Dropbox Integration Guide for KitchenMAMA

This document provides guidelines and best practices for maintaining Git repositories within Dropbox for the KitchenMAMA precision marketing project.

## Background

Storing Git repositories in Dropbox can cause synchronization issues because:

1. Git creates many small files that change frequently
2. Dropbox syncs files individually, potentially during Git operations
3. Syncing between multiple devices can lead to corrupt Git indexes
4. Symlinks and special files can cause conflicts

## Recommended Setup Process

### Setup for Existing Project

Use this procedure when replacing an existing repository:

```bash
# Step 1: Clone outside Dropbox first
mkdir -p ~/temp_repo
git clone git@github.com:kiki830621/precision_marketing_global_scripts.git ~/temp_repo/global_scripts

# Step 2: Set up Dropbox integration before moving to Dropbox
cd ~/temp_repo/global_scripts
./12_bash/exclude_git_from_dropbox_noninteractive.sh

# Step 3: Backup existing files if needed
mv /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing/precision_marketing_KitchenMAMA/precision_marketing_app/update_scripts/global_scripts /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing/precision_marketing_KitchenMAMA/precision_marketing_app/update_scripts/global_scripts_backup

# Step 4: Move to Dropbox using rsync (preserves symlinks)
rsync -av --delete ~/temp_repo/global_scripts/ /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing/precision_marketing_KitchenMAMA/precision_marketing_app/update_scripts/global_scripts/

# Step 5: Verify and clean up
cd /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing/precision_marketing_KitchenMAMA/precision_marketing_app/update_scripts/global_scripts
git status
rm -rf ~/temp_repo
```

### Setup for New Project

Use this procedure when setting up a completely new project:

```bash
# Step 1: Create project directory structure
mkdir -p /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing_[new_company]/precision_marketing_app/update_scripts/

# Step 2: Clone outside Dropbox first
mkdir -p ~/temp_repo
git clone git@github.com:kiki830621/precision_marketing_global_scripts.git ~/temp_repo/global_scripts

# Step 3: Set up Dropbox integration before moving to Dropbox
cd ~/temp_repo/global_scripts
./12_bash/exclude_git_from_dropbox_noninteractive.sh

# Step 4: Move to Dropbox using rsync (preserves symlinks)
rsync -av ~/temp_repo/global_scripts/ /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing_[new_company]/precision_marketing_app/update_scripts/global_scripts/

# Step 5: Create company-specific configuration 
mkdir -p /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing_[new_company]/precision_marketing_app/local_scripts
cp /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing_[new_company]/precision_marketing_app/update_scripts/global_scripts/03_config/global_parameters.R /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing_[new_company]/precision_marketing_app/local_scripts/brand_specific_parameters.R

# Step 6: Verify and clean up
cd /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing_[new_company]/precision_marketing_app/update_scripts/global_scripts
git status
rm -rf ~/temp_repo
```

### Verification Steps

After setup, verify these conditions:

1. `.git` is a symlink pointing to `.git.nosync`
   ```bash
   ls -la | grep .git
   # Should show: .git -> .git.nosync
   ```

2. `.git.nosync` is a regular directory (not a symlink)
   ```bash
   ls -la .git.nosync | grep .git
   # Should NOT show .git.nosync -> something
   ```

3. `.dropbox.ignore` exists and contains `.git/`
   ```bash
   cat .dropbox.ignore
   # Should contain: .git/
   ```

4. Git commands work correctly
   ```bash
   git status
   # Should run without errors
   ```

## Recovery Procedure

If Git operations fail or GitHub Desktop can't use the repository, use this recovery procedure:

```bash
# Step 1: Backup important files (if any uncommitted changes)
mkdir -p ~/git_recovery_backup
cp -a /path/to/important/uncommitted/files ~/git_recovery_backup/

# Step 2: Remove problematic Git setup
cd /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing/precision_marketing_KitchenMAMA/precision_marketing_app/update_scripts/global_scripts
rm -f .git
rm -rf .git.nosync

# Step 3: Initialize fresh Git repository
git init

# Step 4: Apply Dropbox integration
./12_bash/exclude_git_from_dropbox_noninteractive.sh

# Step 5: Connect to remote and reset
git remote add origin git@github.com:kiki830621/precision_marketing_global_scripts.git
git fetch origin
git reset --hard origin/main
git branch -u origin/main

# Step 6: Verify it works
git status
```

## Common Issues and Solutions

### Issue: GitHub Desktop Cannot Access Repository

**Symptoms:**
- GitHub Desktop shows error loading repository
- Repository shows "not a Git repository" status

**Solution:**
1. Use the recovery procedure above
2. Use command line Git instead of GitHub Desktop when possible
3. Consider creating a repository outside Dropbox for GitHub Desktop use

### Issue: Recursive Symlinks

**Symptoms:**
- `.git.nosync/.git.nosync` exists as a symlink
- Error: "too many levels of symbolic links"

**Solution:**
1. Remove problematic symlinks: `rm -f .git.nosync/.git.nosync`
2. Run recovery procedure if issues persist

### Issue: Corrupted Git Index

**Symptoms:**
- Error: "index file smaller than expected"
- Error: "fatal: bad object" or other index errors

**Solution:**
1. Remove index: `rm -f .git.nosync/index`
2. Recreate index: `git reset`
3. Run recovery procedure if issues persist

## Preventative Measures

1. **Use separate repositories** for each brand (e.g., KitchenMAMA, WISER)
2. **Never directly clone** into Dropbox without using the setup process
3. **Run periodic checks** on Git-Dropbox integration:
   ```bash
   # Add to crontab or run manually
   find /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing -name ".git.nosync" -type d | while read repo; do
     cd "$(dirname "$repo")" && ./12_bash/git_dropbox_check.sh
   done
   ```
4. **Add integration checks** to CI/CD pipeline
5. **Consider a Git LFS setup** for larger repositories

## Technical Details

The Dropbox integration works by:

1. Moving the `.git` directory to `.git.nosync`
2. Creating a symbolic link from `.git` to `.git.nosync`
3. Adding `.git/` to `.dropbox.ignore` to tell Dropbox to ignore Git files
4. Adding Dropbox conflict patterns to `.gitignore`

This approach prevents Dropbox from syncing Git metadata files while keeping Git functionality intact.

---

For additional help, contact the development team or consult the global scripts documentation.

Last Updated: April 1, 2025