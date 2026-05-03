# Precision Marketing Git Workflow Guide

## Overview

This document outlines the recommended Git workflow for the Precision Marketing projects. It provides guidance on how to effectively manage the Git repository, coordinate team efforts, and maintain code quality.

## Shared global_scripts Directory

The `global_scripts` directory is a shared resource used by multiple company projects (KitchenMAMA, WISER, etc.). Changes made to this directory in one project need to be propagated to all other projects.

## Table of Contents

1. [Managing Shared global_scripts](#managing-shared-global_scripts)
2. [Daily Workflow](#daily-workflow)
3. [Branch Strategy](#branch-strategy)
4. [Commit Guidelines](#commit-guidelines)
5. [Pull Requests](#pull-requests)
6. [Code Reviews](#code-reviews)
7. [Versioning](#versioning)
8. [Handling R-Specific Code](#handling-r-specific-code)
9. [Git Cheat Sheet](#git-cheat-sheet)

## Managing Shared global_scripts

The global_scripts directory in each company project (KitchenMAMA, WISER, etc.) is linked to the same remote Git repository. This setup enables code sharing across all projects while allowing separate development environments.

### Repository Structure

- Each company has its own project folder:
  - `/precision_marketing/precision_marketing_KitchenMAMA/`
  - `/precision_marketing/precision_marketing_WISER/`
  - etc.

- All projects share a common `global_scripts` repository:
  - `/precision_marketing/precision_marketing_*/precision_marketing_app/update_scripts/global_scripts/`

### Making Changes to Shared Code

When making changes to global_scripts:

1. **Choose One Project for Development**:
   - Work in only one company's global_scripts directory at a time (e.g., KitchenMAMA or WISER).
   - Ensure you're working with the latest code by pulling first.

2. **Commit and Push Changes**:
   ```bash
   # While in the global_scripts directory of your chosen project
   git add [changed_files]
   git commit -m "[prefix] Descriptive commit message"
   git push origin main
   ```

3. **Update Other Company Projects**:
   ```bash
   # Move to global_scripts in another company project
   cd /precision_marketing/precision_marketing_[OTHER_COMPANY]/precision_marketing_app/update_scripts/global_scripts
   
   # Pull changes from the remote repository
   git pull origin main
   ```

### Handling Conflicts

If you encounter merge conflicts when pulling changes:

1. **Stash any local changes**:
   ```bash
   git stash
   ```

2. **Pull remote changes**:
   ```bash
   git pull origin main
   ```

3. **Apply stashed changes (if any)**:
   ```bash
   git stash pop
   ```

4. **Resolve conflicts** if they occur

### Synchronization Script (Optional)

To ensure all company projects are in sync, you can use this script:

```bash
#!/bin/bash
# sync_global_scripts.sh

COMPANIES=("KitchenMAMA" "WISER")
BASE_PATH="/precision_marketing"

# Pull latest changes in all companies
for company in "${COMPANIES[@]}"; do
  echo "Syncing $company..."
  cd "$BASE_PATH/precision_marketing_$company/precision_marketing_app/update_scripts/global_scripts"
  git pull origin main
done

echo "All global_scripts directories are now in sync."
```

Save as `sync_global_scripts.sh`, make executable with `chmod +x sync_global_scripts.sh`, and run whenever needed.

### Best Practices

1. **Always pull before making changes** to avoid conflicts
2. **Test changes in multiple company environments** when possible
3. **Use clear commit messages** with appropriate prefixes
4. **Communicate major changes** to all teams using the shared code
5. **Keep company-specific code outside** of global_scripts

## Daily Workflow

### Start of Day
```bash
# Switch to main branch
git checkout main

# Get latest changes
git pull origin main

# Create a new branch for today's work
git checkout -b feature/descriptive-name
```

### During the Day
```bash
# Check status of files
git status

# Review your changes
git diff

# Stage specific files
git add path/to/file.R

# Commit with meaningful message
git commit -m "[category] Brief description of changes"

# Push your branch to remote (first time)
git push -u origin feature/descriptive-name

# Push subsequent changes
git push
```

### End of Day (if work is incomplete)
```bash
# Commit all changes (even work in progress)
git add .
git commit -m "[WIP][category] Description of work in progress"

# Push to remote to back up your work
git push
```

## Branch Strategy

### Branch Types
- **main**: Production-ready code
- **feature/**: New functionality (`feature/new-analysis-model`)
- **bugfix/**: Bug fixes (`bugfix/correct-calculation-error`)
- **hotfix/**: Urgent production fixes (`hotfix/critical-data-issue`)
- **refactor/**: Code improvements without new features (`refactor/optimize-data-processing`)
- **docs/**: Documentation updates (`docs/update-model-parameters`)

### Branch Lifecycle
1. Create branch from main
2. Develop and commit changes
3. Push branch to remote
4. Create pull request
5. Address review feedback
6. Merge to main
7. Delete branch

## Commit Guidelines

### Commit Message Format
```
[category] Short summary (50 chars or less)

More detailed explanation if necessary. Wrap text at
around 72 characters. Explain what and why vs. how.

Reference issues at bottom: #123
```

### Categories for R Scripts
- `[config]`: Configuration file changes
- `[db]`: Database operations
- `[models]`: Statistical models and analysis
- `[data]`: Data processing, transformations
- `[utils]`: Utility functions
- `[ai]`: AI/ML components
- `[docs]`: Documentation updates

### Best Practices
- Make small, focused commits
- Each commit should represent one logical change
- Include relevant issue/ticket numbers
- Don't commit commented-out code
- Don't commit temporary debug statements
- Don't commit large data files

## Pull Requests

### Creating a PR
1. Push your branch to GitHub
2. Go to the repository on GitHub
3. Click "Compare & pull request"
4. Fill in the PR template
5. Request reviews from relevant team members

### PR Template
```markdown
## Description
[Provide a brief description of the changes]

## Type of Change
- [ ] New feature (non-breaking change adding functionality)
- [ ] Bug fix (non-breaking change fixing an issue)
- [ ] Breaking change (fix or feature causing existing functionality to change)
- [ ] Documentation update
- [ ] Refactoring/code improvement

## How Has This Been Tested?
[Describe the testing process]

## Checklist:
- [ ] My code follows the project's style guidelines
- [ ] I have commented my code, particularly in hard-to-understand areas
- [ ] I have updated the documentation accordingly
- [ ] My changes generate no new warnings
- [ ] Any dependent changes have been merged and published
```

## Code Reviews

### Reviewer Guidelines
- Check that the code accomplishes the stated goals
- Verify code readability and maintainability
- Confirm adherence to R coding standards
- Look for potential bugs or edge cases
- Suggest optimizations when appropriate
- Verify documentation is updated

### Author Guidelines
- Respond to all comments, either by making changes or explaining why changes weren't made
- Break large PRs into smaller, more manageable pieces
- Provide context and reasoning in the PR description

## Versioning

We follow [Semantic Versioning](https://semver.org/):
- **MAJOR.MINOR.PATCH** (e.g., 1.2.3)
- **MAJOR**: Incompatible API changes
- **MINOR**: New functionality (backwards compatible)
- **PATCH**: Bug fixes (backwards compatible)

### Creating a Version
```bash
# Tag a version
git tag -a v1.2.3 -m "Version 1.2.3 - Add new recommendation model"

# Push tag to remote
git push origin v1.2.3
```

## Handling R-Specific Code

### Script Dependencies
- Document script dependencies at the top of each file
- Update dependencies when they change
- Consider using package management tools (renv, packrat)

### Data File Management
- Don't commit large data files to Git
- Use .gitignore to exclude data directories
- Document where data files should be stored
- Consider using Git LFS for essential data files

### R Package Best Practices
- Follow tidyverse style guide for R code
- Organize code in modular functions
- Document functions with roxygen-style comments
- Add tests for critical functionality

## Git Cheat Sheet

### Basic Commands
```bash
# Clone repository
git clone git@github.com:kiki830621/precision_marketing_global_scripts.git

# Check status
git status

# View changes
git diff

# Stage files
git add file.R
git add .  # Add all files (use carefully)

# Commit
git commit -m "[category] Message"

# Push to remote
git push

# Pull from remote
git pull

# Check branch
git branch

# Create branch
git checkout -b branch-name

# Switch branch
git checkout branch-name

# Merge branches (rarely needed with PR workflow)
git merge other-branch

# View commit history
git log
git log --oneline --graph --decorate --all  # Pretty history
```

### Advanced Commands
```bash
# Undo last commit (keeping changes)
git reset --soft HEAD~1

# Discard uncommitted changes
git checkout -- file.R
git restore file.R  # Git 2.23+

# Temporarily save changes without committing
git stash
git stash pop  # Restore stashed changes

# View changes between branches
git diff main..feature-branch

# Cherry-pick a commit
git cherry-pick commit-hash

# View file history
git log -p filename

# Find when a bug was introduced
git bisect start
git bisect bad  # Current commit has bug
git bisect good commit-hash  # Known good commit
# Git will help you find the problematic commit
```

## Useful Tools

- **GitHub Desktop**: Visual Git management for Windows/Mac
- **RStudio Git Integration**: Basic Git operations from RStudio
- **GitKraken**: Full-featured Git GUI
- **Sourcetree**: Another popular Git GUI
- **GitHub CLI**: Command-line interface for GitHub

## Getting Help

If you encounter Git issues:
1. Check this workflow document
2. Review the [Managing Shared global_scripts](#managing-shared-global_scripts) section for help with cross-project issues
3. Consult [GitHub's documentation](https://docs.github.com/en/github)
4. Contact the development team lead

---

Last Updated: April 1, 2025