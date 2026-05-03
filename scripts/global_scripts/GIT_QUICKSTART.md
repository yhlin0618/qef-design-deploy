# Git Quick Start Guide

## Setup (One-time)

1. **Install Git** from [git-scm.com](https://git-scm.com/)

2. **Set your identity**:
   ```bash
   git config --global user.name "Your Name"
   git config --global user.email "your.email@example.com"
   ```

3. **Set up SSH key** (for easy GitHub access):
   ```bash
   # Generate SSH key
   ssh-keygen -t ed25519 -C "your.email@example.com"
   
   # View public key to copy
   cat ~/.ssh/id_ed25519.pub
   ```
   Then add this key to your GitHub account under Settings > SSH and GPG keys

4. **Clone the repository**:
   ```bash
   git clone git@github.com:kiki830621/precision_marketing_global_scripts.git global_scripts
   cd global_scripts
   ```

5. **Set up Dropbox-Git integration** (IMPORTANT!):
   ```bash
   # After cloning, run this script to prevent Dropbox sync issues
   bash 12_bash/exclude_git_from_dropbox.sh
   # OR for non-interactive usage:
   bash 12_bash/exclude_git_from_dropbox_noninteractive.sh
   ```

## Daily Workflow (5 Steps)

### 1. Update your main branch
```bash
git checkout main
git pull origin main
```

### 2. Create a new branch for your work
```bash
git checkout -b feature/your-task-name
```

### 3. Make changes and commit frequently
```bash
# Check status
git status

# Add changes
git add file1.R file2.R

# Commit with meaningful message
git commit -m "[07_models] Add new regression function for sales prediction"
```

### 4. Push your changes to GitHub
```bash
git push -u origin feature/your-task-name
```

### 5. Create a Pull Request
- Go to GitHub repository
- Click "Compare & Pull Request"
- Fill out description
- Request review

## Common Tasks

### Checking your work
```bash
# See what files you've changed
git status

# See line-by-line changes
git diff

# See commit history
git log --oneline
```

### Handling mistakes
```bash
# Undo uncommitted changes to a file
git restore file.R

# Add more changes to your last commit
git add forgotten-file.R
git commit --amend

# Undo last commit but keep changes
git reset --soft HEAD~1
```

### Keeping up with other changes
```bash
# Update your local main
git checkout main
git pull

# Incorporate latest main into your branch
git checkout your-branch
git merge main
```

### Creating a clean history
```bash
# Before pushing: combine multiple commits
git rebase -i HEAD~3  # Interactive rebase of last 3 commits
```

## R-Specific Tips

- **Script headers**: Add a comment at top of each script with purpose and dependencies
- **Function changes**: Document new parameters in commit messages
- **Data files**: Don't commit large data files - add patterns to .gitignore
- **Testing**: Test your changes before committing

## Using the Root Path Configuration

To handle running scripts on different computers, use the root path utilities:

```r
# At the beginning of your R script
source("04_utils/set_root_path.R")
paths <- setup_project_paths("YOUR_COMPANY_NAME")  # e.g., "WISER", "KitchenMAMA"

# Now you can use:
data_dir <- paths$data_path
```

## Getting Help

- Run `git --help` or `git command --help`
- More details in GIT_WORKFLOW.md
- Ask the team's Git expert

---

Remember: commit early, commit often, pull regularly, and push your work daily!

Last updated: March 2025