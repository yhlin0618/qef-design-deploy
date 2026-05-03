# Module0: Setting Up Project Structure (Claude Prompt Collection)

This document provides prompts for interacting with Claude to help you set up a precision marketing project structure. Each step includes instructions you can directly copy and paste to Claude to streamline the process.

## 🚀 Initial Setup and GitHub Synchronization

### 1. Check Project Structure

```
Please check my project structure. My root directory is /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing_[company]/. Please show me the files and subdirectories in this directory and tell me if it conforms to the standard precision marketing project structure.
```

### 2. Merge and Organize Utility Directories

```
Please help me merge /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing_[company]/precision_marketing_app/update_scripts/global_scripts/utils_old into the utils directory, and merge rshinyapp_utils_old into the rshinyapp_utils directory to resolve duplications and ambiguities.
```

### 3a. Set Up GitHub Repository in Existing Project

```
Let the Company be [company]. Please help me set up a clean GitHub repository integration with Dropbox for an EXISTING project, using the following safer approach:

1. First, clone the repository to a temporary location outside of Dropbox:
   mkdir -p ~/temp_repo
   git clone git@github.com:kiki830621/precision_marketing_global_scripts.git ~/temp_repo/global_scripts

2. Run the Dropbox integration script while the repository is still outside Dropbox:
   cd ~/temp_repo/global_scripts
   bash 12_bash/exclude_git_from_dropbox_noninteractive.sh

3. Create a backup of any existing files in my target location:
   mv /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing_[company]/precision_marketing_app/update_scripts/global_scripts /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing_[company]/precision_marketing_app/update_scripts/global_scripts_backup

4. Move the properly prepared repository into Dropbox using rsync:
   rsync -av --delete ~/temp_repo/global_scripts/ /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing_[company]/precision_marketing_app/update_scripts/global_scripts/

5. Verify that Git operations work correctly after the move:
   cd /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing_[company]/precision_marketing_app/update_scripts/global_scripts
   git status

6. Clean up temporary files if everything is working:
   rm -rf ~/temp_repo

Please walk me through these steps, explaining what each command does and any potential issues to watch out for. If you see any way to improve this process further, please let me know.
```

### 3b. Set Up GitHub Repository in New Project

```
Let the Company be [new_company]. I am setting up a completely new precision marketing project and need to clone the global_scripts repository. Please help me set up a clean GitHub repository integration with Dropbox for this NEW project:

1. First, ensure the project directory structure exists:
   mkdir -p /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing_[new_company]/precision_marketing_app/update_scripts/

2. Clone the repository to a temporary location outside of Dropbox:
   mkdir -p ~/temp_repo
   git clone git@github.com:kiki830621/precision_marketing_global_scripts.git ~/temp_repo/global_scripts

3. Run the Dropbox integration script while the repository is still outside Dropbox:
   cd ~/temp_repo/global_scripts
   bash 12_bash/exclude_git_from_dropbox_noninteractive.sh

4. Move the properly prepared repository into the new project's Dropbox location:
   rsync -av ~/temp_repo/global_scripts/ /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing_[new_company]/precision_marketing_app/update_scripts/global_scripts/

5. Verify that Git operations work correctly after the move:
   cd /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing_[new_company]/precision_marketing_app/update_scripts/global_scripts
   git status
   
6. Create company-specific configuration:
   mkdir -p /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing_[new_company]/precision_marketing_app/local_scripts
   cp /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing_[new_company]/precision_marketing_app/update_scripts/global_scripts/03_config/global_parameters.R /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing_[new_company]/precision_marketing_app/local_scripts/brand_specific_parameters.R
   
7. Update brand_specific_parameters.R with the new company name and settings

8. Clean up temporary files:
   rm -rf ~/temp_repo

Please walk me through these steps, explaining what each command does and providing any additional steps needed to complete the setup of a new project.
```

### 4. Create Git Synchronization Tool

```
Please create an R script that helps me easily pull and push GitHub changes from within R. Save it as utils/git_sync.R.
```

### 5. Create Dropbox-Git Integration Scripts

```
Please create two bash scripts for Git-Dropbox integration:

1. An interactive version (exclude_git_from_dropbox.sh) that:
   - Finds all .git directories in the precision_marketing folder
   - Renames each .git directory to .git.nosync
   - Creates symbolic links from .git to .git.nosync
   - Adds .dropbox.ignore files with ".git/" entry
   - Updates .gitignore files to ignore Dropbox conflict files
   - Asks for confirmation before processing each repository

2. A non-interactive version (exclude_git_from_dropbox_noninteractive.sh) that:
   - Does the same operations as the interactive version
   - Only processes the current repository without asking for confirmation
   - Suitable for use in automated scripts or during initial repository setup

3. A verification script (git_dropbox_check.sh) that:
   - Checks that .git is a symlink to .git.nosync
   - Checks that .git.nosync is not a symlink (no recursive symlinks)
   - Verifies .dropbox.ignore exists and contains ".git/"
   - Confirms Git operations are working
   - Reports any problems found

All scripts should be saved in the global_scripts/12_bash/ directory and made executable.
```

## 📁 Directory Structure Creation and Configuration

### 6. Create a New Precision Marketing Project

```
I need to create a brand new precision marketing project for a new client [company]. Please guide me through the standard process of creating all required directories in Dropbox with proper permissions and structure.
```

### 7. Configure .gitignore

```
Please create a .gitignore file suitable for a precision marketing project, ensuring that R project files (.RData, .Rhistory, .Rproj.user) and sensitive data are not synced to Git. Also include patterns to ignore Dropbox conflict files.
```

### 8. Initialize Local Scripts

```
Please help me check if my local_scripts directory contains all necessary files, especially ensuring that brand_specific_parameters.R is correctly configured. If needed, please help me create or update these files.
```

## 🔧 Database and Tool Configuration

### 9. Set Up DuckDB Database

```
Please help me check and configure the DuckDB database connections, ensuring all functions in the db_utils directory work properly. If new table structures need to be created, please provide the necessary SQL statements.
```

### 10. Check AI Model Integration

```
Please check if the scripts in the ai directory are correctly configured, ensuring that the integration with Python works properly. If updates are needed, please provide the necessary modifications.
```

### 11. Update Initialization Script

```
Please check and update the 000g_initialization_update_mode.R script to ensure it properly loads all necessary libraries and global scripts in the correct order.
```

## 📊 Data Processing and Analysis Setup

### 12. Set Up Data Processing Pipeline

```
Please help me check the scripts in the data_processing directory to ensure they can correctly process data from Amazon and the official website. If any scripts are missing or need updates, please provide recommendations.
```

### 13. Configure Query Functions

```
Please help me check the query functions in the queries directory to ensure they can correctly retrieve data from the database. If needed, please provide new query functions for specific business needs.
```

### 14. Set Up Model Training Workflow

```
Please check the model training scripts in the models directory to ensure they can correctly train and evaluate models. If needed, please provide improvement suggestions.
```

## 🖥️ Shiny Application Configuration

### 15. Configure Shiny Modules

```
Please check the rshinyapp_modules directory to ensure all necessary Shiny modules are ready and can correctly display data and analysis results.
```

### 16. Check Shiny Utility Functions

```
Please check the utility functions in the rshinyapp_utils directory to ensure they can support the proper functioning of the Shiny application.
```

## 🔄 Daily Operations and Maintenance

### 17. Create Data Update Workflow

```
Please help me create a clear workflow describing how to regularly update data, including the complete process from raw data to processed data, to model training and application updates.
```

### 18. Create Git Synchronization Workflow

```
Please help me create a clear workflow describing how to use the git_sync.R script to synchronize changes in the global_scripts directory, including how to handle conflicts and merge changes between Git and Dropbox.
```

### 19. Troubleshooting Guide

```
Please create a comprehensive troubleshooting guide explaining common issues that may arise when setting up or running a precision marketing project. Focus especially on:

1. Git-Dropbox integration issues, including:
   - Recursive symlink problems (.git.nosync/.git.nosync errors)
   - Corrupted git index files (index file smaller than expected)
   - Problems with GitHub Desktop recognizing repositories
   - Dropbox sync conflicts with Git files
   - How to recover a broken Git repository in Dropbox
   - Steps to restore Git functionality if synchronization breaks

2. Database connection issues:
   - DuckDB connection errors
   - Table creation and modification problems
   - Database locking issues

3. R package compatibility issues:
   - Environment setup problems
   - Package version conflicts

4. Data processing pipeline failures:
   - Input data formatting issues
   - Data transformation errors

For each issue, provide clear diagnostic steps, solution procedures, and preventative measures to avoid future occurrences.
```

---

## 📝 Usage Guide

1. Choose the appropriate prompt based on your current project stage
2. Copy and paste the prompt to Claude
3. Replace placeholders like `[username]` and `[company]` with your actual information
4. Follow Claude's guidance to complete the operation
5. Proceed to the next prompt as you complete each step

These prompts are designed to help you efficiently interact with Claude and successfully set up your precision marketing project!