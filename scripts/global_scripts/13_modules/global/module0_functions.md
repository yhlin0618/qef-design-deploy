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

### 3. Set Up GitHub Repository

```
Let the Company be [company], Please clone a GitHub repository connection to replace a local repository. If there are files in the local repository, they should be deleted. 

My local repository is located at /Users/[username]/Library/CloudStorage/Dropbox/precision_marketing/precision_marketing_[company]/precision_marketing_app/update_scripts/global_scripts/, and the remote repository URL is git@github.com:kiki830621/precision_marketing_global_scripts.git

After cloning, please run the exclude_git_from_dropbox.sh script from the bash directory to set up proper Dropbox-Git integration, and verify that Git operations still work correctly.

IMPORTANT: When running the script, first make sure to CD into the global_scripts directory, then run the script with a relative path: bash bash/exclude_git_from_dropbox.sh
```

### 4. Create Git Synchronization Tool

```
Please create an R script that helps me easily pull and push GitHub changes from within R. Save it as utils/git_sync.R.
```

### 5. Create Dropbox-Git Integration Script

```
Please create a bash script that can be applied to all Git repositories in Dropbox to prevent sync conflicts. The script should:
1. Find all .git directories in the precision_marketing folder
2. Rename each .git directory to .git.nosync
3. Create symbolic links from .git to .git.nosync
4. Add .dropbox.ignore files
5. Update .gitignore files to ignore Dropbox conflict files

The script should use relative paths and detect the repository location automatically, since it will be run from different computers with different path structures.

Save the script at global_scripts/bash/exclude_git_from_dropbox.sh and make it executable.
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
Please check and update the 00_principles/000g_initialization_update_mode.R script to ensure it properly loads all necessary libraries and global scripts in the correct order.
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
Please create a troubleshooting guide explaining common issues that may arise when setting up or running a precision marketing project, especially focusing on Git-Dropbox integration issues, and how to resolve these issues.
```

---

## 📝 Usage Guide

1. Choose the appropriate prompt based on your current project stage
2. Copy and paste the prompt to Claude
3. Replace placeholders like `[username]` and `[company]` with your actual information
4. Follow Claude's guidance to complete the operation
5. Proceed to the next prompt as you complete each step

These prompts are designed to help you efficiently interact with Claude and successfully set up your precision marketing project!