# Precision Marketing Platform Deployment Guide

This document provides specific instructions for deploying the Customer DNA Analysis application to both ShinyApps.io and Posit Connect, based on our existing deployment process.

## Current Deployment Configuration

The project already has a deployment script at `/Users/che/Library/CloudStorage/Dropbox/precision_marketing/precision_marketing_MAMBA/precision_marketing_app/rsconnect/deployment.R`, which is configured for ShinyApps.io deployment with the following settings:

- **App Name**: WISER_Precition_Markrting_Martech
- **App Title**: AI Marketing Platform
- **Account**: kyle-lin (ShinyApps.io)

The script includes sophisticated file management to:
1. Exclude unnecessary files (.RData, .Rproj, etc.)
2. Include only essential script folders for deployment
3. Configure deployment options (bundle size and file limits)

## Deploying to Posit Connect

To deploy to Posit Connect instead of ShinyApps.io, we need to modify the deployment process.

### Step 1: Configure Posit Connect Server

First, register your Posit Connect server (do this once):

```r
# Add the Posit Connect server
rsconnect::addServer(
  url = "https://connect.example.com/",  # Replace with your Posit Connect URL
  name = "precision-marketing-connect"
)
```

### Step 2: Authenticate with Posit Connect

Choose one of the following authentication methods:

**Option 1: Interactive Authentication (recommended for local deployment)**

```r
# Interactive authentication - will open a browser
rsconnect::connectUser(server = "precision-marketing-connect")
```

**Option 2: API Key Authentication (for automated deployment)**

```r
# Get your API key from Posit Connect → User Menu → API Keys
rsconnect::connectApiUser(
  server = "precision-marketing-connect",
  apiKey = "YOUR_API_KEY"  # Replace with your actual API key
)
```

### Step 3: Modify the Deployment Script for Posit Connect

Create a new deployment script `posit_connect_deploy.R` based on our existing `deployment.R`:

```r
# AI Marketing Platform - Customer DNA App Deployment Script for Posit Connect
library(rsconnect)
library(stringr)

#-----------------------------------------------------------------------------
# Configuration
#-----------------------------------------------------------------------------
APP_NAME <- "precision-marketing-customer-dna"
APP_TITLE <- "Customer DNA Analysis Platform"
SERVER <- "precision-marketing-connect"  # Replace with your server name

#-----------------------------------------------------------------------------
# Files to exclude from deployment
#-----------------------------------------------------------------------------
files_to_exclude <- c(
  ".RData", 
  ".Rproj", 
  ".idea", 
  ".Rhistory", 
  "deprecated", 
  "app_screenshots",
  "output"
)

#-----------------------------------------------------------------------------
# Get only essential files needed for Shiny app deployment
#-----------------------------------------------------------------------------
# Only include specific necessary script folders
essential_script_paths <- c(
  # Configuration files
  "update_scripts/global_scripts/03_config",
  # UI utility functions
  "update_scripts/global_scripts/11_rshinyapp_utils",
  # Formatting utilities
  "update_scripts/global_scripts/04_utils",
  # Shiny app modules - critical for app functionality
  "update_scripts/global_scripts/10_rshinyapp_modules",
  # Customer DNA specific components
  "update_scripts/global_scripts/10_rshinyapp_components/micro/microCustomer",
  # Universal data accessor
  "update_scripts/global_scripts/02_db_utils"
)

# Function to get files from specific paths
get_essential_files <- function(paths) {
  files <- c()
  for(path in paths) {
    if(dir.exists(path)) {
      # For the modules directory, include subdirectories recursively
      if(grepl("10_rshinyapp_modules|10_rshinyapp_components", path)) {
        path_files <- list.files(
          path = path,
          pattern = "\\.R$",  # Only R files
          full.names = TRUE,
          recursive = TRUE    # Include all subdirectories for modules
        )
      } else {
        # For other directories, just include files at the top level
        path_files <- list.files(
          path = path,
          pattern = "\\.R$",  # Only R files
          full.names = TRUE,
          recursive = FALSE
        )
      }
      files <- c(files, path_files)
    }
  }
  return(files)
}

# Get essential files
add_file_path <- get_essential_files(essential_script_paths)

# Uncomment to see which files will be included in deployment
# cat("\nIncluding the following files from essential paths:\n")
# for(file in add_file_path) {
#   cat(" - ", file, "\n")
# }
# cat("\nTotal files from essential paths:", length(add_file_path), "\n")

#-----------------------------------------------------------------------------
# Set deployment options
#-----------------------------------------------------------------------------
options(
  rsconnect.max.bundle.size = (5 * 1024 * 1024 * 1024),  # 5GB max bundle size
  rsconnect.max.bundle.files = 10000                     # 10,000 max files
)

#-----------------------------------------------------------------------------
# Create file list for deployment
#-----------------------------------------------------------------------------
deployment_files <- c(
  listDeploymentFiles(getwd())[
    !str_detect(
      listDeploymentFiles(getwd()), 
      paste(files_to_exclude, collapse = "|")
    )
  ],
  add_file_path
)

#-----------------------------------------------------------------------------
# Primary application file to deploy
#-----------------------------------------------------------------------------
app_file <- "update_scripts/global_scripts/21_rshinyapp_templates/customer_dna_production_app.R"

# Add this to the deployment files if not already included
if(!app_file %in% deployment_files) {
  deployment_files <- c(deployment_files, app_file)
}

#-----------------------------------------------------------------------------
# Deploy the application
#-----------------------------------------------------------------------------
deployApp(
  appDir = getwd(),
  appName = APP_NAME,
  appTitle = APP_TITLE,
  appFiles = deployment_files,
  appPrimaryDoc = app_file,  # Specify the main R file
  server = SERVER,
  forceUpdate = TRUE,
  logLevel = "verbose",
  envManagement = TRUE  # Let Posit Connect manage dependencies
)

#-----------------------------------------------------------------------------
# Application URL
#-----------------------------------------------------------------------------
cat("\nDeployment complete!\n")
cat(paste0("Check the Posit Connect dashboard for your application URL\n"))
```

### Step 4: Environment Management

For proper dependency management in Posit Connect, there are two options:

**Option 1: Let Posit Connect manage dependencies**

This is configured with `envManagement = TRUE` in the deployment script.

**Option 2: Use renv for dependency management**

```r
# Initialize renv
renv::init()

# Snapshot dependencies
renv::snapshot()

# Deploy with renv
deployApp(
  appDir = getwd(),
  appName = APP_NAME,
  appTitle = APP_TITLE,
  appFiles = deployment_files,
  server = SERVER,
  forceUpdate = TRUE,
  envManagement = FALSE  # Use renv instead
)
```

### Step 5: Environment Variables

For secure configuration (database credentials, API keys, etc.):

```r
# Set environment variables locally
Sys.setenv(
  DB_CONNECTION_STRING = "your-connection-string",
  API_KEY = "your-api-key"
)

# Deploy with environment variables
deployApp(
  # ... other parameters ...
  envVars = c("DB_CONNECTION_STRING", "API_KEY")
)
```

## Deployment Options for Customer DNA App

### Standard Deployment

For a standard deployment of the Customer DNA app:

```r
rsconnect::deployApp(
  appDir = getwd(),
  appName = "customer-dna-platform",
  appTitle = "Customer DNA Analysis Platform",
  appFiles = deployment_files,
  appPrimaryDoc = "update_scripts/global_scripts/21_rshinyapp_templates/customer_dna_production_app.R",
  server = "precision-marketing-connect",
  forceUpdate = TRUE,
  logLevel = "verbose"
)
```

### Minimal Deployment

For a lightweight version:

```r
rsconnect::deployApp(
  appDir = "update_scripts/global_scripts/21_rshinyapp_templates",
  appName = "customer-dna-minimal",
  appTitle = "Customer DNA Analysis (Minimal)",
  server = "precision-marketing-connect"
)
```

### Memory-Optimized Deployment

For large datasets:

```r
rsconnect::deployApp(
  appDir = getwd(),
  appName = "customer-dna-platform",
  appTitle = "Customer DNA Analysis Platform",
  appFiles = deployment_files,
  server = "precision-marketing-connect",
  forceUpdate = TRUE,
  metadata = list(
    runtime_config = list(
      max_processes = 3,
      max_connections = 20,
      connect_timeout = 120
    )
  )
)
```

## Automated Deployment with GitHub Actions

For automated deployment via GitHub Actions, create a workflow file:

```yaml
name: Deploy Customer DNA App

on:
  push:
    branches: [ main ]
  workflow_dispatch:

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      
      - name: Set up R
        uses: r-lib/actions/setup-r@v2
        
      - name: Install dependencies
        run: |
          install.packages(c("rsconnect", "stringr"))
        shell: Rscript {0}
        
      - name: Deploy to Posit Connect
        run: |
          rsconnect::setAccountInfo(
            name="${{ secrets.CONNECT_ACCOUNT }}",
            token="${{ secrets.CONNECT_TOKEN }}",
            secret="${{ secrets.CONNECT_SECRET }}"
          )
          source("path/to/posit_connect_deploy.R")
        shell: Rscript {0}
        env:
          CONNECT_ACCOUNT: ${{ secrets.CONNECT_ACCOUNT }}
          CONNECT_TOKEN: ${{ secrets.CONNECT_TOKEN }}
          CONNECT_SECRET: ${{ secrets.CONNECT_SECRET }}
```

## Troubleshooting Project-Specific Issues

### Common Issues

1. **Missing dependencies**: Use `envManagement = TRUE` to let Posit Connect install required packages.

2. **File paths**: Ensure all file paths in the app use relative paths from the app's root directory.

3. **Data access**: If using database connections, ensure credentials are properly passed through environment variables.

4. **Memory issues**: Configure resource allocation in Posit Connect for memory-intensive operations.

## Access Control

In Posit Connect, set access permissions for your application:

1. After deployment, navigate to your app in Posit Connect
2. Go to "Access" tab
3. Configure permissions:
   - "Anyone - no login required" for public access
   - "All users" for authenticated access
   - "Specific users/groups" for restricted access