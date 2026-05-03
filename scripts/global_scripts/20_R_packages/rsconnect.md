# Deploying Shiny Applications to Posit Connect with rsconnect

This documentation provides a comprehensive guide for deploying Shiny applications to Posit Connect (formerly RStudio Connect) using the rsconnect package.

## Table of Contents

1. [Introduction](#introduction)
2. [Installation](#installation)
3. [Server Configuration](#server-configuration)
4. [Authentication](#authentication)
5. [Deployment](#deployment)
6. [Advanced Configuration](#advanced-configuration)
7. [Application Management](#application-management)
8. [Troubleshooting](#troubleshooting)
9. [Best Practices](#best-practices)

## Introduction

Posit Connect is a publishing platform for R and Python content, including Shiny applications, R Markdown reports, Plumber APIs, Quarto documents, and more. The `rsconnect` package provides R functions for deploying content to Posit Connect, ShinyApps.io, or Posit Cloud.

## Installation

To get started, install the rsconnect package:

```r
# Install from CRAN
install.packages("rsconnect")

# Or install development version from GitHub
# install.packages("devtools")
# devtools::install_github("rstudio/rsconnect")

# Load the package
library(rsconnect)
```

## Server Configuration

Before deploying applications, you must register the Posit Connect server with rsconnect.

### Adding a Server

```r
# Add a Posit Connect server
rsconnect::addServer(
  url = "https://connect.example.com/", # Replace with your server URL
  name = "my-connect-server"           # Optional custom name
)

# List available servers
rsconnect::servers()
```

If your server uses a self-signed certificate, you can provide it:

```r
rsconnect::addServer(
  url = "https://connect.example.com/",
  name = "my-connect-server",
  certificate = "/path/to/certificate.pem"
)
```

## Authentication

After configuring the server, you need to authenticate with it.

### Interactive Authentication

The simplest method (for interactive sessions):

```r
rsconnect::connectUser(
  server = "my-connect-server"  # The server name you specified earlier
)
```

This will open a web browser for you to log in to your Posit Connect server.

### API Key Authentication

For non-interactive or automated deployments, use an API key:

1. Log in to your Posit Connect server in a web browser
2. Click your username in the top-right corner
3. Select "API Keys"
4. Click "New API Key"
5. Copy the generated API key

Then, authenticate with:

```r
rsconnect::connectApiUser(
  server = "my-connect-server",
  apiKey = "YOUR_API_KEY_HERE"
)
```

### Viewing Accounts

To see registered accounts:

```r
# List all accounts
rsconnect::accounts()

# View details for a specific account
rsconnect::accountInfo(name = "your-username", server = "my-connect-server")
```

## Deployment

Once your server and authentication are configured, you can deploy applications.

### Basic Deployment

For a simple Shiny app in the current directory:

```r
rsconnect::deployApp()
```

For an app in another directory:

```r
rsconnect::deployApp(
  appDir = "/path/to/your/app"
)
```

### Customized Deployment

```r
rsconnect::deployApp(
  appDir = "/path/to/your/app",
  appName = "my-custom-app-name",
  appTitle = "My Descriptive App Title",
  account = "your-username",        # Optional if you have multiple accounts
  server = "my-connect-server",
  launch.browser = TRUE             # Open browser after deployment
)
```

### Selective File Deployment

To deploy only specific files:

```r
rsconnect::deployApp(
  appDir = "/path/to/your/app",
  appFiles = c(
    "app.R",                     # Main app file
    "data/processed_data.csv",   # Data file
    "www/custom.css"             # Style file
  )
)
```

### Environment Variables

To securely pass environment variables to your application:

```r
# Set environment variables locally first
Sys.setenv(DB_USERNAME = "user")
Sys.setenv(DB_PASSWORD = "password")

# Deploy with these environment variables
rsconnect::deployApp(
  appDir = "/path/to/your/app",
  envVars = c("DB_USERNAME", "DB_PASSWORD")
)
```

Values are sent securely and not stored in the deployment bundle.

## Advanced Configuration

### App Visibility

Control app visibility (ShinyApps.io only):

```r
rsconnect::deployApp(
  appDir = "/path/to/your/app",
  appVisibility = "private"  # or "public"
)
```

### Environment Management

Control how Posit Connect installs packages:

```r
rsconnect::deployApp(
  appDir = "/path/to/your/app",
  envManagement = TRUE  # or FALSE
)
```

### Python Integration

For apps that use reticulate:

```r
rsconnect::deployApp(
  appDir = "/path/to/your/app",
  python = "/path/to/python",
  forceGeneratePythonEnvironment = TRUE
)
```

### Metadata

Include custom metadata with your deployment:

```r
rsconnect::deployApp(
  appDir = "/path/to/your/app",
  metadata = list(
    author = "Your Name",
    version = "1.0.0",
    department = "Data Science"
  )
)
```

## Application Management

### List Applications

View your deployed applications:

```r
rsconnect::applications(server = "my-connect-server")
```

### Terminate an Application

Remove a deployed application:

```r
rsconnect::terminateApp(
  appName = "my-app-name",
  server = "my-connect-server"
)
```

### Restart an Application

Restart a running application:

```r
rsconnect::restartApp(
  appName = "my-app-name",
  server = "my-connect-server"
)
```

## Troubleshooting

### Deployment Logs

For verbose logs during deployment:

```r
rsconnect::deployApp(
  appDir = "/path/to/your/app",
  logLevel = "verbose"
)
```

### Handling Deployment Failures

Set up a function to handle deployment failures:

```r
handle_failure <- function(url) {
  message("Deployment failed. Check logs at: ", url)
  # Additional error handling
}

rsconnect::deployApp(
  appDir = "/path/to/your/app",
  on.failure = handle_failure
)
```

### Updating Without Deployment Records

If you need to update an existing app without a deployment record:

```r
rsconnect::deployApp(
  appDir = "/path/to/your/app",
  appId = "123456",  # The app ID from Posit Connect
  server = "my-connect-server"
)
```

## Best Practices

1. **Version Control**: Include the `rsconnect/` directory in your version control to preserve deployment settings.

2. **Dependencies**: Ensure all package dependencies are properly declared in your app.

3. **Environment Management**: Consider using `renv` for reproducible environments.

4. **Automation**: For CI/CD pipelines, use `connectApiUser()` with API keys.

5. **Security**: Use `envVars` parameter for sensitive information rather than hardcoding it.

6. **Testing**: Test your application thoroughly locally before deployment.

7. **Deployment Directory**: For apps with data preprocessing, use the `recordDir` parameter to keep deployment records with source code instead of with generated files.

8. **Content Organization**: Use consistent naming conventions for `appName` and `appTitle` to make content easier to find.

9. **Deployment Frequency**: Create regular deployment schedules for apps that need frequent updates.

10. **Documentation**: Keep a record of deployment settings, especially custom configurations.

## Deploying Our Customer DNA App

For our specific Customer DNA application, here's the recommended deployment approach:

```r
# Assuming we're in the project directory
library(rsconnect)

# Only needed once to set up the server connection
# rsconnect::addServer(
#   url = "https://connect.company.com/", 
#   name = "precision-marketing-connect"
# )

# Authenticate with the server (only needed once)
# rsconnect::connectUser(server = "precision-marketing-connect")

# Deploy the application
rsconnect::deployApp(
  appDir = "update_scripts/global_scripts/21_rshinyapp_templates",
  appName = "customer-dna-dashboard",
  appTitle = "Customer DNA Analysis Dashboard",
  appFiles = c(
    "customer_dna_production_app.R",
    # Include other necessary files here
  ),
  envVars = c(
    # Add any environment variables needed
  ),
  logLevel = "verbose"
)
```

This deployment configuration is tailored for our Customer DNA application, ensuring all necessary files and environment variables are included.