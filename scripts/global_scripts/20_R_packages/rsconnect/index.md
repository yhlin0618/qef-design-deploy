# rsconnect Package: Deployment Guide

The `rsconnect` package is the primary tool for deploying R content to Posit Connect (formerly RStudio Connect), ShinyApps.io, and Posit Cloud. This guide focuses on deploying Shiny applications to Posit Connect from R.

## Contents

- [Overview](#overview)
- [Key Functions](#key-functions)
- [Workflow](#workflow)
- [Example Usage](#example-usage)
- [Related Documentation](#related-documentation)

## Overview

The `rsconnect` package provides a streamlined workflow for deploying:
- Shiny applications
- R Markdown documents 
- Plumber APIs
- Quarto documents
- Static content
- TensorFlow models

It handles bundling, dependency management, authentication, and deployment tracking.

## Key Functions

### Server Management
- `addServer()` - Register a Posit Connect server
- `servers()` - List all registered servers
- `removeServer()` - Remove a server from the registry

### Authentication
- `connectUser()` - Interactive authentication with Posit Connect
- `connectApiUser()` - Authenticate using an API key
- `accounts()` - List all authenticated accounts

### Deployment
- `deployApp()` - Deploy a Shiny application
- `deployDoc()` - Deploy an R Markdown document
- `deployAPI()` - Deploy a Plumber API
- `deploySite()` - Deploy a static website
- `deployTFModel()` - Deploy a TensorFlow model

### Application Management
- `applications()` - List deployed applications
- `terminateApp()` - Remove a deployed application
- `restartApp()` - Restart a running application

## Workflow

The typical deployment workflow follows these steps:

1. **Configure the server** - Register the Posit Connect server using `addServer()`
2. **Authenticate** - Connect your user account using `connectUser()` or `connectApiUser()`
3. **Deploy content** - Deploy your application using `deployApp()` or related functions
4. **Manage applications** - View, terminate, or restart applications as needed

## Example Usage

Simple deployment example:

```r
library(rsconnect)

# 1. Register server (only needed once)
addServer(
  url = "https://connect.example.com/",
  name = "my-connect"
)

# 2. Authenticate (only needed once per session)
connectUser(server = "my-connect")

# 3. Deploy the application
deployApp(
  appDir = "/path/to/app",
  appName = "my-shiny-app",
  server = "my-connect"
)
```

## Related Documentation

For more detailed information, see:
- [Full rsconnect deployment guide](../rsconnect.md)
- [Posit Connect User Guide](https://docs.posit.co/connect/user/)
- [ShinyApps.io User Guide](https://docs.posit.co/shinyapps.io/)