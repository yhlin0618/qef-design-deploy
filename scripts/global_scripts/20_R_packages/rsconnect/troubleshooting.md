# Troubleshooting rsconnect Deployments

This document provides guidance for diagnosing and resolving common issues encountered when deploying R content with the rsconnect package.

## Common Deployment Issues

### Authentication Problems

| Issue | Symptoms | Solution |
|-------|----------|----------|
| Invalid credentials | "Unauthorized" or 401 errors | Re-authenticate with `connectUser()` or `connectApiUser()` |
| Expired API key | "Invalid API key" error | Generate a new API key in Posit Connect UI |
| Server connection | Unable to connect to server | Check server URL, internet connection, and SSL certificates |

To diagnose authentication issues:

```r
# Check account status
rsconnect::accounts()

# Re-authenticate if needed
rsconnect::connectUser(server = "your-server")
```

### Package Dependency Issues

| Issue | Symptoms | Solution |
|-------|----------|----------|
| Missing packages | Error during bundle creation | Install missing packages locally |
| Version conflicts | Deployment succeeds but app fails | Use `renv` to manage dependencies |
| Binary incompatibility | App works locally but fails on server | Check package versions and platforms |

To diagnose dependency issues:

```r
# Use verbose logging
rsconnect::deployApp(
  appDir = "your-app",
  logLevel = "verbose"
)

# Check package dependencies
rsconnect::appDependencies("your-app")
```

### Content Size Limitations

| Issue | Symptoms | Solution |
|-------|----------|----------|
| Large app bundle | Slow upload or timeout | Reduce app size by excluding unnecessary files |
| Data file too large | Deployment fails with size error | Use external data storage or database |
| Many small files | Slow bundling process | Consolidate files or use `.rscignore` |

To manage content size:

```r
# Create an .rscignore file in your app directory
# Example .rscignore contents:
.git/
.Rproj.user/
*.csv
*.rds
data/raw/
```

## Diagnosing Problems

### Verbose Logging

Enable detailed logs during deployment:

```r
rsconnect::deployApp(
  appDir = "your-app",
  logLevel = "verbose"
)
```

### Check Deployment Manifest

Examine the deployment files that will be uploaded:

```r
rsconnect::listDeploymentFiles("your-app")
```

### Deployment Capturing

For debugging complex deployments:

```r
rsconnect::wrproductanifest("your-app")
```

This creates a `manifest.json` file with all deployment settings.

## Fixing Specific Issues

### SSL Certificate Problems

If you encounter SSL certificate validation errors:

```r
# Add certificate to server configuration
rsconnect::addServerCertificate(
  name = "your-server",
  certificate = "/path/to/cert.pem"
)
```

### Deployment Without Records

If you need to update an app without a local deployment record:

```r
# Find the app ID first
apps <- rsconnect::applications(server = "your-server")
app_id <- apps$id[apps$name == "your-app-name"]

# Deploy using app ID
rsconnect::deployApp(
  appDir = "your-app",
  appId = app_id,
  server = "your-server"
)
```

### Handling Platform-specific Issues

For cross-platform deployment:

```r
# Windows to Linux deployment
rsconnect::deployApp(
  appDir = "your-app",
  # Avoid platform-specific binary dependencies
  # Use envManagement to let Connect handle package installation
  envManagement = TRUE
)
```

### Environment Variable Problems

If environment variables aren't working:

```r
# Make sure they're set locally first
Sys.setenv(API_KEY = "your-key")

# Then include in deployment
rsconnect::deployApp(
  appDir = "your-app",
  envVars = c("API_KEY")
)
```

## Advanced Troubleshooting

### Custom Deployment Function

Create a function with error handling:

```r
safe_deploy <- function(app_dir, server_name) {
  tryCatch({
    rsconnect::deployApp(
      appDir = app_dir,
      server = server_name,
      logLevel = "verbose",
      on.failure = function(log_url) {
        message("Deployment failed. Logs available at: ", log_url)
      }
    )
  }, error = function(e) {
    message("Error during deployment: ", e$message)
    # Additional error handling
  })
}
```

### Testing Deployment Locally

Create a minimal test app to verify your setup:

```r
# Create minimal test app
dir.create("test-app")
cat('library(shiny)\nui <- fluidPage("Test")\nserver <- function(input, output) {}\nshinyApp(ui, server)', 
    file = "test-app/app.R")

# Try deploying it
rsconnect::deployApp("test-app")
```

### Server-side Logs

After deployment, check logs on the server:

```r
# For ShinyApps.io
rsconnect::showLogs(appName = "your-app")

# For Posit Connect, check the logs in the Connect dashboard
```

## Contact Support

If you've tried the solutions above and still have issues:

1. For ShinyApps.io: Contact support at support@rstudio.com
2. For Posit Connect: Contact your server administrator
3. For package bugs: File an issue at https://github.com/rstudio/rsconnect/issues