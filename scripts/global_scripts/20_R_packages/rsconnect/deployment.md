# Deployment Functions in rsconnect

This document covers the key deployment functions in the rsconnect package, with a focus on deploying Shiny applications to Posit Connect.

## Core Deployment Functions

The rsconnect package provides several specialized deployment functions:

| Function | Purpose |
|----------|---------|
| `deployApp()` | Deploy Shiny applications |
| `deployDoc()` | Deploy R Markdown documents |
| `deployAPI()` | Deploy Plumber APIs |
| `deploySite()` | Deploy static websites |
| `deployTFModel()` | Deploy TensorFlow models |

## deployApp()

`deployApp()` is the most commonly used function, designed for deploying Shiny applications.

### Basic Syntax

```r
rsconnect::deployApp(
  appDir = getwd(),          # Directory containing the app
  appName = NULL,            # Name for the app
  appTitle = NULL,           # Display title
  account = NULL,            # Account name
  server = NULL,             # Server name
  launch.browser = TRUE      # Launch browser after deploy
)
```

### Common Parameters

| Parameter | Description |
|-----------|-------------|
| `appDir` | Directory containing the application files |
| `appName` | Application name (letters, numbers, `-` and `_` only) |
| `appTitle` | Human-readable title displayed in the UI |
| `appFiles` | Specific files to include in the deployment |
| `appFileManifest` | Path to a file listing files to include |
| `envVars` | Environment variables to send to the server |
| `appId` | Deploy to a specific application ID |
| `account` | Account name to use for deployment |
| `server` | Server name to deploy to |
| `logLevel` | Verbosity level (`"normal"`, `"quiet"`, or `"verbose"`) |
| `launch.browser` | Whether to open the deployed app in a browser |

### File Selection

You have three options for specifying which files to include:

1. **Default** - Include all files in `appDir` (except those in `.rscignore`)
2. **Explicit list** - Provide a character vector to `appFiles`
3. **Manifest file** - Specify a file containing the list with `appFileManifest`

```r
# Option 1: Default (all files)
deployApp()

# Option 2: Explicit list
deployApp(
  appFiles = c("app.R", "data/data.csv", "www/style.css")
)

# Option 3: Manifest file
deployApp(
  appFileManifest = "deployment_files.txt"
)
```

### Environment Variables

Environment variables can be securely passed to the server:

```r
# First set the variables locally
Sys.setenv(
  API_KEY = "secret-key",
  DATABASE_URL = "postgres://user:pass@host/db"
)

# Then deploy with those variables
deployApp(
  envVars = c("API_KEY", "DATABASE_URL")
)
```

### Deployment Records

After successful deployment, rsconnect creates a deployment record in the `rsconnect/` directory within your application folder. This record allows for simpler subsequent deployments by:

- Remembering the server and account
- Tracking the application ID
- Storing other deployment settings

These records should typically be committed to version control.

## deployDoc()

`deployDoc()` is specialized for deploying R Markdown documents:

```r
rsconnect::deployDoc(
  doc = "analysis.Rmd",      # Path to R Markdown document
  appName = NULL,            # Name for the deployed content
  appTitle = NULL,           # Display title
  account = NULL,            # Account name
  server = NULL,             # Server name
  launch.browser = TRUE      # Launch browser after deploy
)
```

## deployAPI()

`deployAPI()` deploys Plumber APIs:

```r
rsconnect::deployAPI(
  api = "plumber.R",         # Path to Plumber API file
  appName = NULL,            # Name for the deployed API
  appTitle = NULL,           # Display title
  account = NULL,            # Account name
  server = NULL,             # Server name
  launch.browser = TRUE      # Launch browser after deploy
)
```

## Advanced Deployment Options

### Quarto Support

For content that needs to be built with Quarto:

```r
deployApp(
  appDir = "quarto-project/",
  quarto = TRUE              # Use Quarto to build content
)
```

### Custom Docker Images

Specify a custom image for Connect to use:

```r
deployApp(
  appDir = "my-app/",
  image = "custom-r-image:4.2.0"
)
```

### Environment Management

Control how packages are installed:

```r
deployApp(
  appDir = "my-app/",
  envManagement = TRUE,      # Let Connect install packages
  # Or control R and Python separately:
  envManagementR = TRUE,
  envManagementPy = TRUE
)
```

### Python Integration

For apps using reticulate:

```r
deployApp(
  appDir = "python-app/",
  python = "/path/to/python",
  forceGeneratePythonEnvironment = TRUE
)
```