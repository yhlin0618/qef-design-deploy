# Authentication with rsconnect

This document covers authentication methods and account management using the rsconnect package, focusing on connecting to Posit Connect, ShinyApps.io, and Posit Cloud.

## Authentication Methods

The rsconnect package provides different authentication methods depending on your deployment target and use case:

| Method | Use Case | Target Platforms |
|--------|----------|-----------------|
| `connectUser()` | Interactive sessions | Posit Connect |
| `connectApiUser()` | Non-interactive/automated deployment | Posit Connect | 
| `setAccountInfo()` | Token-based authentication | ShinyApps.io, Posit Cloud |

## Authenticating with Posit Connect

### Interactive Authentication

For typical interactive sessions, use `connectUser()`:

```r
library(rsconnect)

# Register the server (only needed once)
addServer(
  url = "https://connect.example.com/", 
  name = "my-connect"
)

# Interactive authentication
connectUser(server = "my-connect")
```

This will:
1. Open a web browser
2. Prompt you to log in to Posit Connect
3. Automatically register your account credentials

### API Key Authentication

For automation or non-interactive settings, use API key authentication:

```r
# Get your API key from Posit Connect:
# 1. Log in to Posit Connect
# 2. Click your username in the top-right corner
# 3. Select "API Keys"
# 4. Click "New API Key"

# Authenticate with API key
connectApiUser(
  server = "my-connect",
  apiKey = "YOUR_API_KEY_HERE"
)
```

This method:
- Works in non-interactive contexts
- Is ideal for CI/CD pipelines
- Requires manual API key management

## Authenticating with ShinyApps.io

For ShinyApps.io, use token-based authentication:

```r
# Get your token from https://www.shinyapps.io/admin/#/tokens
setAccountInfo(
  name = "username",
  token = "TOKEN",
  secret = "SECRET"
)
```

## Authenticating with Posit Cloud

For Posit Cloud:

```r
# Get your token from Posit Cloud → Account → Tokens
setAccountInfo(
  name = "username", 
  token = "TOKEN", 
  secret = "SECRET",
  server = "posit.cloud"
)
```

## Account Management

### Listing Accounts

View all authenticated accounts:

```r
# View all accounts
accounts()

# Filter by server
accounts(server = "my-connect")
```

### Account Information

Retrieve details about a specific account:

```r
accountInfo(name = "username", server = "my-connect")
```

### Removing Accounts

Remove an account from your local configuration:

```r
removeAccount(name = "username", server = "my-connect")
```

## Authentication Files

Your authentication credentials are stored in:

- **Windows**: `%APPDATA%/rsconnect/`
- **MacOS**: `~/.rsconnect/`
- **Linux**: `~/.config/rsconnect/`

Important security notes:
- These files contain sensitive authentication credentials
- They should never be committed to version control
- For team environments, each user should manage their own credentials

## Server Management

Register and manage Posit Connect servers:

```r
# Add a server
addServer(
  url = "https://connect.example.com/",
  name = "my-connect"
)

# List all servers
servers()

# Remove a server
removeServer(name = "my-connect")
```

## SSL Certificates

If your Posit Connect server uses a self-signed certificate:

```r
# Add server with certificate
addServer(
  url = "https://connect.example.com/",
  name = "my-connect",
  certificate = "/path/to/certificate.pem"
)

# Add certificate to existing server
addServerCertificate(
  name = "my-connect",
  certificate = "/path/to/certificate.pem"
)
```

## Best Practices

1. **API Keys**: Rotate API keys regularly
2. **Automation**: Use environment variables to store API keys in CI/CD pipelines
3. **Permissions**: Use the least privilege principle for API keys
4. **Shared Environments**: Each user should authenticate with their own account
5. **Security**: Never share or expose tokens, secrets, or API keys