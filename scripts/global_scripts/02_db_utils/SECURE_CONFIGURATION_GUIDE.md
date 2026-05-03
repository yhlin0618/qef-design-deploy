# Secure Configuration Guide for MAMBA SQL Server Connection

## 🔒 Security First

**CRITICAL**: Never hardcode passwords or sensitive credentials in source code. This guide explains how to securely configure the MAMBA SQL Server tunnel connection.

## Required Environment Variables

The following environment variables must be set for the MAMBA SQL Server connection to work:

### SSH Tunnel Configuration
- `EBY_SSH_HOST`: SSH server hostname
- `EBY_SSH_USER`: SSH username  
- `EBY_SSH_PASSWORD`: SSH password
- `EBY_SQL_HOST`: SQL Server hostname (internal network)
- `EBY_SQL_PORT`: SQL Server port (default: 1433)
- `EBY_LOCAL_PORT`: Local port for tunnel (default: 1433)

### SQL Server Database Configuration
- `EBY_SQL_DATABASE`: Database name
- `EBY_SQL_USER`: SQL Server username
- `EBY_SQL_PASSWORD`: SQL Server password

## Configuration Methods (Choose One)

### Method 1: Using .env File (Recommended for Development)

1. Create a `.env` file in your project root:
```bash
touch .env
chmod 600 .env  # Restrict file permissions
```

2. Add your credentials to `.env`:
```bash
# SSH Tunnel Configuration
EBY_SSH_HOST=your_ssh_host
EBY_SSH_USER=your_ssh_username
EBY_SSH_PASSWORD=your_ssh_password
EBY_SQL_HOST=your_sql_host
EBY_SQL_PORT=1433
EBY_LOCAL_PORT=1433

# SQL Server Configuration
EBY_SQL_DATABASE=your_database_name
EBY_SQL_USER=your_sql_username
EBY_SQL_PASSWORD=your_sql_password
```

3. Load the .env file in your R session:
```r
# Use dotenv package to load environment variables
library(dotenv)
dotenv::load_dot_env()

# Or use autoinit() if available
source("scripts/global_scripts/22_initializations/sc_initialization_update_mode.R")
autoinit()
```

4. **IMPORTANT**: Add `.env` to your `.gitignore`:
```bash
echo ".env" >> .gitignore
```

### Method 2: System Environment Variables (Recommended for Production)

Set environment variables in your shell profile or system:

#### macOS/Linux:
```bash
# Add to ~/.bashrc, ~/.zshrc, or ~/.bash_profile
export EBY_SSH_HOST="your_ssh_host"
export EBY_SSH_USER="your_ssh_username"
export EBY_SSH_PASSWORD="your_ssh_password"
export EBY_SQL_HOST="your_sql_host"
export EBY_SQL_DATABASE="your_database_name"
export EBY_SQL_USER="your_sql_username"
export EBY_SQL_PASSWORD="your_sql_password"

# Reload your shell
source ~/.bashrc  # or ~/.zshrc
```

#### Windows:
Use System Properties > Environment Variables or PowerShell:
```powershell
[System.Environment]::SetEnvironmentVariable("EBY_SSH_HOST", "your_ssh_host", "User")
[System.Environment]::SetEnvironmentVariable("EBY_SSH_USER", "your_ssh_username", "User")
# ... repeat for all variables
```

### Method 3: RStudio Environment Variables

1. Create/edit `.Renviron` in your project:
```r
usethis::edit_r_environ("project")
```

2. Add variables:
```
EBY_SSH_HOST=your_ssh_host
EBY_SSH_USER=your_ssh_username
EBY_SSH_PASSWORD=your_ssh_password
# ... add all required variables
```

3. Restart R session for changes to take effect

### Method 4: Secure Vault (Production Best Practice)

For production environments, use a secure vault service:
- AWS Secrets Manager
- Azure Key Vault
- HashiCorp Vault
- Kubernetes Secrets

Example with AWS Secrets Manager:
```r
library(aws.signature)
library(jsonlite)

# Retrieve secrets from AWS
secrets <- aws.signature::get_secret("mamba-sql-credentials")
credentials <- fromJSON(secrets)

# Set environment variables
Sys.setenv(
  EBY_SSH_HOST = credentials$ssh_host,
  EBY_SSH_USER = credentials$ssh_user,
  EBY_SSH_PASSWORD = credentials$ssh_password,
  EBY_SQL_DATABASE = credentials$sql_database,
  EBY_SQL_USER = credentials$sql_user,
  EBY_SQL_PASSWORD = credentials$sql_password
)
```

## Usage Example

Once environment variables are properly configured:

```r
# Source the tunnel function
source("scripts/global_scripts/02_db_utils/fn_ensure_mamba_tunnel.R")

# Connect to MAMBA SQL Server (automatically establishes SSH tunnel)
conn <- fn_connect_mamba_sql(auto_tunnel = TRUE)

# Use the connection
result <- DBI::dbGetQuery(conn, "SELECT * FROM your_table LIMIT 10")

# Disconnect when done
fn_disconnect_mamba_sql(conn, close_tunnel = FALSE)
```

## Security Checklist

- [ ] Never commit `.env` files to version control
- [ ] Never hardcode passwords in source code
- [ ] Use strong, unique passwords
- [ ] Restrict file permissions on credential files (chmod 600)
- [ ] Rotate credentials regularly
- [ ] Use different credentials for development/staging/production
- [ ] Enable audit logging for database access
- [ ] Use encrypted connections (SSH tunnel provides this)
- [ ] Follow principle of least privilege for database users
- [ ] Consider using SSH keys instead of passwords where possible

## Troubleshooting

### Missing Environment Variables Error
If you see "Missing required environment variables", check:
1. Variables are properly set in your chosen method
2. R session has been restarted after setting variables
3. Variable names match exactly (case-sensitive)

### SSH Tunnel Issues
1. Install `sshpass` for automatic password entry:
   ```bash
   # macOS
   brew install hudochenkov/sshpass/sshpass
   
   # Linux
   sudo apt-get install sshpass
   ```

2. Or create tunnel manually:
   ```bash
   ssh -N -L 1433:sql_server_host:1433 user@ssh_host
   ```

### Connection Refused
- Verify SSH tunnel is running: `ps aux | grep ssh`
- Check firewall rules allow connections
- Verify SQL Server is listening on the correct port

## Additional Security Resources

- [OWASP Database Security Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Database_Security_Cheat_Sheet.html)
- [R Security Best Practices](https://solutions.rstudio.com/admin/security/)
- [Environment Variable Security](https://12factor.net/config)

## Contact

For security concerns or questions about credential management, contact your system administrator or security team.

---

**Remember**: Security is everyone's responsibility. Never share credentials, always use secure methods, and report any suspected security issues immediately.