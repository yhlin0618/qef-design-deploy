# ==============================================================================
# Function: Ensure MAMBA SQL Server SSH Tunnel
# Purpose: Automatically establish SSH tunnel if not already running
# Following MP096: Data Storage Selection Strategy
# 
# SECURITY: This function requires proper environment variables to be set.
# NEVER hardcode passwords in source code!
# 
# Required environment variables:
#   - EBY_SSH_HOST: SSH server hostname
#   - EBY_SSH_USER: SSH username
#   - EBY_SSH_PASSWORD: SSH password (use .env file or secure vault)
#   - EBY_SQL_HOST: SQL Server hostname
#   - EBY_SQL_PORT: SQL Server port
#   - EBY_LOCAL_PORT: Local port for tunnel
# ==============================================================================

fn_ensure_mamba_tunnel <- function(verbose = TRUE) {
  # Check if tunnel already exists
  check_cmd <- "ps aux | grep -E 'ssh.*1433.*125.227.84.85' | grep -v grep"
  existing <- system(check_cmd, intern = TRUE, ignore.stderr = TRUE)
  
  if (length(existing) > 0) {
    if (verbose) message("✅ SSH tunnel already running")
    return(TRUE)
  }
  
  if (verbose) message("🔄 No SSH tunnel found, establishing connection...")
  
  # Get credentials from environment - NO DEFAULTS WITH PASSWORDS!
  ssh_host <- Sys.getenv("EBY_SSH_HOST")
  ssh_user <- Sys.getenv("EBY_SSH_USER")
  ssh_password <- Sys.getenv("EBY_SSH_PASSWORD")
  sql_host <- Sys.getenv("EBY_SQL_HOST")
  sql_port <- Sys.getenv("EBY_SQL_PORT", "1433")
  local_port <- Sys.getenv("EBY_LOCAL_PORT", "1433")
  
  # Validate required environment variables
  required_vars <- list(
    EBY_SSH_HOST = ssh_host,
    EBY_SSH_USER = ssh_user,
    EBY_SSH_PASSWORD = ssh_password,
    EBY_SQL_HOST = sql_host
  )
  
  missing_vars <- names(required_vars)[required_vars == ""]
  if (length(missing_vars) > 0) {
    stop(paste0(
      "❌ Missing required environment variables: ", 
      paste(missing_vars, collapse = ", "),
      "\n\nPlease set these variables in your .env file or environment.",
      "\nSee documentation for secure configuration methods."
    ))
  }
  
  # Check if sshpass is available
  has_sshpass <- system("which sshpass", ignore.stdout = TRUE, ignore.stderr = TRUE) == 0
  
  if (has_sshpass) {
    # Use sshpass for automatic password entry
    # Add timeout and verbose options for better debugging
    tunnel_cmd <- sprintf(
      "sshpass -p '%s' ssh -o ConnectTimeout=10 -o StrictHostKeyChecking=no -f -N -L %s:%s:%s %s@%s",
      ssh_password, local_port, sql_host, sql_port, ssh_user, ssh_host
    )
    
    if (verbose) {
      message(sprintf("🔐 Attempting SSH connection to %s@%s...", ssh_user, ssh_host))
      message(sprintf("   Local port: %s -> Remote: %s:%s", local_port, sql_host, sql_port))
    }
    
    # Capture error output for debugging
    result <- system(tunnel_cmd, ignore.stdout = FALSE, ignore.stderr = FALSE)
    
    if (result == 0) {
      Sys.sleep(2)  # Wait for tunnel to establish
      
      # Verify tunnel is running
      check_again <- system(check_cmd, intern = TRUE, ignore.stderr = TRUE)
      if (length(check_again) > 0) {
        if (verbose) message("✅ SSH tunnel established automatically")
        return(TRUE)
      } else {
        if (verbose) message("⚠️ SSH command succeeded but tunnel process not found")
      }
    } else {
      if (verbose) {
        message(sprintf("❌ SSH connection failed with exit code: %d", result))
        message("   Possible causes:")
        message("   - Network connectivity issue (firewall, VPN required?)")
        message("   - Invalid credentials")
        message("   - SSH server not reachable")
        message(sprintf("   - Try manual connection: ssh %s@%s", ssh_user, ssh_host))
      }
    }
  } else {
    if (verbose) {
      message("⚠️ sshpass not installed. Install with: brew install hudochenkov/sshpass/sshpass")
      message("   Or manually create tunnel with:")
      message(sprintf("   ssh -N -L %s:%s:%s %s@%s", 
                     local_port, sql_host, sql_port, ssh_user, ssh_host))
      message("   Note: You will be prompted for the SSH password")
    }
  }
  
  # Final check
  final_check <- system(check_cmd, intern = TRUE, ignore.stderr = TRUE)
  if (length(final_check) > 0) {
    if (verbose) message("✅ SSH tunnel is now running")
    return(TRUE)
  } else {
    if (verbose) message("❌ Failed to establish SSH tunnel")
    return(FALSE)
  }
}

# Helper function to connect to MAMBA SQL Server
# 
# SECURITY: Requires proper environment variables:
#   - EBY_SQL_DATABASE: Database name
#   - EBY_SQL_USER: SQL Server username
#   - EBY_SQL_PASSWORD: SQL Server password (use .env file or secure vault)
fn_connect_mamba_sql <- function(auto_tunnel = TRUE) {
  # Ensure tunnel is running if requested
  if (auto_tunnel) {
    tunnel_ok <- fn_ensure_mamba_tunnel()
    if (!tunnel_ok) {
      stop("Could not establish SSH tunnel to MAMBA SQL Server")
    }
  }
  
  # Get database credentials - NO DEFAULTS WITH PASSWORDS!
  db_name <- Sys.getenv("EBY_SQL_DATABASE")
  db_user <- Sys.getenv("EBY_SQL_USER")
  db_password <- Sys.getenv("EBY_SQL_PASSWORD")
  
  # Validate required database credentials
  if (db_name == "" || db_user == "" || db_password == "") {
    missing <- c()
    if (db_name == "") missing <- c(missing, "EBY_SQL_DATABASE")
    if (db_user == "") missing <- c(missing, "EBY_SQL_USER")
    if (db_password == "") missing <- c(missing, "EBY_SQL_PASSWORD")
    
    stop(paste0(
      "❌ Missing required database credentials: ",
      paste(missing, collapse = ", "),
      "\n\nPlease set these variables in your .env file or environment.",
      "\nNEVER hardcode passwords in source code!"
    ))
  }
  
  # Create connection
  conn <- DBI::dbConnect(
    odbc::odbc(),
    .connection_string = sprintf(
      "Driver={ODBC Driver 18 for SQL Server};Server=127.0.0.1,1433;Database=%s;Uid=%s;Pwd=%s;TrustServerCertificate=yes;Encrypt=no",
      db_name, db_user, db_password
    )
  )
  
  return(conn)
}

# Helper function to safely disconnect and close tunnel
fn_disconnect_mamba_sql <- function(conn, close_tunnel = FALSE) {
  # Disconnect from database
  if (!is.null(conn)) {
    DBI::dbDisconnect(conn)
  }
  
  # Optionally close SSH tunnel
  if (close_tunnel) {
    system("pkill -f 'ssh.*1433.*125.227.84.85'", ignore.stdout = TRUE, ignore.stderr = TRUE)
    message("SSH tunnel closed")
  }
}