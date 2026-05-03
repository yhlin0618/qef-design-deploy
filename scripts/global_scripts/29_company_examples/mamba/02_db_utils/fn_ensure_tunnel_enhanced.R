# ==============================================================================
# Enhanced MAMBA SQL Server SSH Tunnel with Timeout and Progress Reporting
# Following MP099: Real-time Progress Reporting
# Following MP106: Console Output Transparency
# Following MP096: Data Storage Selection Strategy
# ==============================================================================
# Version: 2.0.0 - Enhanced with timeout handling and debugging
# 
# IMPROVEMENTS:
# - Added timeout handling for all blocking operations
# - Real-time progress reporting at each step
# - Detailed error messages with actionable solutions
# - Non-blocking SSH tunnel creation
# - Connection validation with retries
# ==============================================================================

# Helper function to run system commands with timeout
fn_system_with_timeout <- function(cmd, timeout_seconds = 10, verbose = TRUE) {
  if (verbose) {
    message(sprintf("⏱️  [%s] Executing command with %ds timeout...", 
                   format(Sys.time(), "%H:%M:%S"), timeout_seconds))
  }
  
  # Use timeout command on Unix systems
  if (Sys.info()["sysname"] != "Windows") {
    timeout_cmd <- sprintf("timeout %d %s", timeout_seconds, cmd)
  } else {
    # Windows doesn't have timeout command in the same way
    timeout_cmd <- cmd
  }
  
  # Capture both stdout and stderr for debugging
  result <- tryCatch({
    system2("bash", args = c("-c", timeout_cmd), 
            stdout = TRUE, stderr = TRUE, timeout = timeout_seconds)
  }, error = function(e) {
    if (verbose) message("❌ Command timed out or failed: ", e$message)
    return(list(status = 1, output = NULL, error = e$message))
  }, warning = function(w) {
    if (verbose) message("⚠️  Warning during command execution: ", w$message)
    return(list(status = 1, output = NULL, error = w$message))
  })
  
  return(result)
}

# Enhanced tunnel establishment function with progress reporting
fn_ensure_mamba_tunnel <- function(verbose = TRUE, 
                                   max_retries = 3,
                                   connection_timeout = 10,
                                   tunnel_wait_time = 3) {
  
  # MP099: Real-time progress reporting
  if (verbose) {
    message(strrep("=", 80))
    message(sprintf("🚀 [%s] MAMBA SSH TUNNEL MANAGER v2.0", format(Sys.time(), "%H:%M:%S")))
    message(strrep("=", 80))
  }
  
  # Step 1: Check existing tunnel
  if (verbose) message(sprintf("📍 [%s] Step 1/5: Checking for existing SSH tunnel...", 
                               format(Sys.time(), "%H:%M:%S")))
  
  check_cmd <- "ps aux | grep -E 'ssh.*1433.*125.227.84.85' | grep -v grep"
  existing <- system(check_cmd, intern = TRUE, ignore.stderr = TRUE)
  
  if (length(existing) > 0) {
    if (verbose) {
      message(sprintf("✅ [%s] SSH tunnel already running (PID found)", 
                     format(Sys.time(), "%H:%M:%S")))
      message("   Tunnel process: ", substr(existing[1], 1, 80))
    }
    return(TRUE)
  }
  
  if (verbose) message(sprintf("🔍 [%s] No existing tunnel found, proceeding to create...", 
                               format(Sys.time(), "%H:%M:%S")))
  
  # Step 2: Validate environment variables
  if (verbose) message(sprintf("📍 [%s] Step 2/5: Validating environment variables...", 
                               format(Sys.time(), "%H:%M:%S")))
  
  ssh_host <- Sys.getenv("EBY_SSH_HOST")
  ssh_user <- Sys.getenv("EBY_SSH_USER")
  ssh_password <- Sys.getenv("EBY_SSH_PASSWORD")
  sql_host <- Sys.getenv("EBY_SQL_HOST")
  sql_port <- Sys.getenv("EBY_SQL_PORT", "1433")
  local_port <- Sys.getenv("EBY_LOCAL_PORT", "1433")
  
  required_vars <- list(
    EBY_SSH_HOST = ssh_host,
    EBY_SSH_USER = ssh_user,
    EBY_SSH_PASSWORD = ssh_password,
    EBY_SQL_HOST = sql_host
  )
  
  missing_vars <- names(required_vars)[required_vars == ""]
  if (length(missing_vars) > 0) {
    message("❌ Missing required environment variables:")
    for (var in missing_vars) {
      message(sprintf("   - %s", var))
    }
    message("\n📝 SOLUTION: Add these to your .env file:")
    message("   EBY_SSH_HOST=220.128.138.146")
    message("   EBY_SSH_USER=your_username")
    message("   EBY_SSH_PASSWORD=your_password")
    message("   EBY_SQL_HOST=125.227.84.85")
    stop("Environment configuration incomplete")
  }
  
  if (verbose) {
    message(sprintf("✅ [%s] All environment variables validated", 
                   format(Sys.time(), "%H:%M:%S")))
    message(sprintf("   SSH Target: %s@%s", ssh_user, ssh_host))
    message(sprintf("   SQL Server: %s:%s -> localhost:%s", sql_host, sql_port, local_port))
  }
  
  # Step 3: Check sshpass availability
  if (verbose) message(sprintf("📍 [%s] Step 3/5: Checking SSH tools...", 
                               format(Sys.time(), "%H:%M:%S")))
  
  has_sshpass <- system("which sshpass", ignore.stdout = TRUE, ignore.stderr = TRUE) == 0
  
  if (!has_sshpass) {
    message("❌ sshpass not installed!")
    message("📝 SOLUTION: Install sshpass first:")
    message("   macOS: brew install hudochenkov/sshpass/sshpass")
    message("   Linux: sudo apt-get install sshpass")
    message("\n🔧 ALTERNATIVE: Create tunnel manually:")
    message(sprintf("   ssh -N -L %s:%s:%s %s@%s", 
                   local_port, sql_host, sql_port, ssh_user, ssh_host))
    stop("Required tool 'sshpass' not found")
  }
  
  if (verbose) message(sprintf("✅ [%s] sshpass found at: %s", 
                               format(Sys.time(), "%H:%M:%S"),
                               system("which sshpass", intern = TRUE)))
  
  # Step 4: Create SSH tunnel with retries
  if (verbose) message(sprintf("📍 [%s] Step 4/5: Creating SSH tunnel (max %d retries)...", 
                               format(Sys.time(), "%H:%M:%S"), max_retries))
  
  tunnel_established <- FALSE
  attempt <- 0
  
  while (!tunnel_established && attempt < max_retries) {
    attempt <- attempt + 1
    
    if (verbose) message(sprintf("🔄 [%s] Attempt %d/%d: Establishing tunnel...", 
                                format(Sys.time(), "%H:%M:%S"), attempt, max_retries))
    
    # Use nohup and & to run in background instead of -f flag
    # This prevents hanging on authentication issues
    tunnel_cmd <- sprintf(
      "nohup sshpass -p '%s' ssh -o ConnectTimeout=%d -o StrictHostKeyChecking=no -N -L %s:%s:%s %s@%s > /tmp/ssh_tunnel.log 2>&1 &",
      ssh_password, connection_timeout, local_port, sql_host, sql_port, ssh_user, ssh_host
    )
    
    # Execute tunnel command
    system(tunnel_cmd, wait = FALSE)
    
    # Wait for tunnel to establish
    if (verbose) message(sprintf("⏳ [%s] Waiting %d seconds for tunnel to establish...", 
                                format(Sys.time(), "%H:%M:%S"), tunnel_wait_time))
    
    # Progressive wait with status checks
    for (i in 1:tunnel_wait_time) {
      Sys.sleep(1)
      if (verbose) message(sprintf("   %d/%d seconds...", i, tunnel_wait_time))
      
      # Check if tunnel is up
      check_result <- system(check_cmd, intern = TRUE, ignore.stderr = TRUE)
      if (length(check_result) > 0) {
        tunnel_established <- TRUE
        break
      }
    }
    
    if (tunnel_established) {
      if (verbose) message(sprintf("✅ [%s] SSH tunnel established on attempt %d!", 
                                  format(Sys.time(), "%H:%M:%S"), attempt))
      break
    } else {
      if (verbose) {
        message(sprintf("⚠️  [%s] Attempt %d failed, tunnel not detected", 
                       format(Sys.time(), "%H:%M:%S"), attempt))
        
        # Check SSH log for errors
        log_file <- "/tmp/ssh_tunnel.log"
        if (file.exists(log_file)) {
          log_content <- readLines(log_file, n = 5, warn = FALSE)
          if (length(log_content) > 0) {
            message("   SSH Error log:")
            for (line in log_content) {
              message("   > ", line)
            }
          }
        }
      }
    }
  }
  
  # Step 5: Final validation
  if (verbose) message(sprintf("📍 [%s] Step 5/5: Final tunnel validation...", 
                               format(Sys.time(), "%H:%M:%S")))
  
  if (tunnel_established) {
    # Test port availability
    port_test <- sprintf("nc -z -v -w5 127.0.0.1 %s 2>&1", local_port)
    port_result <- system(port_test, intern = TRUE, ignore.stderr = FALSE)
    
    if (any(grepl("succeeded|open", port_result, ignore.case = TRUE))) {
      if (verbose) {
        message(sprintf("✅ [%s] Port %s is open and accepting connections", 
                       format(Sys.time(), "%H:%M:%S"), local_port))
        message(strrep("=", 80))
        message("🎉 SSH TUNNEL SUCCESSFULLY ESTABLISHED!")
        message(strrep("=", 80))
      }
      return(TRUE)
    } else {
      if (verbose) {
        message(sprintf("⚠️  [%s] Tunnel process exists but port %s not responding", 
                       format(Sys.time(), "%H:%M:%S"), local_port))
      }
    }
  }
  
  # Failed to establish tunnel
  if (verbose) {
    message(strrep("=", 80))
    message("❌ FAILED TO ESTABLISH SSH TUNNEL")
    message(strrep("=", 80))
    message("\n🔍 TROUBLESHOOTING STEPS:")
    message("1. Check network connectivity:")
    message(sprintf("   ping %s", ssh_host))
    message("2. Test SSH connection manually:")
    message(sprintf("   ssh %s@%s", ssh_user, ssh_host))
    message("3. Check firewall settings for port 22 and 1433")
    message("4. Verify credentials in .env file")
    message("5. Check SSH server logs on remote host")
  }
  
  return(FALSE)
}

# Enhanced connection function with timeout and progress reporting
fn_connect_mamba_sql <- function(auto_tunnel = TRUE, 
                                 verbose = TRUE,
                                 connection_timeout = 30) {
  
  if (verbose) {
    message(strrep("=", 80))
    message(sprintf("🔌 [%s] MAMBA SQL SERVER CONNECTION MANAGER", format(Sys.time(), "%H:%M:%S")))
    message(strrep("=", 80))
  }
  
  # Step 1: Ensure tunnel if requested
  if (auto_tunnel) {
    if (verbose) message(sprintf("📍 [%s] Step 1/3: Ensuring SSH tunnel...", 
                                format(Sys.time(), "%H:%M:%S")))
    
    tunnel_ok <- fn_ensure_mamba_tunnel(verbose = verbose)
    if (!tunnel_ok) {
      stop("❌ Could not establish SSH tunnel to MAMBA SQL Server")
    }
  }
  
  # Step 2: Validate database credentials
  if (verbose) message(sprintf("📍 [%s] Step 2/3: Validating database credentials...", 
                               format(Sys.time(), "%H:%M:%S")))
  
  db_name <- Sys.getenv("EBY_SQL_DATABASE")
  db_user <- Sys.getenv("EBY_SQL_USER")
  db_password <- Sys.getenv("EBY_SQL_PASSWORD")
  
  if (db_name == "" || db_user == "" || db_password == "") {
    missing <- c()
    if (db_name == "") missing <- c(missing, "EBY_SQL_DATABASE")
    if (db_user == "") missing <- c(missing, "EBY_SQL_USER")
    if (db_password == "") missing <- c(missing, "EBY_SQL_PASSWORD")
    
    message("❌ Missing database credentials:")
    for (var in missing) {
      message(sprintf("   - %s", var))
    }
    message("\n📝 SOLUTION: Add these to your .env file")
    stop("Database configuration incomplete")
  }
  
  if (verbose) {
    message(sprintf("✅ [%s] Database credentials validated", 
                   format(Sys.time(), "%H:%M:%S")))
    message(sprintf("   Database: %s", db_name))
    message(sprintf("   User: %s", db_user))
  }
  
  # Step 3: Connect to database with timeout
  if (verbose) message(sprintf("📍 [%s] Step 3/3: Connecting to SQL Server (timeout: %ds)...", 
                               format(Sys.time(), "%H:%M:%S"), connection_timeout))
  
  connection_string <- sprintf(
    "Driver={ODBC Driver 18 for SQL Server};Server=127.0.0.1,1433;Database=%s;Uid=%s;Pwd=%s;TrustServerCertificate=yes;Encrypt=no;Connection Timeout=%d",
    db_name, db_user, db_password, connection_timeout
  )
  
  # Attempt connection with timeout handling
  conn <- tryCatch({
    start_time <- Sys.time()
    
    conn <- DBI::dbConnect(
      odbc::odbc(),
      .connection_string = connection_string
    )
    
    elapsed <- round(difftime(Sys.time(), start_time, units = "secs"), 2)
    
    if (verbose) {
      message(sprintf("✅ [%s] Connected to SQL Server in %.2f seconds", 
                     format(Sys.time(), "%H:%M:%S"), elapsed))
      
      # Test connection with simple query
      test_result <- DBI::dbGetQuery(conn, "SELECT @@VERSION AS version")
      message("   SQL Server Version: ", substr(test_result$version[1], 1, 50), "...")
    }
    
    conn
  }, error = function(e) {
    if (verbose) {
      message(sprintf("❌ [%s] Failed to connect to SQL Server", 
                     format(Sys.time(), "%H:%M:%S")))
      message("   Error: ", e$message)
      message("\n🔍 TROUBLESHOOTING:")
      message("1. Check if tunnel is still active:")
      message("   ps aux | grep ssh")
      message("2. Test local port connectivity:")
      message("   telnet 127.0.0.1 1433")
      message("3. Verify ODBC driver installation:")
      message("   odbcinst -q -d")
      message("4. Check SQL Server credentials")
    }
    stop("Database connection failed: ", e$message)
  })
  
  if (verbose) {
    message(strrep("=", 80))
    message("🎉 SUCCESSFULLY CONNECTED TO MAMBA SQL SERVER!")
    message(strrep("=", 80))
  }
  
  return(conn)
}

# Enhanced disconnect function with cleanup
fn_disconnect_mamba_sql <- function(conn, close_tunnel = FALSE, verbose = TRUE) {
  
  if (verbose) {
    message(strrep("=", 80))
    message(sprintf("🔌 [%s] DISCONNECTING FROM MAMBA SQL SERVER", format(Sys.time(), "%H:%M:%S")))
    message(strrep("=", 80))
  }
  
  # Disconnect from database
  if (!is.null(conn)) {
    tryCatch({
      DBI::dbDisconnect(conn)
      if (verbose) message(sprintf("✅ [%s] Database connection closed", 
                                  format(Sys.time(), "%H:%M:%S")))
    }, error = function(e) {
      if (verbose) message(sprintf("⚠️  [%s] Error closing connection: %s", 
                                  format(Sys.time(), "%H:%M:%S"), e$message))
    })
  }
  
  # Optionally close SSH tunnel
  if (close_tunnel) {
    if (verbose) message(sprintf("🔄 [%s] Closing SSH tunnel...", 
                                format(Sys.time(), "%H:%M:%S")))
    
    kill_result <- system("pkill -f 'ssh.*1433.*125.227.84.85'", 
                          ignore.stdout = TRUE, ignore.stderr = TRUE)
    
    if (kill_result == 0) {
      if (verbose) message(sprintf("✅ [%s] SSH tunnel closed", 
                                  format(Sys.time(), "%H:%M:%S")))
    } else {
      if (verbose) message(sprintf("⚠️  [%s] No SSH tunnel found to close", 
                                  format(Sys.time(), "%H:%M:%S")))
    }
  } else {
    if (verbose) message(sprintf("ℹ️  [%s] SSH tunnel kept alive for other connections", 
                                format(Sys.time(), "%H:%M:%S")))
  }
  
  if (verbose) {
    message(strrep("=", 80))
  }
}

# Test function for debugging
fn_test_mamba_connection <- function() {
  message("\n🧪 TESTING MAMBA CONNECTION WITH FULL DIAGNOSTICS\n")
  
  # Test environment variables
  message("1️⃣ Checking environment variables...")
  required_vars <- c("EBY_SSH_HOST", "EBY_SSH_USER", "EBY_SSH_PASSWORD",
                     "EBY_SQL_HOST", "EBY_SQL_DATABASE", "EBY_SQL_USER", "EBY_SQL_PASSWORD")
  
  for (var in required_vars) {
    val <- Sys.getenv(var)
    if (val == "") {
      message(sprintf("   ❌ %s: NOT SET", var))
    } else {
      # Mask passwords
      if (grepl("PASSWORD", var)) {
        message(sprintf("   ✅ %s: ****** (set)", var))
      } else {
        message(sprintf("   ✅ %s: %s", var, val))
      }
    }
  }
  
  # Test network connectivity
  message("\n2️⃣ Testing network connectivity...")
  ssh_host <- Sys.getenv("EBY_SSH_HOST")
  if (ssh_host != "") {
    ping_result <- system(sprintf("ping -c 1 -W 2 %s", ssh_host), 
                          ignore.stdout = TRUE, ignore.stderr = TRUE)
    if (ping_result == 0) {
      message(sprintf("   ✅ Can reach SSH host: %s", ssh_host))
    } else {
      message(sprintf("   ❌ Cannot reach SSH host: %s", ssh_host))
    }
  }
  
  # Test SSH tools
  message("\n3️⃣ Checking SSH tools...")
  has_ssh <- system("which ssh", ignore.stdout = TRUE, ignore.stderr = TRUE) == 0
  has_sshpass <- system("which sshpass", ignore.stdout = TRUE, ignore.stderr = TRUE) == 0
  
  message(sprintf("   %s ssh: %s", 
                 ifelse(has_ssh, "✅", "❌"),
                 ifelse(has_ssh, system("which ssh", intern = TRUE), "NOT FOUND")))
  message(sprintf("   %s sshpass: %s", 
                 ifelse(has_sshpass, "✅", "❌"),
                 ifelse(has_sshpass, system("which sshpass", intern = TRUE), "NOT FOUND")))
  
  # Test ODBC driver
  message("\n4️⃣ Checking ODBC driver...")
  drivers <- odbc::odbcListDrivers()
  sql_driver <- drivers[grepl("SQL Server", drivers$name, ignore.case = TRUE), ]
  
  if (nrow(sql_driver) > 0) {
    message("   ✅ SQL Server ODBC driver found:")
    for (i in 1:nrow(sql_driver)) {
      message(sprintf("      - %s", sql_driver$name[i]))
    }
  } else {
    message("   ❌ No SQL Server ODBC driver found")
    message("   Install with: brew install microsoft/mssql-release/msodbcsql18")
  }
  
  message("\n5️⃣ Test complete!")
}