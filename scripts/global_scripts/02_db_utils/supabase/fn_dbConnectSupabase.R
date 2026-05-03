#' Connect to Supabase PostgreSQL Database
#'
#' @description
#' Establishes a connection to Supabase PostgreSQL database for deployment.
#' Uses environment variables for secure credential management.
#'
#' @param host Character. Supabase database host. Default: from SUPABASE_DB_HOST env var
#' @param port Character. Database port. Default: "5432"
#' @param dbname Character. Database name. Default: "postgres"
#' @param user Character. Database user. Default: "postgres"
#' @param password Character. Database password. Default: from SUPABASE_DB_PASSWORD env var
#' @param verbose Logical. Print connection status messages. Default: TRUE
#'
#' @return DBI connection object to Supabase PostgreSQL
#'
#' @details
#' Required environment variables:
#'   - SUPABASE_DB_HOST: Database host (e.g., db.xxxxx.supabase.co)
#'   - SUPABASE_DB_PASSWORD: Database password
#'
#' Optional environment variables:
#'   - SUPABASE_DB_PORT: Default "5432"
#'   - SUPABASE_DB_NAME: Default "postgres"
#'   - SUPABASE_DB_USER: Default "postgres"
#'
#' Following Principles:
#'   - SEC_R001: Credential Management (no hardcoded credentials)
#'   - DM_R023: Universal DBI Approach
#'   - SO_R007: One Function One File
#'
#' @export
#' @importFrom DBI dbConnect
#' @use_package DBI
#' @use_package RPostgres

dbConnectSupabase <- function(
    host = NULL,
    port = NULL,
    dbname = NULL,
    user = NULL,
    password = NULL,
    verbose = TRUE
) {
  # Ensure RPostgres is available (must be pre-installed via manifest.json)
  if (!requireNamespace("RPostgres", quietly = TRUE)) {
    stop(
      "RPostgres package is not available!\n",
      "This package must be pre-installed in the deployment environment.\n",
      "Add library(RPostgres) to app.R to include it in manifest.json."
    )
  }

  # Get connection parameters from environment variables if not provided
  host <- host %||% Sys.getenv("SUPABASE_DB_HOST", "")
  port <- port %||% Sys.getenv("SUPABASE_DB_PORT", "5432")
  dbname <- dbname %||% Sys.getenv("SUPABASE_DB_NAME", "postgres")
  user <- user %||% Sys.getenv("SUPABASE_DB_USER", "postgres")
  password <- password %||% Sys.getenv("SUPABASE_DB_PASSWORD", "")

  # Validate required parameters
  if (host == "") {
    stop(
      "Supabase host not configured!\n",
      "Set SUPABASE_DB_HOST environment variable or pass 'host' parameter.\n",
      "Example: db.xxxxx.supabase.co"
    )
  }

  if (password == "") {
    stop(
      "Supabase password not configured!\n",
      "Set SUPABASE_DB_PASSWORD environment variable or pass 'password' parameter."
    )
  }

  if (verbose) {
    message("Connecting to Supabase PostgreSQL...")
    message("  Host: ", host)
    message("  Port: ", port)
    message("  Database: ", dbname)
    message("  User: ", user)
  }

  # Establish connection with SSL
  tryCatch({
    con <- DBI::dbConnect(
      RPostgres::Postgres(),
      host = host,
      port = as.integer(port),
      dbname = dbname,
      user = user,
      password = password,
      sslmode = "require"
    )

    # Add connection metadata
    attr(con, "connection_type") <- "supabase_postgres"
    attr(con, "connection_time") <- Sys.time()
    attr(con, "supabase_host") <- host

    if (verbose) {
      message("Successfully connected to Supabase PostgreSQL")
    }

    return(con)

  }, error = function(e) {
    stop(
      "Failed to connect to Supabase:\n",
      "  Error: ", e$message, "\n\n",
      "Troubleshooting:\n",
      "  1. Verify SUPABASE_DB_HOST is correct\n",
      "  2. Check SUPABASE_DB_PASSWORD is valid\n",
      "  3. Ensure your IP is allowed in Supabase Dashboard\n",
      "  4. Verify the database is not paused"
    )
  })
}

# Null coalescing operator (if not already defined)
`%||%` <- function(x, y) if (is.null(x) || x == "") y else x
