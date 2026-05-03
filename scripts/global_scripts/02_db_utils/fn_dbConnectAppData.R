#' Connect to the application database
#'
#' @description
#' Establishes a connection to the app_data database.
#' Supports configuration-driven mode selection: DuckDB locally, Supabase PostgreSQL on deployment.
#'
#' @param db_path Path to the DuckDB database file (overrides config if provided)
#' @param use_supabase Deprecated. Use app_config.yaml database.mode instead.
#' @param config_path Path to app_config.yaml. Default: "app_config.yaml"
#' @param verbose Logical. Print connection status messages. Default: TRUE
#'
#' @return A database connection object with "connection_type" attribute
#'
#' @details
#' Connection mode is determined by app_config.yaml > database.mode:
#'   - "duckdb": Force DuckDB, error if file invalid
#'   - "supabase": Force Supabase, error if env vars missing
#'   - "auto": DuckDB if valid file exists, else Supabase
#'
#' Following Principles:
#'   - MP142: Configuration-Driven Pipeline
#'   - MP45: Automatic Data Availability Detection
#'   - DM_R023: Universal DBI Approach
#'   - DM_R056: Posit Connect Deployment Assets
#'   - SEC_R001: Credential Management
#'
#' @export
dbConnectAppData <- function(
    db_path = NULL,
    use_supabase = NULL,
    config_path = "app_config.yaml",
    verbose = TRUE
) {
  # Null coalescing operator
  `%||%` <- function(x, y) if (is.null(x) || (is.character(x) && x == "")) y else x

  # 1. Load configuration
  # When db_path is explicitly provided, config is optional (use defaults).
  # This supports Shiny server contexts where wd differs from project root.
  db_config <- list(mode = "auto")
  if (file.exists(config_path)) {
    tryCatch({
      config <- yaml::read_yaml(config_path)
      if (!is.null(config$database)) {
        db_config <- config$database
      }
    }, error = function(e) {
      stop("Unable to read app config for DB selection: ", e$message)
    })
  } else if (is.null(db_path)) {
    # Config required when no explicit db_path — cannot determine where DB is
    stop("Required config file not found: ", config_path)
  } else if (verbose) {
    message("[dbConnectAppData] Config not found at '", config_path,
            "'; using defaults with provided db_path")
  }

  # 2. Determine mode (explicit parameter overrides config)
  if (!is.null(use_supabase)) {
    # Deprecated parameter provided - use for backward compatibility
    if (verbose) {
      message("Note: 'use_supabase' parameter is deprecated. Use app_config.yaml database.mode instead.")
    }
    mode <- if (use_supabase) "supabase" else "duckdb"
  } else {
    mode <- db_config$mode %||% "auto"
  }

  # 3. Determine DuckDB path
  duckdb_path <- db_path %||% (db_config$duckdb$path %||% "data/app_data/app_data.duckdb")
  read_only <- db_config$duckdb$read_only %||% TRUE

  # 4. Check DuckDB validity
duckdb_valid <- FALSE
  if (file.exists(duckdb_path)) {
    file_size <- file.info(duckdb_path)$size
    # LFS pointer files are ~130-150 bytes; valid DuckDB should be >1KB
    if (!is.na(file_size) && file_size > 1000) {
      duckdb_valid <- TRUE
    } else if (verbose) {
      message("DuckDB file appears to be LFS pointer (", file_size, " bytes)")
    }
  }

  # 5. Check Supabase availability
  supabase_host <- Sys.getenv("SUPABASE_DB_HOST", "")
  supabase_password <- Sys.getenv("SUPABASE_DB_PASSWORD", "")
  supabase_available <- supabase_host != "" && supabase_password != ""

  # 6. Connect based on mode
  if (mode == "duckdb") {
    # Forced DuckDB mode
    if (!duckdb_valid) {
      stop(
        "DuckDB mode requested but file not available!\n",
        "  Path: ", duckdb_path, "\n",
        "  Status: ", if (file.exists(duckdb_path)) "exists but invalid (LFS pointer?)" else "not found", "\n\n",
        "Solutions:\n",
        "  1. Run 'git lfs pull' to fetch the actual DuckDB file\n",
        "  2. Change database.mode to 'auto' or 'supabase' in app_config.yaml"
      )
    }
    return(connect_duckdb(duckdb_path, read_only, verbose))

  } else if (mode == "supabase") {
    # Forced Supabase mode
    if (!supabase_available) {
      stop(
        "Supabase mode requested but credentials not configured!\n",
        "  SUPABASE_DB_HOST: ", if (supabase_host == "") "NOT SET" else "set", "\n",
        "  SUPABASE_DB_PASSWORD: ", if (supabase_password == "") "NOT SET" else "set", "\n\n",
        "Solutions:\n",
        "  1. Set environment variables in .env file or shell\n",
        "  2. Change database.mode to 'auto' or 'duckdb' in app_config.yaml"
      )
    }
    return(connect_supabase(verbose))

  } else {
    # Auto mode (default)
    if (duckdb_valid) {
      return(connect_duckdb(duckdb_path, read_only, verbose))
    } else if (supabase_available) {
      return(connect_supabase(verbose))
    } else {
      stop(
        "No database backend available!\n",
        "  DuckDB: ", duckdb_path, " (", if (file.exists(duckdb_path)) "exists but invalid" else "not found", ")\n",
        "  Supabase: credentials not configured\n\n",
        "Solutions:\n",
        "  1. For local dev: Run 'git lfs pull' to get app_data.duckdb\n",
        "  2. For deployment: Set SUPABASE_DB_HOST and SUPABASE_DB_PASSWORD env vars"
      )
    }
  }
}

#' Internal: Connect to DuckDB
#' @keywords internal
connect_duckdb <- function(db_path, read_only = TRUE, verbose = TRUE) {
  if (!requireNamespace("DBI", quietly = TRUE)) {
    stop("DBI package is required but not installed.")
  }
  if (!requireNamespace("duckdb", quietly = TRUE)) {
    stop("duckdb package is required but not installed.")
  }

  if (verbose) {
    message("\U0001F4E6 Connecting to DuckDB: ", db_path)
    message("  Mode: ", if (read_only) "read-only" else "read-write")
  }

  conn <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = read_only)
  attr(conn, "connection_type") <- "duckdb"
  attr(conn, "connection_time") <- Sys.time()
  attr(conn, "db_path") <- db_path

  if (verbose) {
    message("\U0001F4E6 Connected to DuckDB successfully")
  }

  return(conn)
}

#' Internal: Connect to Supabase
#' @keywords internal
connect_supabase <- function(verbose = TRUE) {
  # Source the Supabase connection function if not available
  if (!exists("dbConnectSupabase", mode = "function")) {
    supabase_fn_path <- "scripts/global_scripts/02_db_utils/supabase/fn_dbConnectSupabase.R"
    if (file.exists(supabase_fn_path)) {
      source(supabase_fn_path)
    } else {
      stop("Supabase connection function not found: ", supabase_fn_path)
    }
  }

  if (verbose) {
    message("\U00002601\UFE0F Connecting to Supabase PostgreSQL...")
  }

  conn <- dbConnectSupabase(verbose = verbose)
  attr(conn, "connection_type") <- "supabase"

  return(conn)
}
