#' Connect to Cyberbiz API
#'
#' Establishes a connection to the Cyberbiz e-commerce platform API
#' following MP094 platform API architecture standards.
#'
#' @param config Optional configuration list. If NULL, reads from api_config.yaml
#' @param verbose Logical, whether to print connection status messages
#' 
#' @return API connection object with class "api_connection" and "cbz_connection"
#' 
#' @examples
#' \dontrun{
#' # Connect using default configuration
#' conn <- fn_cbz_connect()
#' 
#' # Connect with custom config
#' custom_config <- yaml::read_yaml("custom_config.yaml")
#' conn <- fn_cbz_connect(config = custom_config)
#' 
#' # Don't forget to disconnect when done
#' fn_cbz_disconnect(conn)
#' }
#' 
#' @export
fn_cbz_connect <- function(config = NULL, verbose = TRUE) {
  # Load configuration if not provided
  if (is.null(config)) {
    config_file <- file.path(
      dirname(sys.frame(1)$ofile %||% getwd()),
      "api_config.yaml"
    )
    
    if (!file.exists(config_file)) {
      # Try to find config in standard location
      config_file <- "scripts/global_scripts/26_platform_apis/cbz/api_config.yaml"
    }
    
    if (!file.exists(config_file)) {
      stop("Cannot find Cyberbiz API configuration file")
    }
    
    config <- yaml::read_yaml(config_file)
  }
  
  # Validate platform_id
  if (config$platform$code != "cbz") {
    warning("Configuration platform_id is not 'cbz'")
  }
  
  # Get authentication token from environment
  token_env_var <- config$authentication$bearer_token$env_var
  api_token <- Sys.getenv(token_env_var)
  
  if (api_token == "") {
    stop(sprintf(
      "Cyberbiz API token not found. Please set environment variable: %s\n",
      token_env_var
    ))
  }
  
  # Load rate limiter
  source("scripts/global_scripts/26_platform_apis/common/fn_api_rate_limiter.R")
  
  # Create connection object
  connection <- list(
    platform_id = config$platform$code,
    platform_name = config$platform$name,
    base_url = config$api$base_url,
    auth_header = paste(
      config$authentication$bearer_token$prefix,
      api_token
    ),
    auth_type = config$api$auth_type,
    rate_limiter = fn_create_rate_limiter(config),
    config = config,
    authenticated = TRUE,
    connected_at = Sys.time(),
    request_count = 0
  )
  
  # Add class for method dispatch
  class(connection) <- c("cbz_connection", "api_connection")
  
  if (verbose) {
    message(sprintf(
      "✓ Connected to %s API at %s",
      config$platform$name,
      config$api$base_url
    ))
  }
  
  return(connection)
}

#' Disconnect from Cyberbiz API
#'
#' Cleanly disconnects from Cyberbiz API and cleans up resources.
#'
#' @param connection API connection object from fn_cbz_connect()
#' @param verbose Logical, whether to print disconnection message
#' 
#' @return NULL (invisibly)
#' 
#' @examples
#' \dontrun{
#' conn <- fn_cbz_connect()
#' # ... use connection ...
#' fn_cbz_disconnect(conn)
#' }
#' 
#' @export
fn_cbz_disconnect <- function(connection, verbose = TRUE) {
  if (!inherits(connection, "cbz_connection")) {
    warning("Not a valid Cyberbiz connection object")
    return(invisible(NULL))
  }
  
  # Calculate session statistics
  session_duration <- difftime(
    Sys.time(), 
    connection$connected_at, 
    units = "mins"
  )
  
  if (verbose) {
    message(sprintf(
      "✓ Disconnected from %s API (Session: %.1f min, Requests: %d)",
      connection$platform_name,
      session_duration,
      connection$request_count
    ))
  }
  
  # Clean up connection object
  rm(connection, envir = parent.frame())
  
  invisible(NULL)
}

#' Check Cyberbiz Connection Status
#'
#' Verifies that a Cyberbiz API connection is valid and active.
#'
#' @param connection API connection object
#' 
#' @return Logical indicating if connection is valid
#' 
#' @export
fn_cbz_check_connection <- function(connection) {
  if (!inherits(connection, "cbz_connection")) {
    return(FALSE)
  }
  
  if (!connection$authenticated) {
    return(FALSE)
  }
  
  # Could add actual API ping here if needed
  # response <- fn_cbz_ping(connection)
  
  return(TRUE)
}

#' Get Cyberbiz Connection Info
#'
#' Returns information about the current connection.
#'
#' @param connection API connection object
#' 
#' @return List with connection details
#' 
#' @export
fn_cbz_connection_info <- function(connection) {
  if (!inherits(connection, "cbz_connection")) {
    stop("Not a valid Cyberbiz connection object")
  }
  
  # Load rate limiter functions
  source("scripts/global_scripts/26_platform_apis/common/fn_api_rate_limiter.R")
  
  info <- list(
    platform = connection$platform_name,
    platform_id = connection$platform_id,
    base_url = connection$base_url,
    connected_at = connection$connected_at,
    session_duration = difftime(Sys.time(), connection$connected_at, units = "mins"),
    request_count = connection$request_count,
    authenticated = connection$authenticated,
    rate_limit_status = fn_rate_limiter_status(connection$rate_limiter)
  )
  
  return(info)
}

# Helper operator for NULL default values
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}