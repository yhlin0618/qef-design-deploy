#' Load Environment Variables from .env File
#'
#' @description
#' Loads environment variables from .env file with fail-fast mechanism.
#' Follows MAMBA standard pattern with dotenv package preference.
#'
#' **Security Principle Compliance**:
#' - Per MP110 (Security Credentials Management): Zero-tolerance for missing credentials
#' - Per SEC_R002 (Environment Variable Standards): Fail-fast when required vars missing
#' - Per MP029 (No Fake Data): No fake/test credentials as defaults
#' - Per MP004 Exception (Line 186): Security-sensitive settings require explicit values
#'
#' **Why Multiple Principles?**
#' - MP029 prevents using fake data as defaults
#' - SEC_R002 + MP110 enforce fail-fast when data is missing
#' - Together: Forces use of real credentials only
#'
#' @param app_dir Character. Project root directory. Default: current working directory
#' @param env_filename Character. Name of .env file. Default: ".env"
#' @param required_vars Character vector. Variables to verify. Default: c("OPENAI_API_KEY")
#' @param required Logical. If TRUE, fail if .env file is missing. Default: TRUE
#' @param silent Logical. Suppress messages. Default: FALSE
#'
#' @return Invisible list with status and loaded variables
#'
#' @examples
#' # Basic usage (fail-fast if .env missing)
#' load_env_file()
#'
#' # Custom location
#' load_env_file(app_dir = "/path/to/project")
#'
#' # Verify multiple variables
#' load_env_file(required_vars = c("OPENAI_API_KEY", "PGHOST", "PGPASSWORD"))
#'
#' # Optional .env file (non-security use case)
#' load_env_file(required = FALSE)
#'
#' @export
load_env_file <- function(app_dir = getwd(),
                          env_filename = ".env",
                          required_vars = c("OPENAI_API_KEY"),
                          required = TRUE,
                          silent = FALSE) {

  env_file <- file.path(app_dir, env_filename)
  result <- list(success = FALSE, method = NULL, loaded_vars = character(0), missing_vars = character(0))

  # Check if .env file exists - FAIL-FAST if required
  if (!file.exists(env_file)) {
    if (required) {
      # Per SEC_R002: Required environment file MUST exist
      stop(sprintf(
        "❌ Required environment file '%s' not found in '%s'.\n\nAction required:\n1. Copy .env.template to .env\n2. Fill in all required credentials\n3. Ensure .env is in .gitignore\n\nSee: SEC_R002, MP110 for security requirements.",
        env_filename,
        app_dir
      ))
    } else {
      if (!silent) message("ℹ️  Optional .env file not found in ", app_dir)
      return(invisible(result))
    }
  }

  # Try dotenv first, fallback to readRenviron
  if (requireNamespace("dotenv", quietly = TRUE)) {
    tryCatch({
      dotenv::load_dot_env(file = env_file)
      result$method <- "dotenv"
      result$success <- TRUE
      if (!silent) message("✅ Environment variables loaded from .env using dotenv package")
    }, error = function(e) {
      if (!silent) warning("⚠️  Failed to load .env with dotenv: ", e$message)
    })
  }

  # Fallback to readRenviron if dotenv failed or unavailable
  if (!result$success) {
    tryCatch({
      readRenviron(env_file)
      result$method <- "readRenviron"
      result$success <- TRUE
      if (!silent) message("✅ Environment variables loaded from .env using readRenviron")
    }, error = function(e) {
      stop(sprintf(
        "❌ Failed to load .env file: %s\n\nCheck that .env file is properly formatted.",
        e$message
      ))
    })
  }

  # Verify required variables - FAIL-FAST if missing
  # Per SEC_R002: Required environment variables MUST be present
  if (result$success && length(required_vars) > 0) {
    missing <- character(0)

    for (var in required_vars) {
      value <- Sys.getenv(var)
      if (nzchar(value)) {
        result$loaded_vars <- c(result$loaded_vars, var)
        if (!silent) {
          # Mask sensitive values in logs
          masked <- paste0(substr(value, 1, min(4, nchar(value))), "***")
          message("  ✓ ", var, ": ", masked)
        }
      } else {
        missing <- c(missing, var)
        result$missing_vars <- c(result$missing_vars, var)
      }
    }

    # FAIL-FAST if any required variables are missing
    if (length(missing) > 0) {
      stop(sprintf(
        "❌ Missing required environment variables in .env file:\n  - %s\n\nAction required:\n1. Add these variables to .env file\n2. Refer to .env.template for correct format\n\nSee: SEC_R002 (Environment Variable Standards)",
        paste(missing, collapse = "\n  - ")
      ))
    }
  }

  if (!silent && result$success) {
    message(sprintf("✅ All %d required environment variables validated", length(required_vars)))
  }

  return(invisible(result))
}
