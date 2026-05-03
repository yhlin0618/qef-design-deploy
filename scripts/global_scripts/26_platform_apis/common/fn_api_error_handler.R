#' Handle API Errors with Retry Logic
#'
#' Provides standardized error handling for platform API calls following
#' MP094 architecture standards.
#'
#' @param error The error object caught
#' @param connection API connection object
#' @param request Request details (URL, method, etc.)
#' @param config Optional configuration override
#' @param attempt Current attempt number (for retry logic)
#' 
#' @return Either retries the request or stops with informative error
#' 
#' @examples
#' \dontrun{
#' response <- tryCatch({
#'   httr::GET(url)
#' }, error = function(e) {
#'   fn_handle_api_error(e, connection, request)
#' })
#' }
#' 
#' @export
fn_handle_api_error <- function(error, connection, request, config = NULL, attempt = 1) {
  # Use connection config if not provided
  if (is.null(config)) {
    config <- connection$config
  }
  
  # Get error handling configuration
  error_config <- config$error_handling
  max_retries <- error_config$retry_count %||% 3
  backoff_strategy <- error_config$backoff_strategy %||% "exponential"
  
  # Log the error
  fn_log_api_error(
    platform = connection$platform_id,
    endpoint = request$url,
    error = error,
    attempt = attempt
  )
  
  # Determine if error is retryable
  is_retryable <- fn_is_retryable_error(error, error_config)
  
  if (is_retryable && attempt <= max_retries) {
    # Calculate wait time based on backoff strategy
    wait_time <- fn_calculate_backoff(
      attempt = attempt,
      strategy = backoff_strategy,
      initial_wait = error_config$initial_wait %||% 1,
      max_wait = error_config$max_wait %||% 60
    )
    
    message(sprintf(
      "API error (attempt %d/%d). Retrying in %.1f seconds...",
      attempt, max_retries, wait_time
    ))
    
    Sys.sleep(wait_time)
    
    # Return signal to retry
    return(list(
      retry = TRUE,
      attempt = attempt + 1,
      wait_time = wait_time
    ))
  }
  
  # Transform error for user
  user_message <- fn_format_api_error(error, connection, request)
  
  # Stop with formatted error
  stop(user_message, call. = FALSE)
}

#' Check if Error is Retryable
#'
#' Determines if an error should trigger a retry attempt.
#'
#' @param error Error object
#' @param config Error handling configuration
#' 
#' @return Logical indicating if error is retryable
#' 
#' @export
fn_is_retryable_error <- function(error, config) {
  error_message <- as.character(error)
  
  # Check for HTTP status codes
  status_pattern <- "HTTP (\\d{3})"
  if (grepl(status_pattern, error_message)) {
    status_code <- as.numeric(gsub(".*HTTP (\\d{3}).*", "\\1", error_message))
    
    # Check if status code is in retry list
    retry_codes <- config$retry_on_status %||% c(429, 500, 502, 503, 504)
    if (status_code %in% retry_codes) {
      return(TRUE)
    }
    
    # Don't retry client errors (4xx except 429)
    if (status_code >= 400 && status_code < 500) {
      return(FALSE)
    }
  }
  
  # Check for connection errors
  connection_errors <- c(
    "timeout", "timed out",
    "connection refused",
    "couldn't connect",
    "network is unreachable",
    "connection reset"
  )
  
  for (pattern in connection_errors) {
    if (grepl(pattern, error_message, ignore.case = TRUE)) {
      return(TRUE)
    }
  }
  
  # Default to not retrying
  return(FALSE)
}

#' Calculate Backoff Wait Time
#'
#' Calculates wait time based on backoff strategy.
#'
#' @param attempt Current attempt number
#' @param strategy "linear" or "exponential"
#' @param initial_wait Initial wait time in seconds
#' @param max_wait Maximum wait time in seconds
#' 
#' @return Wait time in seconds
#' 
#' @export
fn_calculate_backoff <- function(attempt, strategy = "exponential", 
                                initial_wait = 1, max_wait = 60) {
  wait_time <- switch(
    strategy,
    "linear" = initial_wait * attempt,
    "exponential" = initial_wait * (2^(attempt - 1)),
    initial_wait  # Default
  )
  
  # Add jitter to prevent thundering herd
  jitter <- runif(1, 0, 0.1 * wait_time)
  wait_time <- wait_time + jitter
  
  # Cap at maximum wait time
  return(min(wait_time, max_wait))
}

#' Format API Error for User
#'
#' Transforms technical error into user-friendly message.
#'
#' @param error Error object
#' @param connection API connection object
#' @param request Request details
#' 
#' @return Formatted error message
#' 
#' @export
fn_format_api_error <- function(error, connection, request) {
  error_message <- as.character(error)
  platform_name <- connection$config$platform$name %||% connection$platform_id
  
  # Extract HTTP status code if present
  if (grepl("HTTP (\\d{3})", error_message)) {
    status_code <- as.numeric(gsub(".*HTTP (\\d{3}).*", "\\1", error_message))
    
    user_message <- switch(
      as.character(status_code),
      "400" = sprintf("%s API: Bad request. Check your parameters.", platform_name),
      "401" = sprintf("%s API: Authentication failed. Check your credentials.", platform_name),
      "403" = sprintf("%s API: Access forbidden. Insufficient permissions.", platform_name),
      "404" = sprintf("%s API: Resource not found at endpoint: %s", platform_name, request$endpoint),
      "429" = sprintf("%s API: Rate limit exceeded. Please try again later.", platform_name),
      "500" = sprintf("%s API: Internal server error. The platform is experiencing issues.", platform_name),
      "502" = sprintf("%s API: Bad gateway. Connection issue with the platform.", platform_name),
      "503" = sprintf("%s API: Service unavailable. The platform may be under maintenance.", platform_name),
      "504" = sprintf("%s API: Gateway timeout. The request took too long.", platform_name),
      sprintf("%s API: HTTP %d error occurred.", platform_name, status_code)
    )
  } else if (grepl("timeout|timed out", error_message, ignore.case = TRUE)) {
    user_message <- sprintf(
      "%s API: Request timed out after %d seconds. The platform may be slow or unresponsive.",
      platform_name, connection$config$error_handling$timeout %||% 30
    )
  } else if (grepl("connection", error_message, ignore.case = TRUE)) {
    user_message <- sprintf(
      "%s API: Connection failed. Check your internet connection and platform status.",
      platform_name
    )
  } else {
    user_message <- sprintf(
      "%s API error: %s",
      platform_name,
      error_message
    )
  }
  
  # Add troubleshooting hints
  user_message <- paste(
    user_message,
    "\n\nTroubleshooting:",
    "\n- Check platform status page",
    "\n- Verify API credentials are correct",
    "\n- Ensure you have proper permissions",
    "\n- Try again in a few minutes"
  )
  
  return(user_message)
}

#' Log API Error
#'
#' Logs API error details for debugging.
#'
#' @param platform Platform ID
#' @param endpoint API endpoint
#' @param error Error object
#' @param attempt Attempt number
#' @param timestamp Time of error
#' 
#' @return NULL (logs to file/console)
#' 
#' @export
fn_log_api_error <- function(platform, endpoint, error, attempt = 1, 
                            timestamp = Sys.time()) {
  error_log <- sprintf(
    "[%s] Platform: %s | Endpoint: %s | Attempt: %d | Error: %s",
    format(timestamp, "%Y-%m-%d %H:%M:%S"),
    platform,
    endpoint,
    attempt,
    as.character(error)
  )
  
  # Log to console (can be redirected to file)
  if (getOption("platform_api_debug", FALSE)) {
    message("API ERROR: ", error_log)
  }
  
  # TODO: Implement file logging based on config
  # log_file <- file.path("logs", paste0(platform, "_api.log"))
  # cat(error_log, "\n", file = log_file, append = TRUE)
  
  invisible(NULL)
}

#' Parse HTTP Response Error
#'
#' Extracts detailed error information from HTTP response.
#'
#' @param response httr response object
#' 
#' @return List with error details
#' 
#' @export
fn_parse_http_error <- function(response) {
  if (!httr::http_error(response)) {
    return(NULL)
  }
  
  error_details <- list(
    status_code = httr::status_code(response),
    status_message = httr::http_status(response)$message,
    url = response$url,
    headers = httr::headers(response)
  )
  
  # Try to parse error body
  tryCatch({
    content <- httr::content(response, "parsed", simplifyVector = TRUE)
    if (!is.null(content)) {
      error_details$body <- content
      
      # Look for common error message fields
      error_fields <- c("error", "message", "error_message", "detail", "errors")
      for (field in error_fields) {
        if (field %in% names(content)) {
          error_details$message <- content[[field]]
          break
        }
      }
    }
  }, error = function(e) {
    # If parsing fails, try to get raw text
    error_details$body <- httr::content(response, "text", encoding = "UTF-8")
  })
  
  return(error_details)
}

# Helper operator for NULL default values
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}