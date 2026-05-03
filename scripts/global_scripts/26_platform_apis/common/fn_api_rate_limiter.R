#' Create Rate Limiter for API Calls
#'
#' Creates a rate limiter object to control API request frequency following
#' MP094 platform API architecture standards.
#'
#' @param config Rate limiting configuration list with elements:
#'   - max_requests: Maximum number of requests allowed
#'   - per_period: Time period in seconds
#'   - strategy: "token_bucket" or "sliding_window"
#'   - delay_between_requests: Optional delay between requests
#' 
#' @return Rate limiter object with class "rate_limiter"
#' 
#' @examples
#' \dontrun{
#' config <- list(
#'   rate_limiting = list(
#'     max_requests = 100,
#'     per_period = 60,
#'     strategy = "token_bucket",
#'     delay_between_requests = 0.1
#'   )
#' )
#' limiter <- fn_create_rate_limiter(config)
#' }
#' 
#' @export
fn_create_rate_limiter <- function(config) {
  # Validate config
  if (is.null(config$rate_limiting)) {
    stop("Rate limiting configuration missing")
  }
  
  rl_config <- config$rate_limiting
  
  # Create limiter object
  limiter <- list(
    max_requests = rl_config$max_requests %||% 100,
    per_period = rl_config$per_period %||% 60,
    strategy = rl_config$strategy %||% "token_bucket",
    delay = rl_config$delay_between_requests %||% 0,
    requests = list(),
    tokens = rl_config$max_requests %||% 100,
    last_refill = Sys.time(),
    created_at = Sys.time()
  )
  
  class(limiter) <- c("rate_limiter", limiter$strategy)
  return(limiter)
}

#' Check and Enforce Rate Limits
#'
#' Checks if a request can proceed based on rate limits and enforces
#' waiting periods if necessary.
#'
#' @param limiter Rate limiter object from fn_create_rate_limiter
#' @param verbose Logical, whether to print wait messages
#' 
#' @return Updated rate limiter object
#' 
#' @examples
#' \dontrun{
#' limiter <- fn_check_rate_limit(limiter)
#' # Make API request after rate limit check
#' response <- httr::GET(url)
#' }
#' 
#' @export
fn_check_rate_limit <- function(limiter, verbose = TRUE) {
  if (!inherits(limiter, "rate_limiter")) {
    stop("Invalid rate limiter object")
  }
  
  current_time <- Sys.time()
  
  if (limiter$strategy == "sliding_window") {
    # Sliding window implementation
    cutoff_time <- current_time - limiter$per_period
    
    # Remove old requests outside the window
    limiter$requests <- limiter$requests[
      sapply(limiter$requests, function(x) x > cutoff_time)
    ]
    
    # Check if limit exceeded
    if (length(limiter$requests) >= limiter$max_requests) {
      # Calculate wait time
      oldest_request <- min(unlist(limiter$requests))
      wait_time <- as.numeric(
        oldest_request + limiter$per_period - current_time,
        units = "secs"
      )
      
      if (wait_time > 0) {
        if (verbose) {
          message(sprintf(
            "Rate limit reached (%d/%d requests). Waiting %.1f seconds...",
            length(limiter$requests), limiter$max_requests, wait_time
          ))
        }
        Sys.sleep(wait_time)
      }
    }
    
    # Record new request
    limiter$requests <- c(limiter$requests, list(current_time))
    
  } else if (limiter$strategy == "token_bucket") {
    # Token bucket implementation
    time_passed <- as.numeric(
      current_time - limiter$last_refill, 
      units = "secs"
    )
    
    # Refill tokens based on time passed
    tokens_to_add <- floor(time_passed * limiter$max_requests / limiter$per_period)
    if (tokens_to_add > 0) {
      limiter$tokens <- min(
        limiter$max_requests,
        limiter$tokens + tokens_to_add
      )
      limiter$last_refill <- current_time
    }
    
    # Check if we have tokens available
    if (limiter$tokens < 1) {
      # Calculate wait time for next token
      wait_time <- (1 - limiter$tokens) * limiter$per_period / limiter$max_requests
      
      if (verbose) {
        message(sprintf(
          "No tokens available. Waiting %.1f seconds...",
          wait_time
        ))
      }
      Sys.sleep(wait_time)
      
      # Refill after waiting
      limiter$tokens <- 1
      limiter$last_refill <- Sys.time()
    }
    
    # Consume a token
    limiter$tokens <- limiter$tokens - 1
  }
  
  # Apply standard delay between requests if configured
  if (limiter$delay > 0) {
    Sys.sleep(limiter$delay)
  }
  
  return(limiter)
}

#' Reset Rate Limiter
#'
#' Resets a rate limiter to its initial state.
#'
#' @param limiter Rate limiter object
#' 
#' @return Reset rate limiter object
#' 
#' @export
fn_reset_rate_limiter <- function(limiter) {
  if (!inherits(limiter, "rate_limiter")) {
    stop("Invalid rate limiter object")
  }
  
  limiter$requests <- list()
  limiter$tokens <- limiter$max_requests
  limiter$last_refill <- Sys.time()
  
  return(limiter)
}

#' Get Rate Limiter Status
#'
#' Returns current status of the rate limiter.
#'
#' @param limiter Rate limiter object
#' 
#' @return List with status information
#' 
#' @export
fn_rate_limiter_status <- function(limiter) {
  if (!inherits(limiter, "rate_limiter")) {
    stop("Invalid rate limiter object")
  }
  
  status <- list(
    strategy = limiter$strategy,
    max_requests = limiter$max_requests,
    per_period = limiter$per_period
  )
  
  if (limiter$strategy == "sliding_window") {
    current_time <- Sys.time()
    cutoff_time <- current_time - limiter$per_period
    active_requests <- sum(sapply(
      limiter$requests, 
      function(x) x > cutoff_time
    ))
    status$active_requests <- active_requests
    status$available <- limiter$max_requests - active_requests
    
  } else if (limiter$strategy == "token_bucket") {
    status$tokens_available <- round(limiter$tokens, 2)
    status$last_refill <- limiter$last_refill
  }
  
  return(status)
}

# Helper operator for NULL default values
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}