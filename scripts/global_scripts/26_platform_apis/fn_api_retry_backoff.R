#' @file fn_api_retry_backoff.R
#' @title Generic API Call Retry with Exponential Backoff
#' @description Shared helper for wrapping REST API calls with transient-error
#'   retry and exponential backoff delays. Used by ETL 0IM scripts that fetch
#'   from paginated upstream APIs (Cyberbiz, eBay, etc.) to survive HTTP 429
#'   rate limits and transient 5xx errors without giving up after a single
#'   failure. Non-retryable statuses (4xx except 429) abort immediately.
#' @principle MP029 No Fake Data — helper wraps real API calls, no mocks
#' @principle DEV_P022 Console Transparency — logs each retry attempt with
#'   status code and delay
#' @principle MP100 UTF-8 Encoding Standard
#' @author spectra change fix-mamba-etl-complete-capture
#' @date 2026-04-14
#' @use httr (for status_code extraction when error is from httr)

#' Call a function with automatic retry + exponential backoff on transient failures
#'
#' Executes `fn(...)` once; if the call succeeds, returns the result. If the
#' call errors and the error carries a retryable HTTP status code, waits
#' `base_delay * backoff_factor^(attempt - 1)` seconds and retries up to
#' `max_retries` additional times. Non-retryable errors abort immediately.
#' Retries exhausted also abort with the last error.
#'
#' @param fn Function reference to call. Typically `function() httr::GET(url, ...)`.
#' @param ... Additional arguments passed to `fn()`.
#' @param max_retries Integer. Maximum number of retry attempts (total attempts
#'   = max_retries + 1). Defaults to 5.
#' @param base_delay Numeric. Base delay in seconds before the first retry.
#'   Defaults to 1.
#' @param backoff_factor Numeric. Multiplier applied to delay between retries
#'   (exponential growth). Defaults to 2 → delays are 1, 2, 4, 8, 16 seconds.
#' @param retry_statuses Integer vector. HTTP status codes that trigger retry.
#'   Defaults to `c(429, 500, 502, 503, 504)`.
#' @return The return value of `fn(...)` on success.
#' @examples
#' \dontrun{
#' result <- api_call_with_retry(
#'   fn = function() httr::GET(url, httr::add_headers(Authorization = token)),
#'   max_retries = 5,
#'   base_delay = 1,
#'   backoff_factor = 2
#' )
#' }
api_call_with_retry <- function(fn,
                                ...,
                                max_retries = 5,
                                base_delay = 1,
                                backoff_factor = 2,
                                retry_statuses = c(429, 500, 502, 503, 504)) {
  stopifnot(is.function(fn),
            is.numeric(max_retries), max_retries >= 0,
            is.numeric(base_delay), base_delay >= 0,
            is.numeric(backoff_factor), backoff_factor >= 1,
            is.numeric(retry_statuses))

  extract_status <- function(e) {
    # httr errors may carry a $response with status_code; otherwise fall back to
    # scanning the error message for a 3-digit code.
    status <- tryCatch(
      httr::status_code(e$response),
      error = function(e2) NA_integer_
    )
    if (is.na(status) && !is.null(e$message)) {
      match <- regmatches(e$message, regexpr("\\b[4-5][0-9]{2}\\b", e$message))
      if (length(match) > 0) status <- as.integer(match[1])
    }
    if (is.null(status)) NA_integer_ else status
  }

  total_attempts <- max_retries + 1L
  last_error <- NULL

  for (attempt in seq_len(total_attempts)) {
    result <- tryCatch(
      fn(...),
      error = function(e) structure(list(error = e), class = "api_call_failure")
    )

    if (!inherits(result, "api_call_failure")) {
      return(result)
    }

    last_error <- result$error
    status <- extract_status(last_error)
    is_last_attempt <- attempt == total_attempts
    is_retryable <- !is.na(status) && status %in% retry_statuses

    if (is_last_attempt) {
      message(sprintf("[api_retry] all %d attempts failed (last status=%s); aborting.",
                      total_attempts,
                      if (is.na(status)) "unknown" else as.character(status)))
      stop(last_error)
    }

    if (!is_retryable) {
      message(sprintf("[api_retry] non-retryable error (status=%s); aborting after attempt %d.",
                      if (is.na(status)) "unknown" else as.character(status),
                      attempt))
      stop(last_error)
    }

    delay <- base_delay * (backoff_factor ^ (attempt - 1))
    message(sprintf("[api_retry] attempt %d/%d got HTTP %d; waiting %.2fs before retry...",
                    attempt, total_attempts, status, delay))
    Sys.sleep(delay)
  }

  # Defensive: unreachable, loop always returns or stops.
  stop(last_error)
}
