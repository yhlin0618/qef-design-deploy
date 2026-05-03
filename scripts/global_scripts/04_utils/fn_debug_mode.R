# =============================================================================
# fn_debug_mode.R — Unified Debug Mode for Shiny App
# Following: SO_R007, SO_R028 (env var naming), DEV_P022 (transparency)
#
# Activated by environment variable: SHINY_DEBUG_MODE=TRUE
# Use cases: E2E testing (shinytest2), interactive debugging (/shiny-debug)
#
# When active, all dashboard-visible elements log their outputs to console,
# enabling automated verification that every component produces results.
# =============================================================================

#' Check if the app is running in debug mode
#'
#' Debug mode is activated by setting the environment variable
#' SHINY_DEBUG_MODE=TRUE before launching the app.
#'
#' @return logical TRUE if debug mode is active
is_debug_mode <- function() {
  isTRUE(toupper(Sys.getenv("SHINY_DEBUG_MODE", "FALSE")) == "TRUE")
}


#' Log a message only when debug mode is active
#'
#' Prefixes messages with [DEBUG][component] for easy grep filtering.
#'
#' @param component Character. Component or module label.
#' @param ... Message parts, passed to message().
debug_log <- function(component, ...) {
  if (is_debug_mode()) {
    message("[DEBUG][", component, "] ", ...)
  }
}


#' Log a reactive output value in debug mode
#'
#' Logs the class, dimensions, and preview of a reactive value.
#' Designed to be called inside observe() or render*() blocks.
#'
#' @param component Character. Component label.
#' @param output_name Character. Name of the output (e.g., "kpi_cards", "main_table").
#' @param value The value to log. Can be data.frame, list, character, numeric, etc.
#' @param max_preview Integer. Max characters for string preview (default 200).
debug_log_output <- function(component, output_name, value, max_preview = 200) {
  if (!is_debug_mode()) return(invisible(NULL))

  if (is.null(value)) {
    message("[DEBUG][", component, "] ", output_name, " = NULL")
  } else if (is.data.frame(value)) {
    message("[DEBUG][", component, "] ", output_name,
            " = data.frame [", nrow(value), " x ", ncol(value), "]",
            " cols: ", paste(head(names(value), 5), collapse = ", "),
            if (ncol(value) > 5) "..." else "")
  } else if (is.list(value)) {
    message("[DEBUG][", component, "] ", output_name,
            " = list(", length(value), ")",
            " keys: ", paste(head(names(value), 5), collapse = ", "))
  } else if (is.character(value)) {
    preview <- substr(paste(value, collapse = " "), 1, max_preview)
    message("[DEBUG][", component, "] ", output_name,
            " = character(", length(value), ") \"", preview, "\"")
  } else if (is.numeric(value)) {
    message("[DEBUG][", component, "] ", output_name,
            " = ", class(value)[1], "(", length(value), ")",
            " [", paste(head(value, 5), collapse = ", "),
            if (length(value) > 5) ", ..." else "", "]")
  } else {
    message("[DEBUG][", component, "] ", output_name,
            " = ", class(value)[1])
  }

  invisible(value)
}
