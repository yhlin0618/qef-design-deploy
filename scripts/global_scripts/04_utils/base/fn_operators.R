#' Custom Operators for Enhanced R Programming
#'
#' This file defines custom operators to improve code readability and provide
#' convenient shortcuts for common operations. Following R67 (Functional Encapsulation)
#' and MP47 (Functional Programming) principles.
#'
#' @author WISER Team
#' @date 2025-07-13

# ==============================================================================
# String and Paste Operators
# ==============================================================================

#' String Concatenation with Repetition Operator
#'
#' Creates repeated strings, useful for creating separators and visual elements
#'
#' @param string Character string to repeat
#' @param n Number of times to repeat
#' @return Character string with repeated elements
#'
#' @examples
#' "=" %|>% 80  # Creates "===============...=" (80 times)
#' "-" %|>% 50  # Creates "---------------...-" (50 times)
#'
#' @export
`%|>%` <- function(string, n) {
  paste(rep(string, n), collapse = "")
}

# ==============================================================================
# Null-Safe Operators
# ==============================================================================

#' Null-Safe Assignment Operator
#'
#' Assigns value only if left-hand side is NULL
#'
#' @param lhs Left-hand side value
#' @param rhs Right-hand side value (default)
#' @return lhs if not NULL, otherwise rhs
#'
#' @examples
#' x <- NULL
#' x %||% "default"  # Returns "default"
#' y <- "value"
#' y %||% "default"  # Returns "value"
#'
#' @export
`%||%` <- function(lhs, rhs) {
  if (is.null(lhs)) rhs else lhs
}

#' Null-Safe String Concatenation Operator
#'
#' Concatenates strings, treating NULL as empty string
#'
#' @param lhs Left-hand side string
#' @param rhs Right-hand side string
#' @return Concatenated string, with NULL treated as ""
#'
#' @examples
#' "Hello" %+% NULL  # Returns "Hello"
#' "Hello" %+% " World"  # Returns "Hello World"
#'
#' @export
`%+%` <- function(lhs, rhs) {
  paste0(lhs %||% "", rhs %||% "")
}

# ==============================================================================
# Logical Operators
# ==============================================================================

#' Not In Operator
#'
#' Logical negation of the %in% operator
#'
#' @param x Vector of values to test
#' @param table Vector of values to test against
#' @return Logical vector indicating if x is NOT in table
#'
#' @examples
#' c(1, 2, 3) %notin% c(2, 4, 6)  # Returns c(TRUE, FALSE, TRUE)
#'
#' @export
`%notin%` <- function(x, table) {
  !x %in% table
}

# ==============================================================================
# Mathematical Operators
# ==============================================================================

#' Between Operator (Inclusive)
#'
#' Tests if values are between two bounds (inclusive)
#'
#' @param x Numeric vector to test
#' @param bounds Numeric vector of length 2 (min, max)
#' @return Logical vector indicating if x is between bounds
#'
#' @examples
#' c(1, 5, 10) %between% c(3, 8)  # Returns c(FALSE, TRUE, FALSE)
#'
#' @export
`%between%` <- function(x, bounds) {
  x >= bounds[1] & x <= bounds[2]
}

# ==============================================================================
# Data Frame Operators
# ==============================================================================

#' Safe Column Selection Operator
#'
#' Selects columns from data frame, returning NULL if column doesn't exist
#'
#' @param df Data frame
#' @param col Column name (character)
#' @return Column vector if exists, NULL otherwise
#'
#' @examples
#' df %col% "existing_column"    # Returns column
#' df %col% "missing_column"     # Returns NULL
#'
#' @export
`%col%` <- function(df, col) {
  if (col %in% names(df)) {
    df[[col]]
  } else {
    NULL
  }
}

# ==============================================================================
# Pipe-like Operators
# ==============================================================================

#' Apply Function Operator
#'
#' Applies function to left-hand side, useful for inline transformations
#'
#' @param x Input value
#' @param f Function to apply
#' @return Result of f(x)
#'
#' @examples
#' "hello world" %apply% toupper  # Returns "HELLO WORLD"
#' c(1, 2, 3) %apply% sum         # Returns 6
#'
#' @export
`%apply%` <- function(x, f) {
  f(x)
}

# ==============================================================================
# Utility Functions for Operators
# ==============================================================================

#' Load All Custom Operators
#'
#' Convenience function to source all operator definitions
#' This function is automatically called when this file is sourced
#'
#' @return Invisible TRUE
#' @export
load_custom_operators <- function() {
  message("Custom operators loaded: %|>%, %||%, %+%, %notin%, %between%, %col%, %apply%")
  invisible(TRUE)
}

# Automatically load operators when file is sourced
load_custom_operators()