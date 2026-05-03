#' Convert Values to Numeric, Replacing Non-Numeric with NA
#'
#' @title Safe Numeric Conversion
#' @description Converts any input to numeric values, replacing non-numeric entries
#' with NA. This function safely handles mixed data types, character strings, and
#' factors by first converting to character then to numeric. Warnings about NA
#' introduction are suppressed as they are expected behavior.
#'
#' Following principles:
#' - R094: Roxygen2 documentation standard
#' - MP047: Functional programming (pure function)
#' - P076: Error handling patterns (graceful NA handling)
#' - R115: Proper type conversion
#'
#' @param x Vector of any type (numeric, character, factor, logical) to convert
#'        to numeric. Can handle mixed types and non-numeric strings.
#'        
#' @return Numeric vector with same length as input. Non-numeric values are
#'         replaced with NA. Numeric values are preserved. Factors are converted
#'         through their character representation, not their internal codes.
#'         
#' @examples
#' # Convert mixed character vector
#' replace_non_numeric(c("123", "45.6", "abc", "7.8e2"))
#' # Returns: c(123, 45.6, NA, 780)
#' 
#' # Handle factors correctly
#' f <- factor(c("10", "20", "hello", "30"))
#' replace_non_numeric(f)
#' # Returns: c(10, 20, NA, 30)
#' 
#' # Preserve existing numeric values
#' replace_non_numeric(c(1, 2, 3, 4))
#' # Returns: c(1, 2, 3, 4)
#' 
#' # Mixed types with NA
#' replace_non_numeric(c("100", NA, "text", 200))
#' # Returns: c(100, NA, NA, 200)
#' 
#' # Scientific notation
#' replace_non_numeric(c("1.23e4", "5E-2", "invalid"))
#' # Returns: c(12300, 0.05, NA)
#' 
#' @export
#' @principle R094 Roxygen2 documentation standard
#' @principle P076 Error handling patterns
#' @note Warnings about NA introduction are intentionally suppressed
replace_non_numeric <- function(x) {
  suppressWarnings(as.numeric(as.character(x)))
}
