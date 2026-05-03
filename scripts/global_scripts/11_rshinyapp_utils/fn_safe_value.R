#' Safe Access Function for Data Values
#'
#' Safely retrieves a value from a data frame, providing a default value if
#' the data is invalid, missing, or doesn't contain the requested field.
#' This function helps implement the UI-Server Pairing Rule by ensuring outputs
#' always have values, even when data is invalid or missing.
#'
#' @param data The data frame to access
#' @param field The field (column) name to retrieve
#' @param index The row index to access (default: 1)
#' @param default The default value to return if the data is invalid or missing
#'
#' @return The value from the data frame, or the default value if invalid
#' @export
#'
#' @examples
#' # Basic usage
#' df <- data.frame(value = c(10, 20, 30))
#' safeValue(df, "value", 2)  # Returns 20
#' safeValue(df, "missing_column", 1, "N/A")  # Returns "N/A"
#' safeValue(df, "value", 99, 0)  # Returns 0 (index out of bounds)
#' safeValue(NULL, "value", 1, "No data")  # Returns "No data" (NULL data)
safeValue <- function(data, field, index = 1, default = NA) {
  if (is.null(data) || 
      nrow(data) < index || 
      !field %in% names(data) || 
      is.na(data[[field]][index])) {
    return(default)
  }
  return(data[[field]][index])
}