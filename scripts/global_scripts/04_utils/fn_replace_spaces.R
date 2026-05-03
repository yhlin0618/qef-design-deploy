#' Replace Spaces with Underscores
#'
#' @title Convert Spaces to Underscores in Character Vectors
#' @description Replaces all space characters in a character vector with underscores.
#' This function is commonly used for cleaning column names, file names, or
#' identifiers to make them compatible with R naming conventions or file system
#' requirements that don't allow spaces.
#'
#' Following principles:
#' - R094: Roxygen2 documentation standard
#' - R019: Object naming convention
#' - MP047: Functional programming (pure function)
#' - R051: Lowercase variable naming
#'
#' @param vectors Character vector containing strings with spaces to be replaced.
#'        Can be a single string or vector of strings.
#'        
#' @return Character vector of the same length as input with all spaces replaced
#'         by underscores. Preserves all other characters unchanged.
#'         
#' @examples
#' # Clean column names
#' col_names <- c("customer id", "product name", "sale date")
#' replace_spaces(col_names)
#' # Returns: c("customer_id", "product_name", "sale_date")
#' 
#' # Clean file names
#' file_name <- "my report 2024.csv"
#' replace_spaces(file_name)
#' # Returns: "my_report_2024.csv"
#' 
#' # Handle multiple spaces
#' text <- "too  many   spaces"
#' replace_spaces(text)
#' # Returns: "too__many___spaces"
#' 
#' # Works with single strings or vectors
#' replace_spaces("hello world")  # Returns: "hello_world"
#' replace_spaces(c("a b", "c d"))  # Returns: c("a_b", "c_d")
#' 
#' @export
#' @principle R094 Roxygen2 documentation standard
#' @principle R019 Object naming convention
#' @note This function replaces ALL spaces, including multiple consecutive spaces
replace_spaces <- function(vectors) {
  gsub(" ", "_", vectors)
}
