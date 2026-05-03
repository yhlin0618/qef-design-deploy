#' @file fn_nrow2.R
#' @author Claude
#' @date 2025-05-19
#' @title Safe Row Count Function
#' @principle R021 One Function One File
#' @principle R067 Functional Encapsulation
#' @principle R069 Function File Naming
#' @principle MP047 Functional Programming
#' @principle MP035 Null Special Treatment
#' @description Provides a safe way to count rows with proper handling of edge cases

#' Safe row count function
#' @param x Object. The object to count rows for.
#' @return Numeric. The number of rows in x, or 0 if x is not a data frame or has no rows.
#' @details This function safely handles NULL values, non-data frame objects, and edge cases.
#' Use this function when you need to count rows but can't guarantee the object type or existence.
#' 
#' @examples
#' nrow2(NULL)                   # Returns 0
#' nrow2(data.frame())           # Returns 0
#' nrow2(data.frame(a = 1:3))    # Returns 3
#' nrow2("not a data frame")     # Returns 0
#' nrow2(matrix(1:4, nrow = 2))  # Returns 2
#' 
#' @export
nrow2 <- function(x) {
  # Handle NULL case
  if (is.null(x)) return(0)
  
  # Handle non-data frame and non-matrix objects
  if (!is.data.frame(x) && !is.matrix(x)) return(0)
  
  # Regular row count for valid objects
  return(nrow(x))
}