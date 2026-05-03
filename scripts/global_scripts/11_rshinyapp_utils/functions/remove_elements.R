#' Remove Elements Function
#'
#' Removes specified elements from a vector, ignoring case
#'
#' @param vector The input vector
#' @param elements Elements to remove
#'
#' @return The filtered vector
#'
remove_elements <- function(vector, elements) {
  vector <- vector[!tolower(vector) %in% tolower(elements)]
  return(vector)
}