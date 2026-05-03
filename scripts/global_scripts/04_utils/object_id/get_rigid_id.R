#' Extract the rigid identifier from an object name
#'
#' Extracts the part before the triple underscore (___) in an object name,
#' which represents the unique identifier that doesn't change with processing state.
#'
#' @param object_name Full name of the object, possibly including descriptors
#' @return The rigid identifier part of the name (before ___)
#' @examples
#' get_rigid_id("df.amazon.sales.by_product_index.at_ALL.now.001___manual")
#' # Returns: "df.amazon.sales.by_product_index.at_ALL.now.001"
#'
get_rigid_id <- function(object_name) {
  if (is.null(object_name) || !is.character(object_name)) {
    stop("Object name must be a character string")
  }
  
  parts <- strsplit(object_name, "___")[[1]]
  return(parts[1])  # Return only the part before ___
}
