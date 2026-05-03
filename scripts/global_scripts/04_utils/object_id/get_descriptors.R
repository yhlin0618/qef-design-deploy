#' Extract the optional descriptors from an object name
#'
#' Extracts the part after the triple underscore (___) in an object name,
#' which represents the descriptive information about processing state.
#'
#' @param object_name Full name of the object, including descriptors
#' @return The descriptors part of the name (after ___), or NULL if no descriptors
#' @examples
#' get_descriptors("df.amazon.sales.by_product_index.at_ALL.now.001___manual")
#' # Returns: "manual"
#'
get_descriptors <- function(object_name) {
  if (is.null(object_name) || !is.character(object_name)) {
    stop("Object name must be a character string")
  }
  
  parts <- strsplit(object_name, "___")[[1]]
  if (length(parts) > 1) {
    return(parts[2])  # Return the part after ___
  } else {
    return(NULL)  # No descriptors present
  }
}