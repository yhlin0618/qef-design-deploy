#' List all available versions of an object
#'
#' Finds all versions of an object with the given rigid identifier.
#'
#' @param rigid_id The rigid identifier of the object
#' @return A character vector of available descriptors, or empty vector if none found
#' @examples
#' list_versions("df.amazon.sales.by_product_index.at_ALL.now.001")
#'
list_versions <- function(rigid_id) {
  # First check objects in the current environment and parent environments
  all_objects <- ls(all.names = TRUE, envir = .GlobalEnv)
  matching_objects <- grep(paste0("^", gsub("\\.", "\\\\.", rigid_id), "(___.*)?$"), all_objects, value = TRUE)
  
  # Extract descriptors from matching object names
  descriptors <- sapply(matching_objects, function(obj) {
    desc <- get_descriptors(obj)
    if (is.null(desc)) {
      return("")  # No descriptor
    } else {
      return(desc)
    }
  })
  
  # Also look for saved data files
  data_files <- list_data_files(rigid_id)
  file_descriptors <- sapply(data_files, function(file) {
    # Extract the object name from the filename
    obj_name <- sub("\\.(rds|rda|RData)$", "", basename(file))
    desc <- get_descriptors(obj_name)
    if (is.null(desc)) {
      return("")  # No descriptor
    } else {
      return(desc)
    }
  })
  
  # Combine and remove duplicates
  all_descriptors <- unique(c(descriptors, file_descriptors))
  
  # Sort descriptors (empty string first, then alphabetically)
  sorted_descriptors <- all_descriptors[order(all_descriptors == "", all_descriptors)]
  
  return(sorted_descriptors)
}