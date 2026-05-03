#' Load a specific version of an object
#'
#' Loads a specific version of an object based on its rigid identifier
#' and descriptor.
#'
#' @param rigid_id The rigid identifier of the object
#' @param descriptor The specific descriptor version to load
#' @return The loaded object, or NULL if not found
#' @examples
#' load_version("df.amazon.sales.by_product_index.at_ALL.now.001", "manual")
#'
load_version <- function(rigid_id, descriptor) {
  # Construct full object name if descriptor is provided
  full_name <- if (!is.null(descriptor) && nchar(descriptor) > 0) {
    paste0(rigid_id, "___", descriptor)
  } else {
    rigid_id
  }
  
  # Check if the object exists in the current environment or parent environments
  if (exists(full_name, inherits = TRUE)) {
    return(get(full_name, inherits = TRUE))
  }
  
  # Try to load from saved data files
  data_path <- find_data_file(full_name)
  if (!is.null(data_path)) {
    # Load based on file extension
    if (grepl("\\.rds$", data_path)) {
      return(readRDS(data_path))
    } else if (grepl("\\.rda$|\\.RData$", data_path)) {
      temp_env <- new.env()
      load(data_path, envir = temp_env)
      # If the object name is in the loaded environment, return it
      if (exists(full_name, envir = temp_env)) {
        return(get(full_name, envir = temp_env))
      } else {
        # Try to find an object in the environment
        objs <- ls(temp_env)
        if (length(objs) > 0) {
          return(get(objs[1], envir = temp_env))
        }
      }
    }
  }
  
  warning(paste("Object not found:", full_name))
  return(NULL)
}