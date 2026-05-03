#' Save an object with version tracking
#'
#' Saves an object with its full name (including descriptors) and manages versions.
#'
#' @param object The object to save
#' @param rigid_id The rigid identifier for the object
#' @param descriptor Optional descriptor for this version
#' @param save_dir Directory to save the file in
#' @param format Format to save in: "rds" (default) or "rda"
#' @return The full path where the object was saved
#' @examples
#' save_version(my_dataframe, "df.amazon.sales.by_product_index.at_ALL.now.001", "manual")
#'
save_version <- function(object, rigid_id, descriptor = NULL, save_dir = "app_data", format = "rds") {
  # Construct full object name
  full_name <- if (!is.null(descriptor) && nchar(descriptor) > 0) {
    paste0(rigid_id, "___", descriptor)
  } else {
    rigid_id
  }
  
  # Clean name for filename
  clean_name <- gsub("\\.", "_", full_name)
  
  # Create save directory if it doesn't exist
  if (!dir.exists(save_dir)) {
    dir.create(save_dir, recursive = TRUE)
  }
  
  # Create the full file path
  file_path <- file.path(save_dir, paste0(clean_name, ".", format))
  
  # Save based on requested format
  if (format == "rds") {
    saveRDS(object, file = file_path)
  } else if (format == "rda") {
    # Assign the object to its full name in a temporary environment
    temp_env <- new.env()
    assign(full_name, object, envir = temp_env)
    # Save the environment
    save(list = full_name, envir = temp_env, file = file_path)
  } else {
    stop("Unsupported format. Use 'rds' or 'rda'.")
  }
  
  # Also assign the object in the current environment with its full name
  assign(full_name, object, envir = .GlobalEnv)
  
  return(file_path)
}