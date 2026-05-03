#' Find a data file for an object
#'
#' Helper function to find a data file that might contain the specified object.
#'
#' @param full_name Full name of the object
#' @return Path to the data file if found, NULL otherwise
#' @keywords internal
#'
find_data_file <- function(full_name) {
  # Define possible data directories to check
  possible_dirs <- c(
    getwd(),
    file.path(getwd(), "data"),
    file.path(getwd(), "app_data"),
    file.path(getwd(), "30_global_data")
  )
  
  # Clean the name for use in filenames
  clean_name <- gsub("\\.", "_", full_name)
  
  # Check for various file extensions
  for (dir in possible_dirs) {
    if (!dir.exists(dir)) next
    
    for (ext in c(".rds", ".rda", ".RData")) {
      file_path <- file.path(dir, paste0(clean_name, ext))
      if (file.exists(file_path)) {
        return(file_path)
      }
      
      # Also check with dots instead of underscores in filename
      file_path <- file.path(dir, paste0(full_name, ext))
      if (file.exists(file_path)) {
        return(file_path)
      }
    }
  }
  
  return(NULL)
}