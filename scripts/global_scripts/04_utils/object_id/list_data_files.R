#' List data files for an object
#'
#' Helper function to list all data files that might contain versions of the specified object.
#'
#' @param rigid_id Rigid identifier of the object
#' @return Character vector of file paths
#' @keywords internal
#'
list_data_files <- function(rigid_id) {
  # Define possible data directories to check
  possible_dirs <- c(
    getwd(),
    file.path(getwd(), "data"),
    file.path(getwd(), "app_data"),
    file.path(getwd(), "30_global_data")
  )
  
  # Clean the name for use in filenames
  clean_id <- gsub("\\.", "_", rigid_id)
  escaped_id <- gsub("\\.", "\\\\.", rigid_id)
  
  all_files <- c()
  
  for (dir in possible_dirs) {
    if (!dir.exists(dir)) next
    
    # List all files in the directory
    files <- list.files(dir, pattern = "\\.(rds|rda|RData)$", full.names = TRUE)
    
    # Filter files that match our pattern
    matching_files <- grep(paste0("^", clean_id, "(___.*)?\\.(rds|rda|RData)$"), basename(files), value = TRUE)
    all_files <- c(all_files, file.path(dir, matching_files))
    
    # Also check with dots instead of underscores in filename
    matching_files <- grep(paste0("^", escaped_id, "(___.*)?\\.(rds|rda|RData)$"), basename(files), value = TRUE)
    all_files <- c(all_files, file.path(dir, matching_files))
  }
  
  return(unique(all_files))
}