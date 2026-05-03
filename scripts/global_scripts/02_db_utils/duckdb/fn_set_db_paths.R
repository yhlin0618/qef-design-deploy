#' Set Custom Database Paths
#'
#' Updates the global database path list with custom paths.
#'
#' @param custom_paths List. A list of database paths to override defaults
#' @param base_dir Character. Optional base directory for relative paths (defaults to current directory)
#' @param reset Logical. Whether to reset to default paths first (defaults to FALSE)
#'
#' @return The updated database path list
#'
#' @details
#' This function allows you to customize database paths while keeping the
#' structure defined in the default path list. You can update specific paths
#' or provide a completely new set of paths.
#'
#' @examples
#' # Update specific database paths
#' set_db_paths(list(
#'   raw_data = "path/to/custom/raw_data.duckdb",
#'   processed_data = "path/to/custom/processed_data.duckdb"
#' ))
#'
#' # Reset to defaults then set custom paths
#' set_db_paths(list(
#'   raw_data = "path/to/custom/raw_data.duckdb"
#' ), reset = TRUE)
#'
#' @export
set_db_paths <- function(custom_paths, base_dir = NULL, reset = FALSE) {
  # Get reference to the global path list
  db_path_list_global <- get("db_path_list", envir = .GlobalEnv)
  
  # Reset to defaults if requested
  if (reset) {
    db_path_list_global <- get_default_db_paths(base_dir)
  }
  
  # Update with custom paths
  for (name in names(custom_paths)) {
    db_path_list_global[[name]] <- custom_paths[[name]]
  }
  
  # Update the global path list
  assign("db_path_list", db_path_list_global, envir = .GlobalEnv)
  
  # Print information about the updated paths
  message("Database paths updated:")
  for (name in names(db_path_list_global)) {
    message("  - ", name, ": ", db_path_list_global[[name]])
  }
  
  return(invisible(db_path_list_global))
}