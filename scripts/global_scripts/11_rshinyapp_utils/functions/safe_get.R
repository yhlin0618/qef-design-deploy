#' Safe Get Function
#'
#' Safely loads an RDS file, returning NULL if the file doesn't exist
#'
#' @param name The name of the RDS file (without extension)
#' @param path The path to the RDS files directory
#'
#' @return The loaded object or NULL
#'
safe_get <- function(name, path = "app_data") {
  file_path <- file.path(path, paste0(name, ".rds"))
  if (file.exists(file_path)) {
    tryCatch({
      readRDS(file_path)
    }, error = function(e) {
      warning(paste("Error reading file:", file_path, "-", e$message))
      NULL
    })
  } else {
    NULL
  }
}