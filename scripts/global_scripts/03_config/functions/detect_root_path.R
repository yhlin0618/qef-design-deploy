#' Detect Root Path
#'
#' Detects the appropriate root path for the project based on the current environment
#'
#' @return Character string with the detected root path
#'
detect_root_path <- function() {
  # Get the username
  username <- Sys.info()["user"]
  
  # Check for Dropbox path based on operating system
  if (.Platform$OS.type == "windows") {
    # Common Windows Dropbox paths to check
    dropbox_paths <- c(
      file.path("C:", "Users", username, "Dropbox", "precision_marketing"),
      file.path("D:", "Dropbox", "precision_marketing")
    )
  } else {
    # macOS/Linux paths
    dropbox_paths <- c(
      file.path("/Users", username, "Dropbox", "precision_marketing"),
      file.path("/Users", username, "Library", "CloudStorage", "Dropbox", "precision_marketing")
    )
  }
  
  # Find the first path that exists
  for (path in dropbox_paths) {
    if (dir.exists(path)) {
      message("Found precision_marketing root path: ", path)
      return(path)
    }
  }
  
  # If no path is found, use COMPANY_DIR if available, otherwise use here::here()
  if (exists("COMPANY_DIR")) {
    warning("Could not find precision_marketing root path. Using COMPANY_DIR.")
    return(COMPANY_DIR)
  } else if (requireNamespace("here", quietly = TRUE)) {
    warning("Could not find precision_marketing root path. Using here::here().")
    return(dirname(here::here()))
  } else {
    stop("Could not find precision_marketing root path and no fallback available.")
  }
}