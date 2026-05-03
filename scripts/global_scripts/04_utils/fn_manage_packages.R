#' Ensure Package Installation and Loading
#'
#' Checks if required packages are installed, installs them if missing,
#' and loads them into the current environment.
#'
#' @param ... Package names to check, install, and load
#'
#' @return NULL invisibly
#'
#' @examples
#' ensure_packages("dplyr", "ggplot2", "readr")
#'
ensure_packages <- function(...) {
  # Get the package names as character strings
  pkgs <- as.character(unlist(list(...)))
  
  # Loop through each package
  for (pkg in pkgs) {
    if (!requireNamespace(pkg, quietly = TRUE)) {
      message(paste("Installing missing package:", pkg))
      install.packages(pkg, dependencies = TRUE)
    }
    # Load the package
    library(pkg, character.only = TRUE)
  }
  
  invisible(NULL)
}