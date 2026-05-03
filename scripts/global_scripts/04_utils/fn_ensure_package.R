#' @file fn_ensure_package.R
#' @principle UX_P001 Lazy Initialization, SO_R007 One Function One File
#' @description Load a package only if its namespace is not already loaded.
#'   Designed for deferred (lazy) package loading — call this inside component
#'   code right before the package is actually needed, instead of loading
#'   everything at app startup.

#' Ensure a package is loaded (lazy library)
#'
#' @param pkg Character. Package name to load.
#' @return Invisible TRUE on success. Stops on failure.
ensure_package <- function(pkg) {
  if (!isNamespaceLoaded(pkg)) {
    library(pkg, character.only = TRUE)
  }
  invisible(TRUE)
}
