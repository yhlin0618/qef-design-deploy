#' @file fn_source_once.R
#' @principle UX_P001 Lazy Initialization, SO_R007 One Function One File
#' @description Source a file only if it hasn't been sourced before.
#'   Uses canonical (normalized) path as key to prevent duplicate loading.
#'   This avoids the cost of re-parsing and re-executing files that autoinit()
#'   or other initialization steps have already loaded.

# Registry of already-sourced files (canonical paths)
if (!exists(".source_once_registry", envir = .GlobalEnv)) {
  .source_once_registry <- character(0)
  assign(".source_once_registry", .source_once_registry, envir = .GlobalEnv)
}

#' Source a file only once
#'
#' @param file Character. Path to the R file to source.
#' @param ... Additional arguments passed to \code{source()}.
#' @return Invisible NULL. Side effect: sources the file if not already loaded.
source_once <- function(file, ...) {
  canonical <- normalizePath(file, mustWork = FALSE)
  registry <- get(".source_once_registry", envir = .GlobalEnv)
  if (canonical %in% registry) {
    return(invisible(NULL))
  }
  source(file, ...)
  assign(
    ".source_once_registry",
    c(registry, canonical),
    envir = .GlobalEnv
  )
  invisible(NULL)
}
