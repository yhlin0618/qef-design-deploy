#' Legacy Compatibility Wrapper for Condition Grid Utilities
#'
#' This script is kept for backward compatibility. Preferred entry point:
#' scripts/global_scripts/04_utils/fn_build_condition_grid.R
#'
#' File: sc_grid_components.R

detect_this_file <- function() {
  for (i in rev(seq_len(sys.nframe()))) {
    ofile <- tryCatch(sys.frame(i)$ofile, error = function(e) NULL)
    if (!is.null(ofile) && nzchar(ofile)) {
      return(normalizePath(ofile, winslash = "/", mustWork = FALSE))
    }
  }
  ""
}

utility_candidates <- c()
if (exists("GLOBAL_DIR", inherits = TRUE)) {
  utility_candidates <- c(
    utility_candidates,
    file.path(GLOBAL_DIR, "04_utils", "fn_build_condition_grid.R")
  )
}
utility_candidates <- c(
  utility_candidates,
  file.path("scripts", "global_scripts", "04_utils", "fn_build_condition_grid.R")
)

this_file <- detect_this_file()
if (nzchar(this_file)) {
  utility_candidates <- c(
    utility_candidates,
    file.path(dirname(dirname(this_file)), "04_utils", "fn_build_condition_grid.R")
  )
}

utility_path <- utility_candidates[which(file.exists(utility_candidates))[1]]
if (is.na(utility_path) || !nzchar(utility_path)) {
  stop("Cannot locate fn_build_condition_grid.R from sc_grid_components.R")
}

app_dir_hint <- NULL
if (exists("APP_DIR", inherits = TRUE)) {
  app_dir_hint <- APP_DIR
} else if (nzchar(this_file)) {
  app_dir_hint <- normalizePath(
    file.path(dirname(this_file), "..", "..", ".."),
    winslash = "/",
    mustWork = FALSE
  )
}

if (!exists("load_dimension_components", mode = "function")) {
  source(utility_path)
}

if (!exists("grid_components", envir = .GlobalEnv, inherits = FALSE)) {
  grid_components <- load_dimension_components(app_dir = app_dir_hint)
}

get_condition_grid <- function(components = grid_components) {
  build_condition_grid(components)
}

get_filtered_condition_grid <- function(
    time_values = NULL,
    product_values = NULL,
    state_values = NULL,
    source_values = NULL,
    country_values = NULL) {

  filtered_components <- filter_condition_components(
    components = grid_components,
    time_values = time_values,
    product_values = product_values,
    state_values = state_values,
    source_values = source_values,
    country_values = country_values
  )

  build_condition_grid(filtered_components)
}
