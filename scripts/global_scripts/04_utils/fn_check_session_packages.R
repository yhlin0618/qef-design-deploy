#' Session-Start Toolchain Readiness Check (DEV_R057)
#'
#' Verifies that the R session has (1) R version >= project minimum,
#' (2) every package in the canonical `core` list installed,
#' (3) `optional` packages reported (warn) if absent.
#'
#' Per SO_R057 / DEV_R057, this is the single source of truth for the
#' canonical core + optional lists. CLI wrapper (`98_test/check_packages.R`)
#' and `autoinit()` (in `sc_Rprofile.R`) both source this file rather than
#' duplicating the lists.
#'
#' @param stop_on_missing_core Logical. If TRUE (default), the function
#'   invokes `stop()` with an actionable error when core packages are
#'   missing or R version is below `r_min`. If FALSE, it returns the
#'   structured result without stopping, allowing callers to inspect.
#' @param core_pkgs Character vector. Defaults to
#'   `check_session_core_packages()`. Override only for testing.
#' @param optional_pkgs Character vector. Defaults to
#'   `check_session_optional_packages()`. Override only for testing.
#' @param r_min Character. Minimum R version required. Default `"4.4.0"`.
#'
#' @return A list with five fields:
#'   \describe{
#'     \item{r_version_actual}{Character. e.g., "4.5.1"}
#'     \item{r_version_required}{Character. The `r_min` argument.}
#'     \item{r_version_ok}{Logical. TRUE iff actual >= required.}
#'     \item{missing_core}{Character vector of missing core packages.}
#'     \item{missing_optional}{Character vector of missing optional.}
#'   }
#'
#' @examples
#' \dontrun{
#' # CLI usage (from autoinit or 98_test/check_packages.R):
#' check_session_packages()  # stop_on_missing_core = TRUE by default
#'
#' # Inspection without stopping (for diagnostic / dashboard use):
#' result <- check_session_packages(stop_on_missing_core = FALSE)
#' if (length(result$missing_core) > 0) {
#'   message("Need to install: ", paste(result$missing_core, collapse = ", "))
#' }
#' }
#'
#' @export
check_session_packages <- function(stop_on_missing_core = TRUE,
                                    core_pkgs = check_session_core_packages(),
                                    optional_pkgs = check_session_optional_packages(),
                                    r_min = "4.4.0") {
  # ---- R version assertion ------------------------------------------------
  r_actual <- paste(R.version$major, R.version$minor, sep = ".")
  r_ok <- utils::compareVersion(r_actual, r_min) >= 0L

  # ---- Package presence check --------------------------------------------
  # Use the "Package" column (more robust than rownames; matches
  # installed.packages() documented matrix layout).
  installed_mat <- utils::installed.packages()
  installed <- if (is.null(installed_mat) || nrow(installed_mat) == 0L) {
    character(0)
  } else if ("Package" %in% colnames(installed_mat)) {
    as.character(installed_mat[, "Package"])
  } else {
    rownames(installed_mat)
  }
  missing_core <- setdiff(core_pkgs, installed)
  missing_optional <- setdiff(optional_pkgs, installed)

  result <- list(
    r_version_actual = r_actual,
    r_version_required = r_min,
    r_version_ok = r_ok,
    missing_core = missing_core,
    missing_optional = missing_optional
  )

  # ---- Optional packages: always warn, never block -----------------------
  if (length(missing_optional) > 0L) {
    warning(sprintf(
      "Missing optional packages (warn-only, session continues): %s",
      paste(missing_optional, collapse = ", ")
    ), call. = FALSE)
  }

  # ---- Stop-on-fail path -------------------------------------------------
  if (stop_on_missing_core) {
    # R version failure takes precedence over missing packages because if R
    # itself is too old, package state is moot.
    if (!r_ok) {
      stop(paste(
        "Session-start R version assertion failed:",
        sprintf("  Running: R %s", r_actual),
        sprintf("  Required: R %s or higher", r_min),
        "  Remedy: upgrade R via the macOS installer at https://cran.r-project.org/",
        "  See DEV_R057 for details.",
        sep = "\n"
      ), call. = FALSE)
    }
    if (length(missing_core) > 0L) {
      remedy <- sprintf("install.packages(c(%s))",
                        paste0("'", missing_core, "'", collapse = ", "))
      stop(paste(
        "Session-start toolchain readiness check failed:",
        sprintf("  Missing core packages: %s",
                paste(missing_core, collapse = ", ")),
        sprintf("  Remedy: %s", remedy),
        "  See DEV_R057 for details.",
        sep = "\n"
      ), call. = FALSE)
    }
  }

  invisible(result)
}

#' Canonical Core Package List (DEV_R057)
#'
#' MUST packages -- session start `stop()`s if any are missing (when
#' `stop_on_missing_core = TRUE`). Single source of truth for the project.
#'
#' Adding / removing entries triggers IC_P002 cross-company verification
#' across all 5 consuming companies (QEF_DESIGN / D_RACING / MAMBA /
#' WISER / kitchenMAMA).
#'
#' @return Character vector of canonical core package names.
#' @export
check_session_core_packages <- function() {
  c(
    # Data manipulation (universal data access pattern, DM_R023)
    "data.table", "dplyr", "dbplyr", "tidyr", "purrr",
    # Database
    "DBI", "duckdb", "RPostgres",
    # File / config
    "readxl", "readr", "yaml", "jsonlite",
    # Time / locale / text
    "lubridate", "stringr",
    # Path / project
    "here"
  )
}

#' Canonical Optional Package List (DEV_R057)
#'
#' SHOULD packages -- session start emits `warning()` if missing but
#' continues. These cover testing infrastructure, quality tools, and
#' integrations that not every flow needs.
#'
#' @return Character vector of canonical optional package names.
#' @export
check_session_optional_packages <- function() {
  c(
    # Testing infrastructure
    "testthat", "shinytest2",
    # Quality tools
    "lintr",
    # Stats utilities
    "broom", "zoo", "knitr",
    # External integration
    "googlesheets4",
    # HTTP / OpenAI
    "httr2", "openssl"
  )
}
