#' Resolve Script Path with Company Variant
#'
#' Resolves the correct script path based on company variant preference.
#' Implements MP122 Quad-Track Shared Symlink Architecture - company variants
#' are included in shared update_scripts (standalone git repo in shared/,
#' symlinked from company projects), and project config determines
#' which variant to execute.
#'
#' @param base_script Character. Base script path relative to update_scripts/
#'   (e.g., "ETL/cbz/cbz_ETL_sales_0IM.R")
#' @param company Character or NULL. Company identifier to look for variant
#'   (e.g., "MAMBA"). If NULL, returns base script path.
#' @param scripts_dir Character. Path to update_scripts directory.
#'   Defaults to "scripts/update_scripts"
#'
#' @return Character. Full path to the resolved script (variant if exists, otherwise base)
#'
#' @examples
#' # Will return eby_ETL_orders_0IM___MAMBA.R if it exists
#' resolve_script_path("ETL/eby/eby_ETL_orders_0IM.R", company = "MAMBA")
#'
#' # Without company, returns base path
#' resolve_script_path("ETL/eby/eby_ETL_orders_0IM.R")
#'
#' @seealso MP122 Quad-Track Shared Symlink Architecture, DM_R037, DM_R038
#' @export
resolve_script_path <- function(base_script,
                                 company = NULL,
                                 scripts_dir = "scripts/update_scripts") {
  # Validate inputs

  if (!is.character(base_script) || length(base_script) != 1) {
    stop("base_script must be a single character string")
  }

  # If no company specified, return base script path
  if (is.null(company) || company == "") {
    full_path <- file.path(scripts_dir, base_script)
    if (!file.exists(full_path)) {
      warning(sprintf("Base script not found: %s", full_path))
    }
    return(full_path)
  }

  # Construct company variant path (per DM_R037 naming convention)
  # Pattern: script_name___COMPANY.R
  variant_script <- gsub("\\.R$", paste0("___", company, ".R"), base_script)
  variant_path <- file.path(scripts_dir, variant_script)

  # Check if variant exists

  if (file.exists(variant_path)) {
    message(sprintf("Using company variant: %s", basename(variant_path)))
    return(variant_path)
  }

  # Fallback to base script
  base_path <- file.path(scripts_dir, base_script)
  if (!file.exists(base_path)) {
    warning(sprintf("Neither variant nor base script found: %s", base_path))
  } else {
    message(sprintf("Company variant not found, using base: %s", basename(base_path)))
  }

  return(base_path)
}


#' Resolve Multiple Script Paths with Company Variant
#'
#' Vectorized version of resolve_script_path for multiple scripts.
#'
#' @param base_scripts Character vector. Base script paths.
#' @param company Character or NULL. Company identifier.
#' @param scripts_dir Character. Path to update_scripts directory.
#'
#' @return Character vector. Resolved script paths.
#'
#' @seealso resolve_script_path
#' @export
resolve_script_paths <- function(base_scripts,
                                  company = NULL,
                                  scripts_dir = "scripts/update_scripts") {
  vapply(base_scripts, function(script) {
    resolve_script_path(script, company = company, scripts_dir = scripts_dir)
  }, character(1), USE.NAMES = FALSE)
}


#' Get Company from App Config
#'
#' Helper function to extract company identifier from app_config.yaml.
#'
#' @param config_path Character. Path to app_config.yaml.
#'   Defaults to "app_config.yaml" in current directory.
#'
#' @return Character or NULL. Company identifier.
#'
#' @examples
#' company <- get_company_from_config()
#' script <- resolve_script_path("ETL/cbz/cbz_ETL_sales_0IM.R", company)
#'
#' @export
get_company_from_config <- function(config_path = "app_config.yaml") {
  if (!file.exists(config_path)) {
    warning(sprintf("Config file not found: %s", config_path))
    return(NULL)
  }

  tryCatch({
    config <- yaml::read_yaml(config_path)

    # Try multiple possible locations for company identifier
    company <- config$execution$company_variant %||%
               config$app_info$company %||%
               config$company %||%
               NULL

    if (is.null(company)) {
      message("No company identifier found in config")
    }

    return(company)
  }, error = function(e) {
    warning(sprintf("Error reading config: %s", e$message))
    return(NULL)
  })
}
