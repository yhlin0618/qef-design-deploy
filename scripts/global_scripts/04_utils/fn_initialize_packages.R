#' @file fn_initialize_packages.R
#' @principle R95 Import Requirements Rule - Centralized Package Management
#' @author Data Team
#' @date 2025-04-10
#' @description Centralized package management following R95 Import Requirements Rule

# !! CENTRALIZED PACKAGE MANAGEMENT !!

#' Initialize application packages in a centralized manner
#'
#' This function implements R95 (Import Requirements Rule) by centralizing all package
#' imports in one location. It loads packages based on the specified mode and
#' application needs, with proper error handling and version checking.
#'
#' @param mode Character string indicating the application mode. Valid options:
#'   "APP_MODE" (default): Production environment, minimal package set
#'   "UPDATE_MODE": Data update environment, includes data processing packages
#'   "DEV_MODE": Development environment, includes all packages
#'   "TEST_MODE": Testing environment, includes testing packages
#' @param verbose Logical. If TRUE, displays detailed loading information
#' @param force_update Logical. If TRUE, attempts to update outdated packages
#' @return Named list of loaded packages grouped by category
#'
#' @examples
#' # Initialize packages for production
#' available_packages <- initialize_packages("APP_MODE")
#'
#' # Initialize packages for development with verbose output
#' available_packages <- initialize_packages("DEV_MODE", verbose = TRUE)
initialize_packages <- function(mode = "APP_MODE", 
                               verbose = FALSE, 
                               force_update = FALSE) {
  
  # ---- parameter normalization -------------------------------------------
  verbose <- isTRUE(verbose)

  # ---- dependency check ---------------------------------------------------
  if (!exists("library2", mode = "function")) {
    stop("library2() is required but not loaded", call. = FALSE)
  }
  
  # ---- acceleration level removed per #204 ---------------------------------
  # ACCELERATION_LEVEL was dead code; future perf config via app_config.yaml

  # Disable duckplyr fallback telemetry/config writes in non-interactive script
  # environments. This avoids permission errors when UPDATE_MODE runs under
  # restricted sandboxes or service accounts.
  Sys.setenv(
    DUCKPLYR_FALLBACK_COLLECT = "0",
    DUCKPLYR_FALLBACK_AUTOUPLOAD = "0",
    DUCKPLYR_FALLBACK_INFO = "0",
    DUCKPLYR_FALLBACK_VERBOSE = "0"
  )
  
  # Initialize package tracking
  loaded_packages <- list()
  failed_packages <- character(0)
  
  # Define package groups based on functionality
  # These will be filled based on mode
  core_packages <- character(0)
  db_packages <- character(0)
  ui_packages <- character(0)
  processing_packages <- character(0)
  dev_packages <- character(0)
  test_packages <- character(0)
  
  # Log helper function
  log_message <- function(msg) {
    if (verbose) message(msg)
  }
  
  # Load package helper that processes a single package category
  load_package_group <- function(pkg_group, group_name) {
    if (length(pkg_group) == 0) return(character(0))
    
    log_message(paste("Loading", group_name, "packages..."))
    successful <- character(0)
    
    for (pkg in pkg_group) {
      tryCatch({
        library2(pkg, force_update = force_update)
        successful <- c(successful, pkg)
      }, error = function(e) {
        message(paste("ERROR loading package:", pkg, "-", e$message))
        failed_packages <<- c(failed_packages, pkg)
      })
    }
    
    log_message(paste("Successfully loaded", length(successful), "of", length(pkg_group), group_name, "packages"))
    return(successful)
  }
  
  # Define core packages needed in all modes
  # UX_P001: In APP_MODE, async packages deferred to first use
  if (mode == "APP_MODE") {
    core_packages <- c(
      "stringr",      # String manipulation
      "glue",         # String templating
      "tools",        # File utilities
      "yaml",         # YAML parsing
      "dotenv"        # Environment variables
      # "future", "future.mirai", "promises", "later" — deferred (UX_P001)
    )
  } else {
    core_packages <- c(
      "stringr",      # String manipulation
      "glue",         # String templating
      "tools",        # File utilities
      "yaml",         # YAML parsing
      "future",       # Future framework for async
      "future.mirai", # Mirai backend for future
      "promises",     # Promise handling for async
      "later",        # Later for event loop
      "dotenv"        # Environment variables
    )
  }

  # Define database packages needed in all modes
  # UX_P001: In APP_MODE, arrow deferred to first use
  if (mode == "APP_MODE") {
    db_packages <- c(
      "DBI",          # Database interface
      "duckdb"        # DuckDB database
      # "arrow" — deferred (UX_P001)
    )
  } else {
    db_packages <- c(
      "DBI",          # Database interface
      "duckdb",       # DuckDB database
      "arrow"         # Apache Arrow format
    )
  }
  
  # Add mode-specific packages
  if (mode == "APP_MODE") {
    # Production environment - UI focused
    # UX_P001: Only startup-essential packages loaded here
    ui_packages <- c(
      "shiny",         # Shiny framework
      "bs4Dash",       # Bootstrap 4 dashboard
      "bslib",         # Bootstrap library
      "shinyjs",       # JavaScript integration
      "shinyWidgets",  # Enhanced widgets
      "htmltools",     # HTML manipulation
      "fontawesome",   # Font icons — needed at UI build time for icon()
      "DT",            # Data tables
      "plotly"         # Interactive plots — needed at UI build time for plotlyOutput()
    )

    # Minimal data processing in APP_MODE
    processing_packages <- c(
      "dplyr",        # Data manipulation
      "tidyr",        # Data tidying
      "readr"         # Data reading
    )
    # UX_P001: Deferred processing packages — loaded on first use via ensure_package()
    # "readxl"  → Excel reading, rarely needed at runtime
    # "scales"  → Scales for visualization
    # "fs"      → File system utilities
    # "janitor" → Data cleaning
  } 
  else if (mode == "UPDATE_MODE") {
    # Data update environment - processing focused
    processing_packages <- c(
      "dplyr",        # Data manipulation
      "tidyr",        # Data tidying
      "readr",        # Data reading
      "readxl",       # Excel reading
      "openxlsx",     # Excel writing
      "data.table",   # Fast data manipulation
      "jsonlite",     # JSON handling
      "lubridate",    # Date manipulation
      "scales",       # Scales for visualization
      "purrr",        # Functional programming
      "tidyverse",    # Full tidyverse suite
      "httr2",        # Modern HTTP requests
      "fastDummies",  # Create dummy variables
      "dbplyr",       # dplyr for databases
      "duckplyr",     # Integrate DuckDB with dplyr
      "psych",        # Psychometrics
      "furrr",        # Parallel processing
      "dtplyr",       # data.table + dplyr
      "caret",        # Machine learning tools
      "progressr",    # Progress reporting
      "googledrive",  # Google Drive access
      "googlesheets4" # Google Sheets access
    )

    # Minimal UI in UPDATE_MODE
    ui_packages <- c(
      "shiny",       # Basic Shiny
      "bslib",       # Bootstrap UI
      "plotly",      # Interactive plots
      "shinyjs",     # JavaScript helpers
      "htmltools",   # HTML helpers
      "fontawesome", # Icons
      "DT"           # Data tables
    )
  }
  else if (mode == "DEV_MODE") {
    # Development environment - comprehensive package set
    ui_packages <- c(
      "shiny",         # Shiny framework
      "bs4Dash",       # Bootstrap 4 dashboard
      "bslib",         # Bootstrap library
      "shinyjs",       # JavaScript integration
      "shinyWidgets",  # Enhanced widgets
      "htmltools",     # HTML manipulation
      "fontawesome",   # Font icons
      "DT",            # Data tables
      "plotly",        # Interactive plots
      "ggplot2",       # Static plots
      "leaflet"        # Maps
    )
    
    processing_packages <- c(
      "dplyr",        # Data manipulation
      "tidyr",        # Data tidying
      "readr",        # Data reading
      "readxl",       # Excel reading
      "openxlsx",     # Excel writing
      "data.table",   # Fast data manipulation
      "jsonlite",     # JSON handling
      "lubridate",    # Date manipulation
      "scales",       # Scales for visualization
      "purrr",        # Functional programming
      "tidyverse",    # Full tidyverse suite
      "R6",           # R6 classes
      "httr",         # HTTP requests
      "xml2"          # XML parsing
    )
    
  dev_packages <- c(
      "devtools",     # Development tools
      "roxygen2",     # Documentation
      "testthat",     # Testing
      "usethis",      # Package development
      "profvis",      # Profiling
      "bench",        # Benchmarking
      "lintr",        # Code linting
      "styler"        # Code styling
    )
  }
  else if (mode == "GLOBAL_MODE") {
    # Global environment - includes development and collaboration tools
    ui_packages <- c(
      "shiny",         # Shiny framework
      "bs4Dash",       # Bootstrap dashboard
      "bslib",         # Bootstrap library
      "shinyjs",       # JavaScript integration
      "shinyWidgets",  # Enhanced widgets
      "htmltools",     # HTML manipulation
      "fontawesome",   # Font icons
      "DT",            # Data tables
      "plotly",        # Interactive plots
      "ggplot2"        # Static plots
    )

    processing_packages <- c(
      "dplyr",        # Data manipulation
      "tidyr",        # Data tidying
      "readr",        # Data reading
      "readxl",       # Excel reading
      "openxlsx",     # Excel writing
      "data.table",   # Fast data manipulation
      "jsonlite",     # JSON handling
      "lubridate",    # Date manipulation
      "scales",       # Scales for visualization
      "purrr",        # Functional programming
      "tidyverse",    # Full tidyverse suite
      "R6",           # R6 classes
      "httr",         # HTTP requests
      "xml2",         # XML parsing
      "dbplyr",       # dplyr for databases
      "duckplyr",     # Integrate DuckDB with dplyr
      "fastDummies",  # Create dummy variables
      "dtplyr",       # data.table + dplyr
      "psych"         # Psychometrics
    )

    dev_packages <- c(
      "devtools",     # Development tools
      "roxygen2",     # Documentation
      "pkgdown",      # Package websites
      "testthat",     # Testing
      "bench",        # Benchmarking
      "profvis",      # Profiling
      "lintr",        # Code linting
      "covr",         # Code coverage
      "usethis",      # Workflow helpers
      "git2r",        # Git interface
      "knitr",        # Reporting
      "rmarkdown",    # R Markdown
      "markdown",     # Markdown processing
      "googledrive",  # Google Drive
      "googlesheets4",# Google Sheets
      "styler"        # Code styling
    )
  }
  else if (mode == "TEST_MODE") {
    # Testing environment - testing focused
    ui_packages <- c(
      "shiny",       # Shiny framework
      "bs4Dash",     # Bootstrap 4 dashboard
      "shinytest2"   # Shiny testing
    )
    
    processing_packages <- c(
      "dplyr",      # Data manipulation
      "tidyr",      # Data tidying
      "readr",      # Data reading
      "purrr"       # Functional programming
    )
    
    test_packages <- c(
      "testthat",      # Testing framework
      "shinytest2",    # Shiny testing
      "mockery",       # Mocking
      "covr"           # Code coverage
    )
  }
  else {
    warning("Unknown mode: ", mode, ". Using APP_MODE packages.")
    mode <- "APP_MODE"
    
    ui_packages <- c(
      "shiny",         # Shiny framework
      "bs4Dash",       # Bootstrap 4 dashboard
      "bslib",         # Bootstrap library
      "shinyjs",       # JavaScript integration
      "shinyWidgets",  # Enhanced widgets
      "htmltools",     # HTML manipulation
      "DT",            # Data tables
      "plotly"         # Interactive plots
    )
    
    processing_packages <- c(
      "dplyr",        # Data manipulation
      "tidyr",        # Data tidying
      "readr",        # Data reading
      "readxl"        # Excel reading
    )
  }
  
  # Load packages in a specific order
  loaded_packages$core <- load_package_group(core_packages, "core")
  loaded_packages$db <- load_package_group(db_packages, "database")
  loaded_packages$processing <- load_package_group(processing_packages, "data processing")
  loaded_packages$ui <- load_package_group(ui_packages, "UI")
  
  # Load optional package groups if defined for the current mode
  if (length(dev_packages) > 0) {
    loaded_packages$dev <- load_package_group(dev_packages, "development")
  }
  
  if (length(test_packages) > 0) {
    loaded_packages$test <- load_package_group(test_packages, "testing")
  }

  # Set aliases for commonly used functions to avoid namespace conflicts
  if ("dplyr" %in% unlist(loaded_packages)) {
    select <- dplyr::select
    filter <- dplyr::filter
    if (verbose) message("Set dplyr function aliases: select, filter")
  }
  
  # Create a flag to indicate initialization is complete
  PACKAGES_INITIALIZED <- list(
    mode = mode,
    timestamp = Sys.time(),
    counts = sapply(loaded_packages, length)
  )
  
  # Assign to global environment so other scripts can check
  assign("PACKAGES_INITIALIZED", PACKAGES_INITIALIZED, envir = .GlobalEnv)
  assign("AVAILABLE_PACKAGES", loaded_packages, envir = .GlobalEnv)
  
  # Summary
  total_loaded <- sum(sapply(loaded_packages, length))
  total_failed <- length(failed_packages)
  
  message(paste0(
    "Package initialization complete for ", mode, ":\n",
    "- ", total_loaded, " packages loaded successfully\n",
    if(total_failed > 0) paste0("- ", total_failed, " packages failed to load\n") else ""
  ))
  
  return(loaded_packages)
}
