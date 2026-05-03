#' Data Availability Detection Package
#'
#' This file loads all functions in the data availability detection module.
#' It follows the pattern described in R56 (Folder-based Sourcing).
#'
#' @author Claude
#' @date 2025-04-08
#' @implements MP45 Automatic Data Availability Detection
#' @implements R56 Folder-based Sourcing

# Get the directory where this file is located
current_dir <- dirname(parent.frame(2)$ofile)

# List all function files in this directory
function_files <- list.files(
  path = current_dir,
  pattern = "^fn_.*\\.R$",
  full.names = TRUE
)

# Source each function file
for (file in function_files) {
  source(file)
}

# Export function to check if all functions are loaded
check_data_availability_module <- function() {
  required_functions <- c(
    "connect_to_app_database",
    "detect_marketing_channel_availability",
    "load_platform_dictionary",
    "initialize_data_availability",
    "is_available",
    "render_adaptive_radio_buttons"
  )
  
  loaded <- sapply(required_functions, exists, envir = .GlobalEnv)
  
  if (all(loaded)) {
    message("Data availability detection module loaded successfully")
    return(TRUE)
  } else {
    missing <- required_functions[!loaded]
    warning("Some data availability functions are missing: ", 
           paste(missing, collapse = ", "))
    return(FALSE)
  }
}