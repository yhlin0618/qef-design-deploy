#' Read Data from App-Specific Data Directory
#'
#' This function reads data from the app-specific app_data directory, following
#' the Data Source Hierarchy Principle which prioritizes app-specific data.
#' Access permissions are enforced based on the current operating mode.
#'
#' @param file_name The name of the file to read
#' @param subdir Optional subdirectory within app_data directory
#' @param required Whether the file is required. If TRUE and the file doesn't exist, throws an error
#' @param default Default data to return if the file doesn't exist and required is FALSE
#' @param write_mode Whether to open the file in write mode (only allowed in UPDATE_MODE and GLOBAL_MODE)
#'
#' @return The data read from the file, with appropriate type based on file extension
#' @export
#'
#' @examples
#' # Read from the root of app_data
#' customer_data <- read_from_app_data("customer_segments.csv")
#'
#' # Read from a subdirectory
#' type1_data <- read_from_app_data("historical_data.csv", subdir = "scd_type1")
#'
#' # Provide a default if file might not exist
#' optional_data <- read_from_app_data("optional_config.yaml", required = FALSE, 
#'                                    default = list(feature_enabled = FALSE))
read_from_app_data <- function(file_name, subdir = NULL, required = TRUE, default = NULL, write_mode = FALSE) {
  # Check if access is allowed based on the Data Source Hierarchy Principle
  access_type <- if (write_mode) "write" else "read"
  
  # Construct the path for access checking
  path <- if (is.null(subdir)) {
    file.path("app_data", file_name)
  } else {
    file.path("app_data", subdir, file_name)
  }
  
  # Check access permissions
  if (!check_data_access("app", access_type, path)) {
    stop(access_type, " access to app_data not allowed in current operating mode")
  }
  # Construct the full path
  path <- if (is.null(subdir)) {
    file.path("app_data", file_name)
  } else {
    file.path("app_data", subdir, file_name)
  }
  
  # Check if file exists
  if (!file.exists(path)) {
    if (required) {
      stop("Required file not found: ", path)
    } else {
      message("Optional file not found, using default: ", path)
      return(default)
    }
  }
  
  # Read the file based on extension
  tryCatch({
    if (endsWith(tolower(file_name), ".csv")) {
      data <- read.csv(path, stringsAsFactors = FALSE)
    } else if (endsWith(tolower(file_name), ".rds")) {
      data <- readRDS(path)
    } else if (endsWith(tolower(file_name), ".json")) {
      data <- jsonlite::fromJSON(path)
    } else if (endsWith(tolower(file_name), ".yaml") || 
               endsWith(tolower(file_name), ".yml")) {
      # You'll need the yaml package: install.packages("yaml")
      if (requireNamespace("yaml", quietly = TRUE)) {
        data <- yaml::read_yaml(path)
      } else {
        stop("The yaml package is required to read YAML files")
      }
    } else if (endsWith(tolower(file_name), ".xlsx") || 
               endsWith(tolower(file_name), ".xls")) {
      if (requireNamespace("readxl", quietly = TRUE)) {
        data <- readxl::read_excel(path)
      } else {
        stop("The readxl package is required to read Excel files")
      }
    } else {
      # For other file types, try a basic file read
      warning("File type not explicitly supported, using readLines: ", file_name)
      data <- readLines(path)
    }
    
    return(data)
  }, error = function(e) {
    if (required) {
      stop("Error reading file ", path, ": ", e$message)
    } else {
      message("Error reading optional file, using default: ", path, " - ", e$message)
      return(default)
    }
  })
}