#' Access Data from Processing Layer
#'
#' This function accesses data from the processing layer directories (processed_data,
#' cleansed_data, raw_data, intermediate_data) following the Data Source Hierarchy
#' Principle. Access is restricted based on the current operating mode.
#'
#' @param file_name The name of the file to read
#' @param data_type The type of processing data ("processed", "cleansed", "raw", "intermediate")
#' @param write_mode Whether to open in write mode (restricted by operating mode)
#' @param required Whether the file is required (throws error if missing)
#' @param default Default value to return if file is missing and not required
#'
#' @return The data read from the file
#' @export
#'
#' @examples
#' # Read processed data (only works in UPDATE_MODE or GLOBAL_MODE)
#' customer_segments <- access_processing_data("customer_segments.csv", "processed")
#' 
#' # Read cleansed data with a default if missing
#' sales_data <- access_processing_data("sales.csv", "cleansed", required = FALSE,
#'                                     default = data.frame())
access_processing_data <- function(file_name, data_type = c("processed", "cleansed", "raw", "intermediate"),
                                   write_mode = FALSE, required = TRUE, default = NULL) {
  # Validate data_type parameter
  data_type <- match.arg(data_type)
  
  # Map data_type to directory name
  dir_map <- list(
    "processed" = "processed_data",
    "cleansed" = "cleansed_data", 
    "raw" = "raw_data",
    "intermediate" = "intermediate_data"
  )
  
  directory <- dir_map[[data_type]]
  path <- file.path(directory, file_name)
  
  # Check access permissions based on Data Source Hierarchy Principle
  access_type <- if (write_mode) "write" else "read"
  
  if (!check_data_access("processing", access_type, path)) {
    stop(access_type, " access to ", directory, " not allowed in current operating mode")
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