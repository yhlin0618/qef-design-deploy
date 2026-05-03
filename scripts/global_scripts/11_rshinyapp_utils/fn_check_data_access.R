#' Check Data Access Permissions Based on Operating Mode
#'
#' This utility function checks whether the current operating mode has
#' the required permissions to access a specific data source. It implements
#' the Data Source Hierarchy Principle's mode-specific access rules.
#'
#' @param data_layer The data layer to check access for ("app", "processing", "global", "development", "external")
#' @param access_type The type of access requested ("read", "write")
#' @param data_path Optional specific path to check
#'
#' @return TRUE if access is allowed, FALSE otherwise. Also prints a warning or error message.
#' @export
#'
#' @examples
#' # Check if current mode can write to app_data
#' if (check_data_access("app", "write", "app_data")) {
#'   # Proceed with write operation
#' }
#'
#' # Check if current mode can access processing layer
#' if (!check_data_access("processing", "read")) {
#'   stop("Cannot access processing data in current mode")
#' }
check_data_access <- function(data_layer, access_type, data_path = NULL) {
  # Default to current operation mode, if set
  if (!exists("OPERATION_MODE")) {
    warning("OPERATION_MODE not set. Assuming UPDATE_MODE for safety.")
    current_mode <- "UPDATE_MODE"
  } else {
    current_mode <- OPERATION_MODE
  }
  
  # Define access rules based on the Data Source Hierarchy Principle
  access_rules <- list(
    "APP_MODE" = list(
      "app" = c("read"),
      "global" = c("read"),
      "processing" = c(),
      "development" = c(),
      "external" = c()
    ),
    "UPDATE_MODE" = list(
      "app" = c("read", "write"),
      "global" = c("read"),
      "processing" = c("read", "write"),
      "development" = c("read", "write"),
      "external" = c("read")
    ),
    "GLOBAL_MODE" = list(
      "app" = c("read", "write"),
      "global" = c("read", "write"),
      "processing" = c("read", "write"),
      "development" = c("read", "write"),
      "external" = c("read")
    )
  )
  
  # Check if the operation mode exists in our rules
  if (!current_mode %in% names(access_rules)) {
    warning("Unknown operation mode: ", current_mode, ". Defaulting to restricted permissions.")
    return(FALSE)
  }
  
  # Check if the data layer exists in our rules
  if (!data_layer %in% names(access_rules[[current_mode]])) {
    warning("Unknown data layer: ", data_layer)
    return(FALSE)
  }
  
  # Check if the access type is allowed for this mode and layer
  allowed_access <- access_rules[[current_mode]][[data_layer]]
  access_allowed <- access_type %in% allowed_access
  
  # Log access attempts based on result
  if (!access_allowed) {
    if (is.null(data_path)) {
      warning(access_type, " access to ", data_layer, " layer not allowed in ", current_mode)
    } else {
      warning(access_type, " access to ", data_path, " (", data_layer, " layer) not allowed in ", current_mode)
    }
  }
  
  return(access_allowed)
}

#' Determine Data Layer from Path
#'
#' Helper function to determine which data layer a given path belongs to.
#'
#' @param path The file or directory path to check
#'
#' @return String indicating the data layer ("app", "processing", "global", "development", "external")
#' @export
#'
#' @examples
#' # Check which data layer a path belongs to
#' layer <- get_data_layer("app_data/customer_segments.csv")
#' # Returns "app"
get_data_layer <- function(path) {
  # Define patterns for each data layer
  patterns <- list(
    # App layer
    "app" = c("^app_data/", "^Data\\.duckdb$", "^app_configs/"),
    
    # Processing layer
    "processing" = c("^processed_data/", "^cleansed_data/", "^raw_data/", "^intermediate_data/"),
    
    # Global layer
    "global" = c("^update_scripts/global_scripts/30_global_data/", "^reference_data/"),
    
    # Development layer
    "development" = c("^test_data/", "^mock_data/"),
    
    # External layer (less specific patterns, so check last)
    "external" = c("^https?://", "^ftp://", "^s3://")
  )
  
  # Normalize path for consistent matching
  normalized_path <- gsub("\\\\", "/", path)
  
  # Check each layer's patterns
  for (layer_name in names(patterns)) {
    layer_patterns <- patterns[[layer_name]]
    for (pattern in layer_patterns) {
      if (grepl(pattern, normalized_path)) {
        return(layer_name)
      }
    }
  }
  
  # If no match found, default to processing (safer assumption)
  warning("Could not determine data layer for path: ", path, ". Assuming 'processing' layer.")
  return("processing")
}