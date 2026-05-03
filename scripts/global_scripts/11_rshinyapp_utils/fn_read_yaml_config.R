#' Read YAML Configuration [DEPRECATED]
#'
#' DEPRECATED: This function has been replaced by load_app_config in 04_utils.
#' This is now just a wrapper for backward compatibility.
#' Please use load_app_config instead as it provides more comprehensive functionality.
#'
#' @param yaml_file Either the full path to the YAML file or just the filename
#' @param base_path Base path to prepend to yaml_file (default: NULL, meaning yaml_file is the full path)
#'
#' @return The configuration as an R list
#' @export
#'
#' @examples
#' # Read configuration from a file in the app_configs directory
#' config <- readYamlConfig("customer_dna_app.yaml", "app_configs")
#' 
#' # Read configuration using full path
#' config <- readYamlConfig("app_config.yaml")
readYamlConfig <- function(yaml_file, base_path = NULL) {
  # Display deprecation warning
  warning("DEPRECATED: readYamlConfig is deprecated. Please use load_app_config from 04_utils instead.")
  
  # Determine the yaml path
  if (is.null(base_path)) {
    # If base_path is NULL, assume yaml_file is the full path
    yaml_path <- yaml_file
  } else {
    # Construct the full path to the YAML file
    yaml_path <- file.path(base_path, yaml_file)
  }
  
  # Check if load_app_config is available
  if (file.exists(file.path(GLOBAL_DIR, "04_utils", "fn_load_app_config.R"))) {
    # Source the file if not already loaded
    if (!exists("load_app_config", mode = "function")) {
      source(file.path(GLOBAL_DIR, "04_utils", "fn_load_app_config.R"))
    }
    
    # Call load_app_config with only the config loading, not the parameter processing
    # This maintains compatibility with the original function
    return(load_app_config(config_path = yaml_path, load_parameters = FALSE))
  } else {
    # If load_app_config is not available, fall back to original implementation
    message("WARNING: Could not find load_app_config. Using deprecated implementation.")
    
    # Validate that the file exists
    if (!file.exists(yaml_path)) {
      warning("Configuration file not found: ", yaml_path)
      return(list())
    }
    
    message("Loading configuration from: ", yaml_path)
    
    # Check if yaml package is available
    if (requireNamespace("yaml", quietly = TRUE)) {
      tryCatch({
        # Use yaml package to read the file
        config <- yaml::read_yaml(yaml_path)
        return(config)
      }, error = function(e) {
        warning("Error reading YAML file: ", e$message, 
                ". Falling back to default configuration.")
        return(list())
      })
    } else {
      message("yaml package not available; returning empty configuration.")
      return(list())
    }
  }
}