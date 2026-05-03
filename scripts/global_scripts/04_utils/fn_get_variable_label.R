#' Get Chinese label for variable names
#' 
#' @description
#' Translates technical variable names to Chinese labels for display in UI.
#' Uses the chinese_labels.yaml configuration file.
#' 
#' @param var_names character vector. Variable names to translate.
#' @param config_path character. Path to the YAML configuration file 
#'        (default: searches common locations).
#' @param fallback_to_original logical. If TRUE, returns original name when 
#'        translation not found (default: TRUE).
#' @param format_unknown logical. If TRUE, formats unknown variables to be 
#'        more readable (default: TRUE).
#' 
#' @return character vector. Chinese labels for the variables.
#' 
#' @examples
#' \dontrun{
#' # Single variable
#' label <- get_variable_label("use_oil")  # Returns "使用油"
#' 
#' # Multiple variables
#' labels <- get_variable_label(c("use_oil", "price", "unknown_var"))
#' 
#' # With custom formatting for unknown
#' labels <- get_variable_label("my_custom_var", format_unknown = TRUE)
#' # Returns "My Custom Var" (formatted)
#' }
#' 
#' @export
get_variable_label <- function(var_names, 
                              config_path = NULL,
                              fallback_to_original = TRUE,
                              format_unknown = TRUE) {
  
  # Load required packages
  if (!requireNamespace("yaml", quietly = TRUE)) {
    warning("Package 'yaml' is required but not installed. Returning original names.")
    return(var_names)
  }
  
  # Set default config path
  if (is.null(config_path)) {
    # Try to find the config file in common locations
    possible_paths <- c(
      "scripts/global_scripts/30_global_data/parameters/scd_type2/chinese_labels.yaml",
      "global_scripts/30_global_data/parameters/scd_type2/chinese_labels.yaml",
      "30_global_data/parameters/scd_type2/chinese_labels.yaml",
      "../30_global_data/parameters/scd_type2/chinese_labels.yaml",
      "../../30_global_data/parameters/scd_type2/chinese_labels.yaml",
      file.path(Sys.getenv("APP_ROOT", "."),
                "scripts/global_scripts/30_global_data/parameters/scd_type2/chinese_labels.yaml")
    )
    
    config_path <- NULL
    for (path in possible_paths) {
      if (file.exists(path)) {
        config_path <- path
        break
      }
    }
    
    if (is.null(config_path)) {
      warning("Could not find chinese_labels.yaml. Returning original names.")
      return(var_names)
    }
  }
  
  # Load configuration with caching
  labels <- tryCatch({
    # Check if already loaded in global environment
    if (exists(".variable_chinese_labels_cache", envir = .GlobalEnv)) {
      get(".variable_chinese_labels_cache", envir = .GlobalEnv)
    } else {
      # Load and cache
      loaded_labels <- yaml::read_yaml(config_path)
      assign(".variable_chinese_labels_cache", loaded_labels, envir = .GlobalEnv)
      loaded_labels
    }
  }, error = function(e) {
    warning(paste("Error loading label config file:", e$message))
    return(NULL)
  })
  
  if (is.null(labels)) {
    return(var_names)
  }
  
  # Function to format unknown variable names
  format_variable_name <- function(var_name) {
    if (!format_unknown) {
      return(var_name)
    }
    
    # Replace underscores with spaces and capitalize
    formatted <- gsub("_", " ", var_name)
    formatted <- gsub("\\b([a-z])", "\\U\\1", formatted, perl = TRUE)
    
    # Handle common abbreviations
    formatted <- gsub("\\bId\\b", "ID", formatted)
    formatted <- gsub("\\bUrl\\b", "URL", formatted)
    formatted <- gsub("\\bApi\\b", "API", formatted)
    formatted <- gsub("\\bRoi\\b", "ROI", formatted)
    
    return(formatted)
  }
  
  # Translate each variable name
  result <- sapply(var_names, function(var_name) {
    # Search through all categories (except metadata and usage)
    for (category in names(labels)) {
      if (category %in% c("metadata", "usage")) next
      
      if (!is.null(labels[[category]]) && var_name %in% names(labels[[category]])) {
        label <- labels[[category]][[var_name]]
        # Add original name in parentheses for clarity
        return(paste0(label, " (", var_name, ")"))
      }
    }
    
    # No translation found
    if (fallback_to_original) {
      return(format_variable_name(var_name))
    } else {
      return(NA_character_)
    }
  }, USE.NAMES = FALSE)
  
  return(result)
}

#' Get variable labels for a data frame
#' 
#' @description
#' Translates all column names in a data frame to Chinese labels.
#' 
#' @param data data.frame. Data frame whose column names to translate.
#' @param add_as_attribute logical. If TRUE, adds labels as an attribute 
#'        to the data frame (default: FALSE).
#' 
#' @return If add_as_attribute is TRUE, returns the data frame with labels 
#'         attribute. Otherwise returns a named vector of labels.
#' 
#' @examples
#' \dontrun{
#' # Get labels for all columns
#' labels <- get_dataframe_labels(my_data)
#' 
#' # Add labels as attribute
#' my_data <- get_dataframe_labels(my_data, add_as_attribute = TRUE)
#' attr(my_data, "variable.labels")  # Access the labels
#' }
#' 
#' @export
get_dataframe_labels <- function(data, add_as_attribute = FALSE) {
  if (!is.data.frame(data)) {
    stop("Input must be a data frame")
  }
  
  # Get labels for all column names
  col_names <- names(data)
  labels <- get_variable_label(col_names)
  
  # Create named vector
  names(labels) <- col_names
  
  if (add_as_attribute) {
    attr(data, "variable.labels") <- labels
    return(data)
  } else {
    return(labels)
  }
}