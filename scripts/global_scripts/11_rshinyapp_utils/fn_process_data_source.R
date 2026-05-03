#' Process Data Source Specification
#'
#' This utility function processes various data source specification formats
#' into a standardized structure. It handles all five YAML configuration patterns:
#' 1. Simple String Format
#' 2. Array Format
#' 3. Object Format with Roles
#' 4a. Single Data Source with Parameters
#' 4b. Multiple Data Sources with Roles and Parameters
#'
#' @param data_source The data source specification, which can be:
#'   - NULL: Will return empty data frames for all tables
#'   - String: A single table/query name (Pattern 1: e.g., "customer_details")
#'   - Array: Multiple related tables in specific order (Pattern 2)
#'   - Object: Multiple tables with specific roles (Pattern 3: e.g., {primary: "table1", history: "table2"})
#'   - Object with data_source: Single data source with parameters (Pattern 4a)
#'   - Object with roles and parameters: Multiple data sources with parameters (Pattern 4b)
#' @param table_names A character vector of expected table role names.
#'   The first name is used for the primary table when a string data_source is provided.
#'   Default: c("primary", "secondary", "tertiary")
#' @param get_table_func A function that takes a table name and returns a data frame.
#'   If NULL, a mock function that returns empty data frames is used.
#'   Default: NULL
#'
#' @return A named list with standardized table entries and parameters
#' @export
#'
#' @examples
#' # Pattern 1: Simple String Format
#' result <- processDataSource("customers")
#' # result$data$primary will contain data from "customers"
#'
#' # Pattern 2: Array Format
#' result <- processDataSource(c("customers", "orders", "products"))
#' # result$data$primary will contain "customers"
#' # result$data$secondary will contain "orders"
#'
#' # Pattern 3: Object Format with Roles
#' result <- processDataSource(list(
#'   primary = "customers_table",
#'   history = "orders_table"
#' ))
#' # result$data$primary will contain "customers_table"
#' # result$data$history will contain "orders_table"
#'
#' # Pattern 4a: Single Data Source with Parameters
#' result <- processDataSource(list(
#'   data_source = "customers_table",
#'   parameters = list(show_kpi = TRUE, refresh_interval = 300)
#' ))
#' # result$data$primary will contain "customers_table"
#' # result$parameters$show_kpi will be TRUE
#'
#' # Pattern 4b: Multiple Data Sources with Roles and Parameters
#' result <- processDataSource(list(
#'   primary = "customers_table",
#'   history = "orders_table",
#'   parameters = list(visualization_type = "tree")
#' ))
#' # result$data$primary will contain "customers_table"
#' # result$data$history will contain "orders_table"
#' # result$parameters$visualization_type will be "tree"
processDataSource <- function(data_source = NULL, 
                              table_names = c("primary", "secondary", "tertiary"),
                              get_table_func = NULL) {
  
  # Default get_table function that returns empty data frames
  if (is.null(get_table_func)) {
    get_table_func <- function(table_name) {
      message("No get_table function provided, returning empty data frame for: ", table_name)
      return(data.frame())
    }
  }
  
  # Initialize result with empty data frames for all expected tables and empty parameters
  result <- list(
    data = list(),
    parameters = list()
  )
  
  for (name in table_names) {
    result$data[[name]] <- data.frame()
  }
  
  # If no data source, return the initialized empty result
  if (is.null(data_source)) {
    return(result)
  }
  
  # Pattern 1: If string, it's just a single table for the primary role
  if (is.character(data_source) && length(data_source) == 1) {
    tryCatch({
      result$data[[table_names[1]]] <- get_table_func(data_source)
    }, error = function(e) {
      message("Error loading table ", data_source, ": ", e$message)
    })
    return(result)
  }
  
  # Pattern 2: If array, it's multiple tables in order of table_names
  if (is.character(data_source) && length(data_source) > 1) {
    for (i in seq_along(data_source)) {
      if (i <= length(table_names)) {
        tryCatch({
          result$data[[table_names[i]]] <- get_table_func(data_source[i])
        }, error = function(e) {
          message("Error loading table ", data_source[i], ": ", e$message)
        })
      }
    }
    return(result)
  }
  
  # Handle Pattern 3, 4a, and 4b (all list formats)
  if (is.list(data_source)) {
    # Check if this is Pattern 4a (has data_source and parameters keys)
    if (!is.null(data_source$data_source)) {
      # Pattern 4a: Single data source with parameters
      tryCatch({
        result$data[[table_names[1]]] <- get_table_func(data_source$data_source)
      }, error = function(e) {
        message("Error loading table ", data_source$data_source, ": ", e$message)
      })
      
      # Extract parameters if they exist
      if (!is.null(data_source$parameters) && is.list(data_source$parameters)) {
        result$parameters <- data_source$parameters
      }
      
      return(result)
    }
    
    # Check for parameters (could be Pattern 4b)
    params <- data_source$parameters
    if (!is.null(params) && is.list(params)) {
      result$parameters <- params
      # Remove parameters from data_source to process the rest as Pattern 3
      data_source$parameters <- NULL
    }
    
    # Process remaining products as Pattern 3 (object with roles)
    for (name in names(data_source)) {
      # Skip parameters field which we already processed
      if (name == "parameters") next
      
      # If the name exists in our expected table_names, use it
      target_name <- name
      if (!(name %in% table_names)) {
        # Log when a role isn't in expected table_names but still use it
        message("Note: Table role '", name, "' wasn't in expected table_names but will be used")
      }
      
      tryCatch({
        result$data[[target_name]] <- get_table_func(data_source[[name]])
      }, error = function(e) {
        message("Error loading table ", data_source[[name]], ": ", e$message)
      })
    }
    
    return(result)
  }
  
  # If we get here, the data_source is an unsupported format
  message("Unsupported data_source format: ", class(data_source))
  return(result)
}