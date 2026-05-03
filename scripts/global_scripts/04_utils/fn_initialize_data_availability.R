#' Check data dimension availability
#'
#' @description
#' Systematic data dimension availability checking following MP45
#'
#' @param conn Database connection
#' @param dimension Name of the dimension being checked
#' @param table_name Name of the table to check for this dimension
#' @return TRUE if dimension is available, FALSE otherwise
#' @export
check_data_dimension <- function(conn, dimension, table_name) {
  tryCatch({
    # Skip check if database connection is invalid
    if (is.null(conn) || !DBI::dbIsValid(conn)) {
      return(FALSE)
    }
    
    # Check if table exists
    tables <- DBI::dbListTables(conn)
    if (!(table_name %in% tables)) {
      message("Table not found for dimension ", dimension, ": ", table_name)
      return(FALSE)
    }
    
    # Check if table has data
    count_query <- paste0("SELECT COUNT(*) AS count FROM ", table_name)
    result <- DBI::dbGetQuery(conn, count_query)
    has_data <- !is.null(result) && nrow(result) > 0 && result$count[1] > 0
    
    message("Dimension ", dimension, " availability: ", has_data, " (rows: ", 
            if(has_data) result$count[1] else 0, ")")
    return(has_data)
  }, error = function(e) {
    message("Error checking data dimension ", dimension, ": ", e$message)
    return(FALSE)
  })
}

#' Check if data is available
#'
#' @description
#' Global function to check data availability from the availability registry
#'
#' @param domain The domain to check (e.g., "database", "channel", "dimension")
#' @param dimension Optional specific dimension within the domain
#' @return TRUE if data is available, FALSE otherwise
#' @export
is_data_available <- function(domain, dimension = NULL) {
  if (!exists("availability_registry")) {
    return(FALSE)
  }
  
  # If no specific dimension requested, check if domain has any available dimensions
  if (is.null(dimension)) {
    return(!is.null(availability_registry[[domain]]) && 
           any(unlist(availability_registry[[domain]], recursive = TRUE)))
  }
  
  # Check specific dimension
  return(!is.null(availability_registry[[domain]]) && 
         !is.null(availability_registry[[domain]][[dimension]]) && 
         availability_registry[[domain]][[dimension]])
}