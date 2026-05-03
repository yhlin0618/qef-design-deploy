#' Initialize the app database connection and register its availability in the system registry
#'
#' @description
#' This function establishes a connection to the app database and registers
#' its availability in the system registry. It handles connection errors
#' gracefully and updates the availability registry appropriately.
#'
#' @param db_path Path to the app database file
#' @return The database connection object if successful, NULL otherwise
#' @export
initialize_app_database <- function(db_path = "data/app_data/app_data.duckdb") {
  # Use <<- to modify the global variable
  app_data_conn <<- NULL
  
  tryCatch({
    app_data_conn <<- connect_to_app_database(db_path)
    message("Connected to ", basename(db_path), " database")
    
    # Register database availability in global registry
    availability_registry[["database"]] <<- list(
      app_data = TRUE,
      connection_valid = TRUE
    )
    
    return(app_data_conn)
    
  }, error = function(e) {
    warning("Failed to connect to ", basename(db_path), ": ", e$message)
    
    # Register database unavailability in global registry
    availability_registry[["database"]] <<- list(
      app_data = FALSE,
      connection_valid = FALSE
    )
    
    return(NULL)
  })
}