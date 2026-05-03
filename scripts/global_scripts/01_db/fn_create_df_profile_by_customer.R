#' Create Customer Profile Table
#'
#' Creates the customer profile table in the app database.
#' This function implements the database table creation strategy (MP058)
#' and follows the Universal DBI Approach (R092).
#'
#' @param con A DBI database connection to the app database.
#' @param or_replace Logical. Whether to replace the table if it already exists (default: TRUE).
#' @param verbose Logical. Whether to display detailed messages during execution (default: TRUE).
#'
#' @return Logical. TRUE if the operation was successful, FALSE otherwise.
#'
#' @examples
#' \dontrun{
#' # Connect to app database
#' app_data <- dbConnect_from_list("app_data")
#' 
#' # Create customer profile table
#' create_df_profile_by_customer(app_data)
#' }
#'
#' @details
#' The customer profile table has the following structure:
#' - customer_id (INTEGER): Unique customer identifier
#' - buyer_name (VARCHAR): Customer's name
#' - email (VARCHAR): Customer's email address
#' - platform_id (INTEGER, NOT NULL): Platform identifier
#' - display_name (VARCHAR): Generated name and email combination
#'
#' The table has a composite primary key on (customer_id, platform_id) and
#' an index on platform_id for faster lookups.
#'
#' @principle MP058 Database Table Creation Strategy
#' @principle R092 Universal DBI Approach
#' @export
create_df_profile_by_customer <- function(con, or_replace = TRUE, verbose = TRUE) {
  # Initialize error tracking
  error_occurred <- FALSE
  
  # Generate CREATE TABLE query for customer profile
  tryCatch({
    if (verbose) message("Generating SQL query for customer profile table...")
    
    create_customer_profile_query <- generate_create_table_query(
      con = con,
      or_replace = or_replace,
      target_table = "df_profile_by_customer",
      source_table = NULL,
      column_defs = list(
        list(name = "customer_id", type = "INTEGER"),
        list(name = "buyer_name", type = "VARCHAR"),
        list(name = "email", type = "VARCHAR"),
        list(name = "platform_id", type = "VARCHAR", not_null = TRUE),
        list(name = "display_name", type = "VARCHAR",
             generated_as = "buyer_name || ' (' || email || ')'")
      ),
      primary_key = c("customer_id", "platform_id"),
      indexes = list(
        list(columns = "platform_id")
      )
    )
    
    # Log the generated query if verbose
    if (verbose) {
      message("Generated SQL Query:")
      cat(create_customer_profile_query, "\n")
    }
    
    # Execute the query
    if (verbose) message("Creating customer profile table...")
    DBI::dbExecute(con, create_customer_profile_query)
    
    if (verbose) message("Customer profile table created successfully")
  }, error = function(e) {
    if (verbose) message("Error creating customer profile table: ", e$message)
    error_occurred <- TRUE
  })
  
  # Verify table creation if no error occurred
  test_passed <- FALSE
  if (!error_occurred) {
    tryCatch({
      # Verify table exists
      table_check_query <- "SELECT name FROM sqlite_master WHERE type='table' AND name='df_profile_by_customer'"
      table_exists <- nrow(DBI::dbGetQuery(con, table_check_query)) > 0
      
      if (table_exists) {
        # Verify table structure
        table_info <- DBI::dbGetQuery(con, "PRAGMA table_info(df_profile_by_customer)")
        
        # Check column count
        if (nrow(table_info) == 5) {
          if (verbose) message("Verification successful: Table has correct column count")
          
          # Check primary key
          has_pk <- any(table_info$pk > 0)
          if (has_pk) {
            if (verbose) message("Verification successful: Primary key exists")
            test_passed <- TRUE
          } else {
            if (verbose) message("Verification failed: No primary key defined")
            test_passed <- FALSE
          }
        } else {
          if (verbose) message("Verification failed: Expected 5 columns, found ", nrow(table_info))
          test_passed <- FALSE
        }
      } else {
        if (verbose) message("Verification failed: Table df_profile_by_customer does not exist")
        test_passed <- FALSE
      }
    }, error = function(e) {
      if (verbose) message("Error verifying table: ", e$message)
      test_passed <- FALSE
    })
  }
  
  # Return test result
  return(test_passed)
}
