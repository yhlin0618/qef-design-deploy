#' Create DNA by Customer Table
#' 
#' Creates the DNA by customer table as defined in D00_create_app_data_frames.md
#' This function follows the Database Table Creation Strategy (MP058) and
#' Universal DBI Approach (R092)
#'
#' @param con Database connection object
#' @param or_replace Logical indicating whether to replace the table if it exists
#' @param verbose Logical indicating whether to display detailed messages
#' @return Logical indicating whether the operation was successful
#' @requires DBI
#' @requires duckdb
#' @requires fn_generate_create_table_query.R
#' @principle MP058 Database Table Creation Strategy
#' @principle R092 Universal DBI Approach
#' @principle MP47 Functional Programming
#' @author Claude
#' @date 2025-05-18
#' @export
create_df_dna_by_customer <- function(con, or_replace = TRUE, verbose = FALSE) {
  # Initialize error tracking and result
  error_occurred <- FALSE
  result <- FALSE
  
  # Validate input parameters
  if (!inherits(con, "DBIConnection")) {
    stop("Invalid connection object. Must be a DBI connection.")
  }
  
  tryCatch({
    # Generate CREATE TABLE query for DNA by customer table
    create_dna_by_customer_query <- generate_create_table_query(
      con = con,
      or_replace = or_replace,
      target_table = "df_dna_by_customer",
      source_table = NULL,
      column_defs = list(
        list(name = "customer_id", type = "INTEGER", not_null = TRUE),
        list(name = "ipt", type = "DOUBLE"),
        list(name = "total_spent", type = "DOUBLE"),
        list(name = "times", type = "INTEGER"),
        list(name = "zipcode", type = "BOOLEAN"),
        list(name = "state", type = "BOOLEAN"),
        list(name = "lat", type = "BOOLEAN"),
        list(name = "lng", type = "BOOLEAN"),
        list(name = "ipt_mean", type = "DOUBLE"),
        list(name = "m_value", type = "DOUBLE"),
        list(name = "sigma_hnorm_mle", type = "DOUBLE"),
        list(name = "sigma_hnorm_bcmle", type = "DOUBLE"),
        list(name = "m_ecdf", type = "DOUBLE"),
        list(name = "m_label", type = "ENUM('Low Value', 'Medium Value', 'High Value')"),
        list(name = "ni", type = "INTEGER"),
        list(name = "date", type = "TIMESTAMP"),
        list(name = "sum_spent_by_date", type = "DOUBLE"),
        list(name = "count_transactions_by_date", type = "INTEGER"),
        list(name = "min_time_by_date", type = "TIMESTAMP"),
        list(name = "ni_2", type = "INTEGER"),
        list(name = "min_time", type = "TIMESTAMP"),
        list(name = "payment_time", type = "TIMESTAMP"),
        list(name = "difftime", type = "INTERVAL"),
        list(name = "nes_ratio", type = "DOUBLE"),
        list(name = "nes_status", type = "ENUM('N', 'E0', 'S1', 'S2', 'S3')", default = "'N'"),
        list(name = "f_value", type = "INTEGER"),
        list(name = "f_ecdf", type = "DOUBLE"),
        list(name = "f_label", type = "ENUM('Low Frequency', 'Medium Frequency', 'High Frequency')"),
        list(name = "r_value", type = "DOUBLE"),
        list(name = "r_ecdf", type = "DOUBLE"),
        list(name = "r_label", type = "ENUM('Long Inactive', 'Medium Inactive', 'Recent Buyer')"),
        list(name = "mle", type = "DOUBLE"),
        list(name = "wmle", type = "DOUBLE"),
        list(name = "cai", type = "DOUBLE"),
        list(name = "cai_ecdf", type = "DOUBLE"),
        list(name = "cai_label", type = "ENUM('Gradually Inactive', 'Stable', 'Increasingly Active')"),
        list(name = "pcv", type = "DOUBLE"),
        list(name = "total_sum", type = "DOUBLE"),
        list(name = "clv", type = "DOUBLE"),
        list(name = "time_first", type = "TIMESTAMP"),
        list(name = "nt", type = "DOUBLE"),
        list(name = "time_first_to_now", type = "DOUBLE"),
        list(name = "e0t", type = "DOUBLE"),
        list(name = "ge", type = "DOUBLE"),
        list(name = "ie", type = "DOUBLE"),
        list(name = "be", type = "DOUBLE"),
        list(name = "cri", type = "DOUBLE"),
        list(name = "cri_ecdf", type = "DOUBLE"),
        list(name = "be2", type = "DOUBLE"),
        list(name = "nrec", type = "ENUM('nrec', 'rec')"),
        list(name = "nrec_prob", type = "DOUBLE"),
        list(name = "p_alive", type = "DOUBLE"),
        list(name = "btyd_expected_transactions", type = "DOUBLE"),
        list(name = "platform_id", type = "VARCHAR", not_null = TRUE),
        list(name = "product_line_id_filter", type = "VARCHAR", not_null = TRUE)        
      ),
      # Add composite primary key on customer_id and platform_id
      primary_key = c("customer_id", "platform_id","product_line_id_filter"),
      # Add index on platform_id for foreign key lookups
      indexes = list(
        list(columns = "platform_id")
      )
    )
    
    # Log the generated query if verbose
    if (verbose) {
      message("Generated SQL Query:")
      cat(create_dna_by_customer_query, "\n")
    }
    
    # Execute the query
    if (verbose) {
      message("Creating DNA by customer table...")
    }
    dbExecute(con, create_dna_by_customer_query)
    
    # Verify table was created
    tables <- dbListTables(con)
    if ("df_dna_by_customer" %in% tables) {
      if (verbose) {
        message("DNA by customer table created successfully")
      }
      result <- TRUE
    } else {
      if (verbose) {
        message("Failed to create DNA by customer table")
      }
      result <- FALSE
    }
    
  }, error = function(e) {
    if (verbose) {
      message("Error creating DNA by customer table: ", e$message)
    }
    error_occurred <- TRUE
    result <- FALSE
  })
  
  return(result)
}
