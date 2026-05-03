# ==============================================================================
# DUCKDB DATA TYPES REFERENCE
# ==============================================================================
# Built-in DuckDB Data Types (for reference):
#
# NUMERIC TYPES:
# - TINYINT (INT1): Signed one-byte integer
# - SMALLINT (INT2, SHORT): Signed two-byte integer  
# - INTEGER (INT4, INT, SIGNED): Signed four-byte integer
# - BIGINT (INT8, LONG): Signed eight-byte integer
# - HUGEINT: Signed sixteen-byte integer
# - UTINYINT: Unsigned one-byte integer
# - USMALLINT: Unsigned two-byte integer
# - UINTEGER: Unsigned four-byte integer
# - UBIGINT: Unsigned eight-byte integer
# - UHUGEINT: Unsigned sixteen-byte integer
# - FLOAT (FLOAT4, REAL): Single precision floating-point (4 bytes)
# - DOUBLE (FLOAT8): Double precision floating-point (8 bytes)
# - DECIMAL(prec, scale) (NUMERIC): Fixed-precision, defaults to DECIMAL(18,3)
#
# TEXT TYPES:
# - VARCHAR (CHAR, BPCHAR, TEXT, STRING): Variable-length character string
# - BIT (BITSTRING): String of 1s and 0s
# - UUID: UUID data type
#
# DATE/TIME TYPES:
# - DATE: Calendar date (year, month, day)
# - TIME: Time of day (no time zone)
# - TIMESTAMP (DATETIME): Combination of date and time
# - TIMESTAMP WITH TIME ZONE (TIMESTAMPTZ): Timestamp using current time zone
# - INTERVAL: Date/time delta
#
# OTHER TYPES:
# - BOOLEAN (BOOL, LOGICAL): Logical true/false
# - BLOB (BYTEA, BINARY, VARBINARY): Variable-length binary data
# - JSON: JSON object (requires json extension)
# ==============================================================================

#' Stage product Profiles from Raw Data to Staged Data (Universal Staging Function)
#'
#' This is a platform-agnostic function that performs comprehensive data staging
#' operations for product profile data. It standardizes formats, cleans data,
#' validates data types, and creates optimized DuckDB table structures.
#'
#' This function is designed for ETL Phase 1 (Staging) and follows the universal
#' staging pattern defined in the ETL framework.
#'
#' @param raw_db_connection A DBI connection to the raw data database
#' @param staged_db_connection A DBI connection to the staged data database
#' @param table_pattern Optional regex pattern to filter raw tables 
#'                     (default: "^df_product_profile_")
#' @param overwrite_existing Logical, whether to overwrite existing staged tables
#'                          (default: TRUE)
#'
#' @return A list containing staging results and metadata
#' @note This function creates tables with names: df_product_profile_*___staged
#'       Input tables should follow pattern: df_product_profile_*
#'
#' @examples
#' \dontrun{
#' # Connect to databases
#' raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = TRUE)
#' staged_data <- dbConnectDuckdb(db_path_list$staged_data, read_only = FALSE)
#' 
#' # Stage all product profiles
#' staging_results <- stage_product_profiles(
#'   raw_db_connection = raw_data,
#'   staged_db_connection = staged_data
#' )
#' }
#'
#' @export
stage_product_profiles <- function(raw_db_connection,
                               staged_db_connection,
                               table_pattern = "^df_product_profile_",
                               overwrite_existing = TRUE) {
  
  # Validate parameters
  if (missing(raw_db_connection) || missing(staged_db_connection)) {
    stop("Both raw_db_connection and staged_db_connection are required")
  }
  
  # Get all raw product profile tables
  raw_tables <- DBI::dbListTables(raw_db_connection)
  product_profile_tables <- raw_tables[grepl(table_pattern, raw_tables)]
  
  if (length(product_profile_tables) == 0) {
    stop("No raw product profile tables found matching pattern: ", table_pattern)
  }
  
  message("STAGING: Found ", length(product_profile_tables), 
          " raw product profile tables to stage")
  
  # Initialize results
  staging_results <- list(
    tables_processed = character(),
    total_rows_staged = 0,
    processing_details = list()
  )
  
  # Process each raw product profile table (simplified pipeline)
  for (table_name in product_profile_tables) {
    message("STAGING: Processing ", table_name)

    tryCatch({
      # ------------------------------------------------------------------
      # 1) Load raw data
      # ------------------------------------------------------------------
      raw_df <- DBI::dbGetQuery(raw_db_connection, paste("SELECT * FROM", table_name))
      if (nrow(raw_df) == 0) {message("  • Empty table, skipped"); next}

      # ------------------------------------------------------------------
      # 2) Clean column names
      # ------------------------------------------------------------------
      names(raw_df) <- make.unique(make_names(names(raw_df), case = "snake_case"), sep="_")

      # ------------------------------------------------------------------
      # 3) Detect data types & build schema
      # ------------------------------------------------------------------
      column_definitions <- lapply(names(raw_df), function(col){
        info <- detect_data_type(col, raw_df[[col]], target_db="duckdb")
        list(name = col, type = info$type, not_null = FALSE)
      })
      field_types <- setNames(vapply(column_definitions, "[[", character(1), "type"),
                              vapply(column_definitions, "[[", character(1), "name"))

      # ------------------------------------------------------------------
      # 4) Create staged table
      # ------------------------------------------------------------------
      staged_tbl <- paste0(table_name, "___staged")
      if (DBI::dbExistsTable(staged_db_connection, staged_tbl)) DBI::dbRemoveTable(staged_db_connection, staged_tbl)
      # 建立 CREATE TABLE SQL，明確使用具名參數避免位置錯誤
      create_sql <- generate_create_table_query(
        con = staged_db_connection,
        target_table = staged_tbl,
        column_defs = column_definitions,
        or_replace = TRUE,
        if_not_exists = FALSE
      )

      DBI::dbExecute(staged_db_connection, create_sql)

      # ------------------------------------------------------------------
      # 5) Prepare staged_df with basic type casting & metadata
      # ------------------------------------------------------------------
      staged_df <- raw_df
      for(col in names(staged_df)) {
        target_type <- field_types[[col]]
        if(grepl("INT|DOUBLE|DECIMAL|NUMERIC", toupper(target_type))) {
          # Replace comma with dot then numeric
          staged_df[[col]] <- as.numeric(gsub(",", ".", staged_df[[col]]))
        }
      }
      staged_df$etl_phase <- "stage"

      # ------------------------------------------------------------------
      # 6) Write data
      # ------------------------------------------------------------------
      DBI::dbWriteTable(
        conn   = staged_db_connection,
        name   = staged_tbl,
        value  = staged_df,
        append = TRUE
      )

      # ------------------------------------------------------------------
      # 7) Add metadata columns
      # ------------------------------------------------------------------
      DBI::dbExecute(staged_db_connection, sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS etl_staging_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP", staged_tbl))
      DBI::dbExecute(staged_db_connection, sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS etl_validation_status VARCHAR DEFAULT 'passed'", staged_tbl))
      DBI::dbExecute(staged_db_connection, sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS etl_phase VARCHAR DEFAULT 'staging'", staged_tbl))

      inserted <- DBI::dbGetQuery(staged_db_connection, sprintf("SELECT COUNT(*) cnt FROM %s", staged_tbl))$cnt
      message("  • Rows inserted: ", inserted)
      if(inserted==0) stop("No data inserted to ", staged_tbl)

      staging_results$tables_processed <- c(staging_results$tables_processed, staged_tbl)
      staging_results$total_rows_staged <- staging_results$total_rows_staged + inserted

    }, error=function(e){
      warning("Error staging ", table_name, ": ", e$message)
    })
  }
  
  # Final summary
  message("STAGING: Completed processing ", length(product_profile_tables), 
          " tables")
  message("STAGING: Successfully staged ", 
          length(staging_results$tables_processed), " tables")
  message("STAGING: Total rows staged: ", staging_results$total_rows_staged)
  
  return(staging_results)
}