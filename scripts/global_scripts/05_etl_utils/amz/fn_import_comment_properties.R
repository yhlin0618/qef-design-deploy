#' Import Comment Properties from Google Sheets
#'
#' This function imports comment property data from Google Sheets for multiple product lines
#' and stores it in the raw_data database. It supports the D03_02 step of the
#' Positioning Analysis derivation flow.
#'
#' @param db_connection A DBI connection to the database where data will be written
#' @param google_sheet_id The ID of the Google Sheet containing comment properties or
#'        a Google Sheets URL that can be converted to an ID
#' @param product_line_df A dataframe containing product line information (default: df_product_line)
#' @param create_table Logical. Whether to create or replace the table (default: TRUE)
#' @param skip_validation Logical. If TRUE, skips the comprehensive validation checks
#'        to speed up the import process. Useful for ETL pipelines where validation is handled in later stages.
#'        Default is FALSE.
#'
#' @return A data frame of comment properties with product_line_id and property details
#'
#' @examples
#' \dontrun{
#' # Connect to database
#' dbConnect_from_list("raw_data")
#' 
#' # Import comment properties
#' import_comment_properties(
#'   db_connection = raw_data,
#'   google_sheet_id = app_configs$googlesheet$comment_property
#' )
#' }
#'
#' @export
import_comment_properties <- function(db_connection = raw_data,
                                     google_sheet_id = NULL,
                                     product_line_df = df_product_line,
                                     create_table = TRUE,
                                     skip_validation = FALSE) {
  
  # Validate parameters
  if (is.null(google_sheet_id)) {
    stop("google_sheet_id is required")
  }
  
  # Make sure required packages are loaded
  if (!requireNamespace("googlesheets4", quietly = TRUE)) {
    message("Loading package 'googlesheets4'")
    library(googlesheets4)
  }
  if (!requireNamespace("janitor", quietly = TRUE)) {
    message("Loading package 'janitor'")
    library(janitor)
  }
  
  # Create Google Sheets connection
  googlesheet_con <- googlesheets4::as_sheets_id(google_sheet_id)
  
  # Filter active product lines (exclude 'all') — config-driven (#363)
  active_product_lines <- get_active_product_lines(product_line_df = product_line_df)
  
  # Initialize result data frame
  result_data <- data.frame()
  
  # For each product line, read data from separate sheet
  for (i in 1:nrow(active_product_lines)) {
    product_line_id <- active_product_lines$product_line_id[i]
    product_line_name <- active_product_lines$product_line_name_chinese[i]
    
    # Use product line Chinese name as sheet name
    sheet_name <- product_line_name
    
    tryCatch({
      message(paste("Reading comment properties from sheet:", sheet_name))
      
      # Read data from sheet and add product_line_id
      sheet_data <- googlesheets4::read_sheet(googlesheet_con, sheet = sheet_name) %>%
        dplyr::mutate(product_line_id = product_line_id) %>% 
        tidyr::drop_na(property_id,property_name)
      
      # Append to result
      result_data <- dplyr::bind_rows(result_data, sheet_data)
      
    }, error = function(e) {
      warning(paste("Error reading sheet", sheet_name, ":", e$message))
    })
  }
  
  # Clean column names
  if (nrow(result_data) > 0) {
    result_data <- janitor::clean_names(result_data, ascii = FALSE)
  } else {
    warning("No comment property data was found")
    return(data.frame())
  }
  
  message(paste0("colnames: ",paste(names(result_data),collapse=", ")))
  
  # Create table if requested
  if (create_table) {
    # Create table query
    query_create_table <- generate_create_table_query(
      con = db_connection,
      or_replace = TRUE,
      target_table = "df_all_comment_property",
      source_table = NULL,
      column_defs = list(
        list(name = "product_line_id", type = "VARCHAR"),
        list(name = "property_id", type = "INTEGER"),
        list(name = "property_name", type = "VARCHAR", not_null = TRUE),
        list(name = "property_name_english", type = "VARCHAR", not_null = TRUE),
        list(name = "frequency", type = "INTEGER"),
        list(name = "proportion", type = "DOUBLE"),
        list(name = "definition", type = "VARCHAR"),
        list(name = "review_1", type = "VARCHAR"),
        list(name = "translation_1", type = "VARCHAR"),
        list(name = "review_2", type = "VARCHAR"),
        list(name = "translation_2", type = "VARCHAR"),
        list(name = "review_3", type = "VARCHAR"),
        list(name = "translation_3", type = "VARCHAR"),
        list(name = "type", type = "VARCHAR"),
        list(name = "note", type = "VARCHAR")
      ),
      primary_key = c("product_line_id", "property_id")
    )
    
    # Display and execute query
    message("Creating table with query:")
    message(query_create_table)
    DBI::dbExecute(db_connection, query_create_table)
  }
  
  # Write to database
  if (nrow(result_data) > 0) {
    DBI::dbWriteTable(db_connection, "df_all_comment_property", result_data, append = TRUE)
    message(paste("Successfully imported", nrow(result_data), "comment properties"))
  }
  
  return(result_data)
}