#' Finalize Position Table
#'
#' Performs final processing on the position table, including coalescing duplicate columns,
#' reordering columns, and performing data cleanup. Part of the D03_11 step in the 
#' Positioning Analysis derivation flow.
#'
#' @param app_data A DBI connection to the app data database.
#'
#' @return Logical. TRUE if processing succeeded, FALSE otherwise.
#'
#' @examples
#' \dontrun{
#' # Finalize position table
#' finalize_position_table(
#'   app_data = app_data,
#'   coalesce_suffix_cols = coalesce_suffix_cols
#' )
#' }
#'
#' @export
finalize_position_table <- function(app_data) {
  # Required packages
  if (!requireNamespace("dplyr", quietly = TRUE)) library(dplyr)
  if (!requireNamespace("DBI", quietly = TRUE)) library(DBI)
  
  # Source the coalesce function if not already available
  if (!exists("coalesce_suffix_cols")) {
    source(file.path("update_scripts", "global_scripts", "04_utils", "fn_coalesce_suffix_cols.R"))
  }
  
  message("\n== PHASE 3: Final data processing ==")
  
  # Check if the position table exists
  if (!DBI::dbExistsTable(app_data, "df_position")) {
    message("  ⚠️ Position table does not exist")
    return(FALSE)
  }
  
  # 1. Merge duplicate columns and reorder
  tryCatch({
    dplyr::tbl(app_data, "df_position") %>% 
      dplyr::collect() %>% 
      coalesce_suffix_cols() %>% 
      dplyr::relocate(product_line_id, product_id, brand, rating, sales, 
                    .before = dplyr::everything()) %>%
      dplyr::arrange(product_line_id) %>%
      DBI::dbWriteTable(app_data, "df_position", ., 
                       overwrite = TRUE, temporary = FALSE)
    
    message("  ✓ Successfully processed and finalized position table")
    return(TRUE)
  }, error = function(e) {
    message("  ⚠️ Error finalizing position table: ", e$message)
    return(FALSE)
  })
}

#' Verify Position Table
#'
#' Performs verification and reports statistics on the final position table.
#' Part of the D03_11 step in the Positioning Analysis derivation flow.
#'
#' @param app_data A DBI connection to the app data database.
#'
#' @return Invisible NULL. Outputs verification information to the console.
#'
#' @examples
#' \dontrun{
#' # Verify position table
#' verify_position_table(app_data = app_data)
#' }
#'
#' @export
verify_position_table <- function(app_data) {
  # Required packages
  if (!requireNamespace("DBI", quietly = TRUE)) library(DBI)
  
  message("\n== Verification ==")
  
  # Check if the position table exists
  if (!DBI::dbExistsTable(app_data, "df_position")) {
    message("  ⚠️ Position table does not exist")
    return(invisible(NULL))
  }
  
  # Table structure
  message("  ✓ Table structure:")
  table_info <- DBI::dbGetQuery(app_data, "PRAGMA table_info('df_position')")
  print(table_info[, c("name", "type")])
  
  # Data preview
  message("  ✓ Data preview:")
  preview_data <- DBI::dbGetQuery(app_data, "SELECT * FROM df_position LIMIT 5")
  print(preview_data)
  
  # Summary statistics
  message("\n== Summary Statistics ==")
  product_lines_count <- DBI::dbGetQuery(
    app_data,
    "SELECT COUNT(DISTINCT product_line_id) FROM df_position"
  )[1, 1]
  
  brands_count <- DBI::dbGetQuery(
    app_data,
    "SELECT COUNT(DISTINCT brand) FROM df_position WHERE brand NOT IN ('Rating', 'Revenue', 'Ideal')"
  )[1, 1]
  
  products_count <- DBI::dbGetQuery(
    app_data,
    "SELECT COUNT(DISTINCT product_id) FROM df_position WHERE product_id NOT IN ('Rating', 'Revenue', 'Ideal')"
  )[1, 1]
  
  message("- Product lines: ", product_lines_count)
  message("- Brands: ", brands_count)
  message("- products: ", products_count)
  
  return(invisible(NULL))
}

#' Clean Up Temporary Tables
#'
#' Removes temporary position tables after merging. Part of the D03_11 step
#' in the Positioning Analysis derivation flow.
#'
#' @param app_data A DBI connection to the app data database.
#'
#' @return Invisible integer. The number of tables cleaned up.
#'
#' @examples
#' \dontrun{
#' # Clean up temporary tables
#' cleanup_temp_position_tables(app_data = app_data)
#' }
#'
#' @export
cleanup_temp_position_tables <- function(app_data) {
  # Required packages
  if (!requireNamespace("DBI", quietly = TRUE)) library(DBI)
  
  # Find temporary position tables
  temp_tables <- DBI::dbListTables(app_data)[
    grepl("^df_position_", DBI::dbListTables(app_data))
  ]
  
  # Clean up if any found
  if (length(temp_tables) > 0) {
    for (table in temp_tables) {
      DBI::dbExecute(app_data, paste0("DROP TABLE IF EXISTS ", table))
    }
    message("- Cleaned up ", length(temp_tables), " temporary tables")
  } else {
    message("- No temporary tables to clean up")
  }
  
  return(invisible(length(temp_tables)))
}