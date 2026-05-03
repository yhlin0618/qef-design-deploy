#' Merge Position Tables for All Product Lines
#'
#' Merges individual product line position tables into a single combined table.
#' Uses SQL FULL OUTER JOIN to merge tables, preserving all columns across product lines.
#' Part of the D03_11 step in the Positioning Analysis derivation flow.
#'
#' @param product_line_ids Character vector. The product line IDs to merge.
#' @param app_data A DBI connection to the app data database.
#' @param paste_ Function. The string concatenation function.
#'
#' @return Logical. TRUE if merging succeeded, FALSE otherwise.
#'
#' @examples
#' \dontrun{
#' # Merge position tables
#' merge_position_tables(
#'   product_line_ids = vec_product_line_id_noall,
#'   app_data = app_data,
#'   paste_ = paste_
#' )
#' }
#'
#' @export
merge_position_tables <- function(product_line_ids, 
                                app_data,
                                paste_) {
  # Required packages
  if (!requireNamespace("DBI", quietly = TRUE)) library(DBI)
  
  message("\n== PHASE 2: Merging data from all product lines ==")
  
  # Check if we have product lines to merge
  if (length(product_line_ids) == 0) {
    message("  ⚠️ No product lines to merge")
    return(FALSE)
  }
  
  # Use standardized product_id column (renamed in process_position_table)
  product_col <- "product_id"
  
  # 1. Generate table name list
  table_names <- paste_("df_position", product_line_ids)
  
  # Check if the first table exists
  if (!DBI::dbExistsTable(app_data, table_names[1])) {
    message("  ⚠️ First table not found: ", table_names[1])
    return(FALSE)
  }
  
  # If only one table, just rename it
  if (length(table_names) == 1) {
    sql <- sprintf("CREATE OR REPLACE TABLE df_position AS SELECT * FROM %s;", table_names[1])
    DBI::dbExecute(app_data, sql)
    message("  ✓ Only one product line, copied to final position table")
    return(TRUE)
  }
  
  # 2. Get all unique columns across tables
  all_columns <- character()
  for (table_name in table_names) {
    if (DBI::dbExistsTable(app_data, table_name)) {
      cols <- DBI::dbListFields(app_data, table_name)
      all_columns <- unique(c(all_columns, cols))
    }
  }
  
  # 3. Build UNION ALL SQL statement with column alignment
  union_parts <- character()
  
  for (i in 1:length(table_names)) {
    if (!DBI::dbExistsTable(app_data, table_names[i])) {
      message("  ⚠️ Skipping non-existent table: ", table_names[i])
      next
    }
    
    # Get columns for this table
    table_cols <- DBI::dbListFields(app_data, table_names[i])
    
    # Build SELECT statement with NULL for missing columns
    select_parts <- character()
    for (col in all_columns) {
      if (col %in% table_cols) {
        select_parts <- c(select_parts, col)
      } else {
        select_parts <- c(select_parts, sprintf("NULL AS %s", col))
      }
    }
    
    # Create SELECT statement for this table
    table_sql <- sprintf("SELECT %s FROM %s", 
                        paste(select_parts, collapse = ", "), 
                        table_names[i])
    
    union_parts <- c(union_parts, table_sql)
  }
  
  # 4. Combine with UNION ALL
  if (length(union_parts) == 0) {
    message("  ⚠️ No valid tables to merge")
    return(FALSE)
  }
  
  union_sql <- paste(union_parts, collapse = " UNION ALL ")
  final_sql <- sprintf("CREATE OR REPLACE TABLE df_position AS\n%s;", union_sql)
  
  tryCatch({
    DBI::dbExecute(app_data, final_sql)
    message("  ✓ Successfully merged data from multiple product lines")
    return(TRUE)
  }, error = function(e) {
    message("  ⚠️ Error merging tables: ", e$message)
    return(FALSE)
  })
}