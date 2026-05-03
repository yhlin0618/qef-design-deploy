#' @file fn_apply_column_templates.R
#' @use Combine column templates with custom columns
#' @requires list_column_templates.R
#' @principle SO_R007 (One Function One File)
#' @principle MP032 (DRY)
#' @principle RC01 (Function File Template)
#' @author Claude
#' @date 2025-12-24

#' Apply Column Templates to Generate Column Definitions
#'
#' Convenience function to combine template columns with custom columns,
#' reducing repetitive code when building column_defs lists for
#' generate_create_table_query().
#'
#' @param templates List of template columns to include (e.g., list_column_template_platform_id)
#' @param custom_columns Additional custom column definitions as a list
#'
#' @return List of complete column definitions ready for generate_create_table_query()
#'
#' @examples
#' # Source the templates and function
#' source("scripts/global_scripts/01_db/generate_create_table_query/list_column_templates.R")
#' source("scripts/global_scripts/01_db/generate_create_table_query/fn_apply_column_templates.R")
#'
#' # Create ETL table with platform_id and timestamps
#' columns <- apply_column_templates(
#'   templates = list(list_column_template_platform_id),
#'   custom_columns = list(
#'     list(name = "order_id", type = "VARCHAR(50)", not_null = TRUE),
#'     list(name = "amount", type = "DECIMAL(10,2)", not_null = TRUE),
#'     list_column_template_etl_timestamps$created_at
#'   )
#' )
#'
#' # Use with generate_create_table_query
#' con <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' query <- generate_create_table_query(
#'   con = con,
#'   target_table = "sales_raw",
#'   column_defs = columns,
#'   primary_key = c("platform_id", "order_id")
#' )
#' cat(query)
#' DBI::dbDisconnect(con)
#'
#' @export
apply_column_templates <- function(templates = list(), custom_columns = list()) {
  # Combine templates and custom columns
  combined <- c(templates, custom_columns)

  # Validate that all elements are lists with 'name' field
  if (!all(sapply(combined, function(x) is.list(x) && !is.null(x$name)))) {
    stop("All column definitions must be lists with a 'name' field")
  }

  return(combined)
}
