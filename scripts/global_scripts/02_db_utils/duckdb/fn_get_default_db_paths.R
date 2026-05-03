#' Get Default Database Paths
#'
#' A function for determining default database paths based on the current directory structure.
#' Each database serves a specific purpose in the five-layer data pipeline:
#' - raw_data: Original unprocessed data (Layer 1)
#' - staged_data: File-level preprocessed data with unified encoding (Layer 2)
#' - transformed_data: Schema-standardized data with unified types and structure (Layer 3)
#' - cleansed_data: Quality-assured data after deduplication and validation (Layer 4)
#' - processed_data: Business logic processed data with KPIs and aggregations (Layer 5)
#' - app_data: Application-ready data for UI consumption (Layer 6)
#' - sales_by_customer_date_data: Sales data aggregated by customer and date
#' - slowly_changing_data: Data that changes slowly over time (dimensions)
#' - comment_property_rating: Product ratings and comments
#' - global_scd_type1: Global slowly changing dimensions (type 1)
#'
#' @param base_dir Optional. Base directory to use instead of auto-detection
#' @return A list containing paths to all databases used in the project
#' @examples
#' db_paths <- get_default_db_paths()
#' db_paths <- get_default_db_paths("/custom/base/dir")
#'
get_default_db_paths <- function() {
  return(list(
    # Five-layer data pipeline databases (DF00 Architecture)
    raw_data = file.path(COMPANY_DIR, "raw_data.duckdb"),
    staged_data = file.path(COMPANY_DIR, "staged_data.duckdb"),
    transformed_data = file.path(COMPANY_DIR, "transformed_data.duckdb"),
    cleansed_data = file.path(COMPANY_DIR, "cleansed_data.duckdb"),
    processed_data = file.path(COMPANY_DIR, "processed_data.duckdb"),
    sales_by_customer_date_data = file.path(APP_DATA_DIR, "sales_by_customer_date_data.duckdb"),
    app_data = file.path(APP_DATA_DIR,"app_data.duckdb"),
    slowly_changing_data = file.path(APP_DATA_DIR, "slowly_changing_data.duckdb"),
    comment_property_rating = file.path(APP_DATA_DIR, "scd_type2", "comment_property_rating.duckdb"),
    comment_property_rating_results = file.path(APP_DATA_DIR, "scd_type2", "comment_property_rating_results.duckdb"),
    
    # Global data in the global_scripts directory
    global_scd_type1 = file.path(GLOBAL_DIR, "30_global_data", "global_scd_type1.duckdb")
  ))
}
