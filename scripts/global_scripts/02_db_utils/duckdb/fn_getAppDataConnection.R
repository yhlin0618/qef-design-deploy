#' getAppDataConnection
#'
#' @description
#' Dual-mode connection for app_data: DuckDB locally, Supabase on deployment.
#' This function is a convenience wrapper around dbConnectAppData() for backward compatibility.
#'
#' Connection mode is determined by app_config.yaml > database.mode:
#'   - "duckdb": Force use local DuckDB file
#'   - "supabase": Force use Supabase PostgreSQL
#'   - "auto": DuckDB if valid file exists, else Supabase (default)
#'
#' @param duckdb_path Character. Path to DuckDB file. Default: from config or "data/app_data/app_data.duckdb"
#' @param verbose Logical. Print connection mode message. Default: TRUE
#' @param config_path Character. Path to app_config.yaml. Default: "app_config.yaml"
#'
#' @return DBI connection object with "connection_type" attribute
#'
#' @details
#' Following Principles:
#'   - MP142: Configuration-Driven Pipeline
#'   - DM_R023: Universal DBI Approach
#'   - DM_R056: Posit Connect Deployment Assets
#'   - DEV_R039: Side-Effect-Free Function Files
#'   - SO_R007: One Function One File
#'
#' @seealso
#' \code{\link{dbConnectAppData}} - Primary connection function with full options
#' \code{\link{dbDiagnose}} - Diagnose connection configuration
#'
#' @export
#' @importFrom DBI dbConnect

getAppDataConnection <- function(
    duckdb_path = NULL,
    verbose = TRUE,
    config_path = "app_config.yaml"
) {

  # Source the primary connection function if not available
  if (!exists("dbConnectAppData", mode = "function")) {
    dbConnectAppData_path <- "scripts/global_scripts/02_db_utils/fn_dbConnectAppData.R"
    if (file.exists(dbConnectAppData_path)) {
      source(dbConnectAppData_path)
    } else {
      # Fallback: try relative path
      dbConnectAppData_path <- "../fn_dbConnectAppData.R"
      if (file.exists(dbConnectAppData_path)) {
        source(dbConnectAppData_path)
      } else {
        stop("dbConnectAppData function not found. Please source fn_dbConnectAppData.R first.")
      }
    }
  }

  # Delegate to the primary function
  con <- dbConnectAppData(
    db_path = duckdb_path,
    config_path = config_path,
    verbose = verbose
  )

  # Add additional metadata for this entry point
  attr(con, "entry_point") <- "getAppDataConnection"

  return(con)
}
