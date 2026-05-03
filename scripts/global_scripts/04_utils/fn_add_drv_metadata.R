#####
# FUNCTION: add_drv_metadata
# PURPOSE: Add standardized derivation metadata columns (DM_R048)
# AUTHOR: MAMBA Framework
# DATE: 2025-12-23
#####

#' Add DRV Metadata Columns
#'
#' Adds standardized derivation metadata columns to a data frame before writing
#' to app_data. Implements DM_R048 (Derivation Timestamp Standard).
#'
#' @param df Data frame to modify
#' @param script_name Name of the DRV script (e.g., "cbz_D01_06")
#' @param batch_id Optional batch ID. If NULL, generates from current time (YYYYMMDD_HHMMSS)
#'
#' @return Data frame with three additional columns:
#'   - derivation_timestamp: POSIXct timestamp when row was derived
#'   - derivation_batch_id: Character batch identifier for execution correlation
#'   - derivation_script: Character name of source script
#'
#' @details
#' This function follows the MAMBA timestamp naming convention:
#' - ETL 0IM: import_timestamp
#' - ETL 1ST: staging_timestamp
#' - ETL 2TR: transform_timestamp
#' - DRV: derivation_timestamp (this function)
#'
#' @examples
#' \dontrun{
#' # In DRV script PART 1: INITIALIZE
#' drv_batch_id <- format(Sys.time(), "%Y%m%d_%H%M%S")
#' drv_script_name <- "cbz_D01_06"
#'
#' # In PART 2: MAIN, before dbWriteTable
#' customer_dna <- add_drv_metadata(customer_dna, drv_script_name, drv_batch_id)
#' dbWriteTable(app_data, "df_dna_by_customer", customer_dna, overwrite = TRUE)
#' }
#'
#' @seealso
#' - DM_R048: Derivation Timestamp Standard Rule
#' - DM_R044: Derivation Implementation Standard
#'
#' @export
add_drv_metadata <- function(df, script_name, batch_id = NULL) {
  if (!is.data.frame(df)) {
    stop("df must be a data frame")
  }
  if (missing(script_name) || is.null(script_name) || script_name == "") {
    stop("script_name is required")
  }

  if (is.null(batch_id)) {
    batch_id <- format(Sys.time(), "%Y%m%d_%H%M%S")
  }

  df %>%
    dplyr::mutate(
      derivation_timestamp = Sys.time(),
      derivation_batch_id = batch_id,
      derivation_script = script_name
    )
}
