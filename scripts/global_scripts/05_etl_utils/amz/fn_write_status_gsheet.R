#' Write anomalies / missing-master / drift outputs to the Status Google Sheet.
#'
#' Implements MP155 "View Surface" via Status Gsheet auto-refresh
#' (qef-gsheet-three-surface-redesign Decision 2). Called at the end of the
#' company product master ETL.
#'
#' Side effects: writes to Google Sheets via googlesheets4::sheet_write.
#' Failure (network, auth, API quota) emits a warning() and returns invisibly;
#' it MUST NOT abort the calling ETL pipeline.
#'
#' @param status_gsheet_config A list with elements:
#'        - sheet_id (string): Google Sheet ID
#'        - tabs (named list of tab names, e.g.
#'          list(anomalies = "_sys_anomalies",
#'               missing_master = "_sys_missing_master",
#'               drift = "_sys_drift",
#'               mapping_gaps = "_sys_mapping_gaps"))
#'        When NULL, the function silently skips (used by companies that have
#'        not adopted Status Gsheet yet).
#' @param anomalies data.frame from fn_detect_anomalies (anomalies output).
#'        Optional - NULL skips that tab.
#' @param missing_master data.frame from fn_detect_anomalies. Optional.
#'        Deprecated in favour of mapping_gaps; during the deprecation window
#'        (one release) both tabs MAY be written simultaneously.
#' @param drift data.frame from fn_detect_anomalies. Optional.
#' @param mapping_gaps data.frame from fn_detect_anomalies (added 2026-04-26 for
#'        Issue #471). Optional - NULL skips that tab. Default tab name is
#'        `_sys_mapping_gaps` per handbook status-gsheet-tab-naming.md.
#'
#' @return Invisibly returns a named logical vector indicating which tabs were
#'         written successfully. When status_gsheet_config is NULL, returns
#'         an empty named logical vector.
#'
#' @export

write_status_gsheet <- function(status_gsheet_config,
                                anomalies = NULL,
                                missing_master = NULL,
                                drift = NULL,
                                mapping_gaps = NULL) {
  if (is.null(status_gsheet_config)) return(invisible(logical(0)))

  if (!requireNamespace("googlesheets4", quietly = TRUE)) {
    warning(
      "write_status_gsheet: googlesheets4 package not installed; skipping ",
      "Status Gsheet writeback. Install with install.packages('googlesheets4').",
      call. = FALSE
    )
    return(invisible(logical(0)))
  }

  sheet_id <- status_gsheet_config$sheet_id
  tabs <- status_gsheet_config$tabs %||% list(
    anomalies = "_sys_anomalies",
    missing_master = "_sys_missing_master",
    drift = "_sys_drift",
    mapping_gaps = "_sys_mapping_gaps"
  )

  if (identical(sheet_id, "TBD")) {
    # Silent skip: "TBD" is the conventional placeholder set in app_config.yaml
    # between Phase 2 config commit and business creating the actual Status
    # Gsheet. No warning to avoid log spam during that window.
    return(invisible(logical(0)))
  }
  if (is.null(sheet_id) || !nzchar(sheet_id)) {
    warning(
      "write_status_gsheet: status_gsheet_config$sheet_id is empty; ",
      "skipping writeback.",
      call. = FALSE
    )
    return(invisible(logical(0)))
  }

  payloads <- list(
    anomalies = anomalies,
    missing_master = missing_master,
    drift = drift,
    mapping_gaps = mapping_gaps
  )

  results <- vapply(names(payloads), function(name) {
    df <- payloads[[name]]
    if (is.null(df)) return(NA)
    tab_name <- tabs[[name]]
    if (is.null(tab_name) || !nzchar(tab_name)) {
      warning(
        sprintf("write_status_gsheet: no tab name configured for '%s'; skipping.",
                name),
        call. = FALSE
      )
      return(FALSE)
    }
    write_one_tab(sheet_id, tab_name, df)
  }, logical(1))

  invisible(results)
}

# ==============================================================================
# Internal helpers
# ==============================================================================

write_one_tab <- function(sheet_id, tab_name, df) {
  result <- tryCatch({
    googlesheets4::sheet_write(
      data = df,
      ss = sheet_id,
      sheet = tab_name
    )
    TRUE
  },
  error = function(e) {
    warning(
      sprintf(
        "write_status_gsheet: failed to write tab '%s' to sheet_id=%s: %s",
        tab_name, sheet_id, conditionMessage(e)
      ),
      call. = FALSE
    )
    FALSE
  })
  result
}

`%||%` <- function(x, y) if (is.null(x)) y else x
