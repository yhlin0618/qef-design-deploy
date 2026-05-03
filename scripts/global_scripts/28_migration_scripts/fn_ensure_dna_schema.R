#' @file fn_ensure_dna_schema.R
#' @title Ensure df_dna_by_customer Schema Matches Canonical Source-of-Truth
#' @description Idempotent schema reconciliation helper. Reads canonical column
#'   definitions from `fn_create_df_dna_by_customer_table.R` by running it
#'   against an in-memory DuckDB and introspecting the result, compares with the
#'   target connection's actual schema, and adds missing columns via
#'   `ALTER TABLE ... ADD COLUMN`. Used by D01_04 to prevent silent column drop
#'   when projecting derivation outputs to app_data after schema additions
#'   (e.g., BTYD `p_alive` from #211).
#' @note Unlike most files in this directory, this is a reusable runtime helper
#'   (called from D01_04 every pipeline run), not a one-time migration script.
#'   It contains only function definitions (no top-level executable code) so
#'   sourcing during pipeline runs is safe.
#' @principle MP154 Side Effect Defense
#' @principle DM_R023 Universal DBI Approach
#' @principle MP058 Database Table Creation Strategy
#' @related Issue #376, fn_D01_04_core.R, fn_create_df_dna_by_customer_table.R
#' @author spectra change fix-mamba-dashboard-empty-data
#' @date 2026-04-14
#' @use DBI, duckdb

#' Ensure df_dna_by_customer schema matches canonical source-of-truth
#'
#' Reads the canonical schema by running `create_df_dna_by_customer_table()`
#' against an in-memory DuckDB connection, then ALTER TABLEs the target
#' connection to add any columns present in the canonical schema but missing
#' from the target table. Idempotent: safe to call repeatedly.
#'
#' @param target_con A DBI connection (DuckDB) to the database whose
#'   `df_dna_by_customer` table should be ensured.
#' @param table_name Character. Defaults to "df_dna_by_customer".
#' @param verbose Logical. If TRUE (default), prints messages about added
#'   columns or "schema is up to date".
#' @return Invisible character vector of added column names (empty if none).
#'
#' @examples
#' \dontrun{
#' con <- dbConnectDuckdb(db_path_list$app_data, read_only = FALSE)
#' added <- ensure_dna_schema(con)
#' DBI::dbDisconnect(con)
#' }
ensure_dna_schema <- function(target_con,
                              table_name = "df_dna_by_customer",
                              verbose = TRUE) {
  stopifnot(inherits(target_con, "DBIConnection"))

  # Skip if target table doesn't exist (D00 hasn't been run yet on this database)
  if (!DBI::dbExistsTable(target_con, table_name)) {
    if (verbose) {
      message(sprintf(
        "[ensure_dna_schema] Table '%s' does not exist on target connection; skipping schema check (run D00 init first).",
        table_name
      ))
    }
    return(invisible(character(0)))
  }

  # Source canonical schema function if not already loaded
  if (!exists("create_df_dna_by_customer_table", mode = "function", inherits = TRUE)) {
    candidate_paths <- c(
      if (exists("GLOBAL_DIR", inherits = TRUE)) {
        file.path(GLOBAL_DIR, "01_db", "fn_create_df_dna_by_customer_table.R")
      } else {
        NULL
      },
      file.path("scripts", "global_scripts", "01_db", "fn_create_df_dna_by_customer_table.R"),
      file.path("..", "global_scripts", "01_db", "fn_create_df_dna_by_customer_table.R"),
      file.path("..", "..", "global_scripts", "01_db", "fn_create_df_dna_by_customer_table.R"),
      file.path("shared", "global_scripts", "01_db", "fn_create_df_dna_by_customer_table.R")
    )
    candidate_paths <- candidate_paths[!is.null(candidate_paths)]
    found <- candidate_paths[file.exists(candidate_paths)]
    if (length(found) == 0) {
      stop("[ensure_dna_schema] Cannot locate fn_create_df_dna_by_customer_table.R in any expected path")
    }
    source(found[1])
  }

  # Introspect canonical schema via in-memory DuckDB
  mem_con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
  on.exit(DBI::dbDisconnect(mem_con, shutdown = TRUE), add = TRUE)

  create_df_dna_by_customer_table(mem_con, or_replace = TRUE, verbose = FALSE)

  canonical_cols <- DBI::dbGetQuery(mem_con, sprintf(
    "SELECT column_name, data_type
     FROM information_schema.columns
     WHERE table_name = '%s' AND table_schema = 'main'
     ORDER BY ordinal_position",
    table_name
  ))

  if (nrow(canonical_cols) == 0) {
    stop(sprintf(
      "[ensure_dna_schema] Canonical schema introspection returned 0 columns for '%s'; check fn_create_df_dna_by_customer_table.R",
      table_name
    ))
  }

  # Query target schema
  target_col_names <- DBI::dbListFields(target_con, table_name)

  # Compute missing columns
  missing_idx <- !(canonical_cols$column_name %in% target_col_names)
  missing_cols <- canonical_cols[missing_idx, , drop = FALSE]

  if (nrow(missing_cols) == 0) {
    if (verbose) {
      message(sprintf(
        "[ensure_dna_schema] Table '%s' schema is up to date (%d columns).",
        table_name, length(target_col_names)
      ))
    }
    return(invisible(character(0)))
  }

  # Add each missing column via ALTER TABLE
  if (verbose) {
    message(sprintf(
      "[ensure_dna_schema] Adding %d missing column(s) to '%s':",
      nrow(missing_cols), table_name
    ))
  }

  for (i in seq_len(nrow(missing_cols))) {
    col_name <- missing_cols$column_name[i]
    col_type <- missing_cols$data_type[i]
    sql <- sprintf(
      'ALTER TABLE %s ADD COLUMN %s %s',
      DBI::dbQuoteIdentifier(target_con, table_name),
      DBI::dbQuoteIdentifier(target_con, col_name),
      col_type
    )
    DBI::dbExecute(target_con, sql)
    if (verbose) {
      message(sprintf("  + %s %s", col_name, col_type))
    }
  }

  invisible(missing_cols$column_name)
}
