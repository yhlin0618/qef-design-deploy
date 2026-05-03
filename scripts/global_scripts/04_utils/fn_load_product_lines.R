#' Load Product Line Data into Database and Memory
#'
#' Reads `df_product_line` from the **canonical runtime source** and caches it
#' in the provided SQLite connection as `df_product_line_profile`. Follows
#' R119 Memory-Resident Parameters Rule.
#'
#' DM_R054 v2.1 (2026-04-19) revised by #424 hotfix (2026-04-20):
#' The canonical runtime source depends on `app_config.yaml > database.mode`:
#'
#'   - `duckdb` (local dev / UPDATE_MODE): read from `meta_data.duckdb` at
#'     the path declared in `db_paths.yaml` (usually populated by autoinit
#'     into `db_path_list$meta_data`).
#'   - `supabase` (Posit Connect / production): read from the live Supabase
#'     PostgreSQL `df_product_line` table via the project-standard
#'     `dbConnectAppData()` helper (MP142, DM_R023).
#'   - `auto`: DuckDB if `meta_data.duckdb` is reachable, else Supabase.
#'
#' There is still NO CSV fallback at runtime (DM_R054 v2.1 §6). The CSV at
#' `data/app_data/parameters/scd_type1/df_product_line.csv` is a bootstrap
#' seed consumed ONLY by the producer ETL `all_ETL_meta_init_0IM.R` (which
#' writes local `meta_data.duckdb`) or by whatever ETL populates Supabase.
#'
#' @param conn A DBI connection object (typically SQLite) to store the
#'   `df_product_line_profile` table for the app's in-memory lookup.
#' @param meta_data_path Absolute path to a `meta_data.duckdb` that already
#'   has a `df_product_line` table. Used for the `duckdb` / `auto` branches.
#'   Defaults to `db_path_list$meta_data` if available.
#' @param csv_path Deprecated / reserved for signature compatibility only.
#'   NEVER consulted at runtime.
#' @param mode One of `"auto"` (default), `"duckdb"`, `"supabase"`. `"auto"`
#'   uses `meta_data.duckdb` when reachable, else falls through to
#'   `dbConnectAppData()` so the Supabase branch can run on Posit Connect.
#' @param verbose Logical. Print progress messages. Default `TRUE`.
#'
#' @return Invisibly returns the product_lines data frame.
#'
#' @examples
#' \dontrun{
#' conn <- DBI::dbConnect(RSQLite::SQLite(), ":memory:")
#' # Local dev (DuckDB)
#' product_lines <- load_product_lines(
#'   conn,
#'   meta_data_path = db_path_list$meta_data
#' )
#' # Force Supabase (deployment smoke test)
#' product_lines <- load_product_lines(conn, mode = "supabase")
#' }
#'
#' @export
load_product_lines <- function(
  conn,
  meta_data_path = if (exists("db_path_list") && !is.null(db_path_list$meta_data))
                   db_path_list$meta_data else NULL,
  csv_path = NULL,
  mode = "auto",
  verbose = TRUE
) {
  if (!is.null(csv_path) && verbose) {
    message("\U21E2 load_product_lines: ignoring csv_path argument (DM_R054 ",
            "v2.1 \u00A76: runtime MUST NOT read CSV seeds).")
  }

  mode <- match.arg(mode, c("auto", "duckdb", "supabase"))

  duckdb_reachable <-
    !is.null(meta_data_path) &&
    is.character(meta_data_path) && nzchar(meta_data_path) &&
    file.exists(meta_data_path)

  use_duckdb <-
    (mode == "duckdb") ||
    (mode == "auto" && duckdb_reachable)

  product_lines <- NULL
  source_label <- NULL

  if (use_duckdb) {
    if (!duckdb_reachable) {
      # Security F1 (#427): redact absolute path in UI-facing stop();
      # full path still logged via message() above for operator debug.
      stop(
        "load_product_lines(mode='duckdb') requires a reachable meta_data.duckdb.\n",
        "  file checked: ",
        if (is.null(meta_data_path)) "(db_path_list$meta_data not set)"
        else basename(meta_data_path), " (full path in server log)\n",
        "Fix: run `Rscript shared/update_scripts/ETL/all/all_ETL_meta_init_0IM.R` ",
        "to produce meta_data.duckdb from the CSV seed, or switch ",
        "app_config.yaml > database.mode to 'supabase'/'auto'.",
        call. = FALSE
      )
    }
    if (verbose) {
      message("\U21E2 Loading product lines from meta_data.duckdb: ",
              meta_data_path)
    }
    meta_con <- DBI::dbConnect(duckdb::duckdb(), meta_data_path, read_only = TRUE)
    on.exit(try(DBI::dbDisconnect(meta_con, shutdown = TRUE), silent = TRUE),
            add = TRUE)
    if (!("df_product_line" %in% DBI::dbListTables(meta_con))) {
      # Security F1 (#427): redact absolute path in UI-facing stop();
      # full path still logged via message() above for operator debug.
      stop(
        "meta_data.duckdb exists but contains no df_product_line table.\n",
        "  file: ", basename(meta_data_path), " (full path in server log)\n",
        "Fix: run `Rscript shared/update_scripts/ETL/all/all_ETL_meta_init_0IM.R` ",
        "to (re)bootstrap metadata tables from the CSV seed.",
        call. = FALSE
      )
    }
    product_lines <- DBI::dbReadTable(meta_con, "df_product_line")
    # Security F1 (#427): source_label propagates into downstream stop()
    # messages (e.g. zero-row guard below, get_active_product_lines), which
    # can surface in Shiny UI. basename() avoids leaking absolute path + user
    # directory structure. Full path logged via message() above.
    source_label <- paste0("meta_data.duckdb (", basename(meta_data_path), ")")

  } else {
    # Supabase branch (mode == "supabase" OR mode == "auto" && !duckdb_reachable)
    if (!exists("dbConnectAppData", mode = "function")) {
      helper_path <- "scripts/global_scripts/02_db_utils/fn_dbConnectAppData.R"
      if (file.exists(helper_path)) {
        source(helper_path, local = FALSE)
      } else {
        stop(
          "Cannot load product lines — dbConnectAppData() not loaded and ",
          helper_path, " not found (DM_R054 v2.1.1, no fallback).\n",
          "Fix (depending on deploy mode):\n",
          "  - DuckDB mode: run `Rscript shared/update_scripts/ETL/all/all_ETL_meta_init_0IM.R` ",
          "to bootstrap meta_data.duckdb from the CSV seed, OR ensure 02_db_utils is sourced in your init script.\n",
          "  - Supabase mode: ensure SUPABASE_DB_HOST + SUPABASE_DB_PASSWORD are set and 02_db_utils is sourced.",
          call. = FALSE
        )
      }
    }
    if (verbose) {
      message("\U21E2 Loading product lines via dbConnectAppData() ",
              "(mode=", mode,
              if (mode == "auto") ", DuckDB unavailable" else "",
              ")")
    }
    # #427 F21 test regression fix: test_fn_load_product_lines_no_fallback.R
    # Test 1 exercises mode="auto" + missing meta_data.duckdb path, which
    # falls through here; when dbConnectAppData() raises (e.g. Supabase
    # creds absent in a dev shell), the native error doesn't name the
    # bootstrap ETL or the "no fallback" contract. Wrap to produce a
    # stop() that surfaces both remediation paths.
    app_con <- tryCatch(
      dbConnectAppData(verbose = verbose),
      error = function(e) {
        stop(
          "load_product_lines(mode='", mode,
          "'): dbConnectAppData() failed (DM_R054 v2.1.1, no fallback).\n",
          "  underlying error: ", conditionMessage(e), "\n",
          "Fix (depending on deploy mode):\n",
          "  - DuckDB mode: run `Rscript shared/update_scripts/ETL/all/all_ETL_meta_init_0IM.R` ",
          "to bootstrap meta_data.duckdb from the CSV seed.\n",
          "  - Supabase mode: set SUPABASE_DB_HOST + SUPABASE_DB_PASSWORD ",
          "(Posit Connect Variable Set).",
          call. = FALSE
        )
      }
    )
    conn_type <- attr(app_con, "connection_type")
    on.exit(try(DBI::dbDisconnect(app_con), silent = TRUE), add = TRUE)

    existing <- try(DBI::dbListTables(app_con), silent = TRUE)
    if (inherits(existing, "try-error") ||
        !("df_product_line" %in% existing)) {
      stop(
        "load_product_lines(): df_product_line not found via ",
        "dbConnectAppData() (connection_type=",
        conn_type %||% "unknown", ").\n",
        "Verify the metadata table is populated in the active backend; ",
        "re-run the ETL that loads df_product_line.",
        call. = FALSE
      )
    }
    product_lines <- DBI::dbReadTable(app_con, "df_product_line")
    source_label <- paste0("dbConnectAppData (", conn_type %||% "unknown", ")")
  }

  if (verbose) {
    message("\U21E2 df_product_line loaded from: ", source_label,
            " (rows=", nrow(product_lines), ")")
  }

  # --- Validate schema ---------------------------------------------------
  required_fields <- c("product_line_name_english",
                       "product_line_name_chinese",
                       "product_line_id")
  missing_fields <- setdiff(required_fields, names(product_lines))
  if (length(missing_fields) > 0) {
    stop("product_lines source missing required fields: ",
         paste(missing_fields, collapse = ", "),
         " (source = ", source_label, ")",
         call. = FALSE)
  }

  # --- Zero-row guard (#427 F21) ----------------------------------------
  # An empty df_product_line table (no rows) would otherwise silently flow
  # into get_active_product_lines() and trip an unhelpful
  # "no active product lines found" downstream. Fail-fast here with the
  # source identity so the operator knows where to fix the data.
  if (nrow(product_lines) == 0) {
    stop(
      "df_product_line is empty at the canonical source (source = ",
      source_label, ").\n",
      "Nothing was loaded — downstream readers would trip with a vague ",
      "'no active product lines' error. Fix the source:\n",
      "  - DuckDB mode: re-run all_ETL_meta_init_0IM.R after restoring the ",
      "CSV seed (data/app_data/parameters/scd_type1/df_product_line.csv).\n",
      "  - Supabase mode: verify public.df_product_line is populated by ",
      "the upload ETL.",
      call. = FALSE
    )
  }

  # R118: Normalize product_line_id to lowercase
  uppercase_ids <- product_lines$product_line_id[
    grepl("[A-Z]", product_lines$product_line_id)
  ]
  if (length(uppercase_ids) > 0) {
    warning("Converting uppercase product_line_ids to lowercase: ",
            paste(uppercase_ids, collapse = ", "))
    product_lines$product_line_id <- tolower(product_lines$product_line_id)
  }

  # --- Store in SQLite conn (in-memory app cache) -----------------------
  if (verbose) {
    message("\U21E2 Storing product line data in SQLite: df_product_line_profile")
  }
  DBI::dbExecute(conn, "
    CREATE TABLE IF NOT EXISTS df_product_line_profile (
      product_line_id CHAR(3) PRIMARY KEY,
      product_line_name_english TEXT NOT NULL,
      product_line_name_chinese TEXT
    )
  ")
  DBI::dbWriteTable(conn, "df_product_line_profile", product_lines,
                    overwrite = TRUE)

  return(product_lines)
}

# `%||%` fallback for R < 4.4
if (!exists("%||%", mode = "function")) {
  `%||%` <- function(a, b) if (is.null(a)) b else a
}
