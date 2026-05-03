#' Backward-Compatible DB Connector
#'
#' Legacy wrapper that resolves a database path from `db_path_list` and opens
#' a DuckDB connection via `dbConnectDuckdb()`.
#'
#' This keeps older scripts working after DB utility reorganization under
#' `02_db_utils/duckdb/`.
#'
#' @param dataset Character. Database key in `db_path_list`.
#' @param path_list Optional list of database paths. Falls back to global
#'   `db_path_list` and then `get_default_db_paths()`.
#' @param read_only Logical. Open connection in read-only mode. Defaults FALSE.
#' @param verbose Logical. Print connection diagnostics. Defaults FALSE.
#'
#' @return A DBI connection object.
fn_dbConnect_from_list <- function(dataset, path_list = NULL, read_only = FALSE, verbose = FALSE) {
  load_helper <- function(rel_path) {
    candidates <- c(
      if (exists("GLOBAL_DIR", inherits = TRUE)) {
        file.path(get("GLOBAL_DIR", inherits = TRUE), "02_db_utils", rel_path)
      } else {
        character(0)
      },
      file.path("scripts", "global_scripts", "02_db_utils", rel_path),
      file.path("..", "global_scripts", "02_db_utils", rel_path),
      file.path("..", "..", "global_scripts", "02_db_utils", rel_path),
      file.path("02_db_utils", rel_path)
    )
    found <- candidates[file.exists(candidates)][1]
    if (is.na(found)) {
      stop("Required DB utility not found: ", rel_path)
    }
    source(found)
    if (isTRUE(verbose)) message("Loaded helper: ", found)
  }

  if (missing(dataset) || is.null(dataset) || !nzchar(as.character(dataset)[1])) {
    stop("dataset is required and must be non-empty")
  }
  dataset <- as.character(dataset)[1]

  if (!exists("get_default_db_paths", mode = "function")) {
    load_helper(file.path("duckdb", "fn_get_default_db_paths.R"))
  }
  if (!exists("dbConnectDuckdb", mode = "function")) {
    load_helper(file.path("duckdb", "fn_dbConnectDuckdb.R"))
  }

  resolved_paths <- path_list
  if (is.null(resolved_paths) || length(resolved_paths) == 0) {
    if (exists("db_path_list", inherits = TRUE)) {
      resolved_paths <- get("db_path_list", inherits = TRUE)
    } else {
      resolved_paths <- get_default_db_paths()
      assign("db_path_list", resolved_paths, envir = .GlobalEnv)
    }
  }

  if (!is.list(resolved_paths) || length(resolved_paths) == 0) {
    stop("No database path list available")
  }
  if (!dataset %in% names(resolved_paths)) {
    stop(
      "Unknown dataset key: ", dataset,
      ". Available keys: ", paste(names(resolved_paths), collapse = ", ")
    )
  }

  db_path <- as.character(resolved_paths[[dataset]])[1]
  if (is.na(db_path) || !nzchar(db_path)) {
    stop("Invalid database path for dataset: ", dataset)
  }

  if (isTRUE(verbose)) {
    message("Connecting dataset '", dataset, "' -> ", db_path, " (read_only=", read_only, ")")
  }
  dbConnectDuckdb(db_path = db_path, read_only = read_only)
}

# Legacy alias expected by older scripts.
if (!exists("dbConnect_from_list", mode = "function")) {
  dbConnect_from_list <- fn_dbConnect_from_list
}
