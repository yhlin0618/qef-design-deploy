#' @file fn_dbDetach_all.R
#' @use_package DBI
#'
#' @title Detach All Attached DuckDB Databases
#'
#' @description
#' Detaches all child databases that were previously attached to a DuckDB
#' connection via `ATTACH ... AS <alias>`.  This is useful when you need to
#' re-attach the same path or simply want to ensure the main catalog is the only
#' one remaining without closing the connection itself.  When `conn` is `NULL`,
#' the function scans the global environment for open DuckDB connections and
#' detaches their attached databases.
#'
#' @param conn   A `duckdb_connection` object.  If `NULL` (default) the function
#'               will attempt to find all valid DuckDB connections in
#'               `.GlobalEnv`.
#' @param keep_main Logical.  Should the main catalog (`memory`) be preserved?
#'                  Set to `FALSE` only if you intend to detach everything,
#'                  including the primary catalog (rare).  Default `TRUE`.
#' @param verbose Logical.  Print progress messages.  Default `TRUE`.
#'
#' @return Invisibly returns the number of `DETACH` operations performed.
#'
#' @examples
#' \dontrun{
#' con <- DBI::dbConnect(duckdb::duckdb(), "my.duckdb")
#' DBI::dbExecute(con, "ATTACH 'other.duckdb' AS other_db")
#' dbDetach_all(con)               # Detaches 'other_db'
#' DBI::dbDisconnect(con)
#' }
#'
#' @export
#' @seealso fn_dbDisconnect_all
#'
#' @author che

dbDetach_all <- function(conn = NULL, keep_main = TRUE, verbose = TRUE) {
  detach_one <- function(cn) {
    if (!inherits(cn, "duckdb_connection") || !DBI::dbIsValid(cn)) return(0L)
    dbs <- tryCatch(DBI::dbGetQuery(cn, "PRAGMA database_list;"),
                    error = function(e) data.frame())
    if (!nrow(dbs)) return(0L)
    # Always exclude the default database (seq == 0) to prevent detach errors
    # DuckDB doesn't allow detaching the default database
    default_names <- dbs$name[dbs$seq == 0]
    to_detach <- setdiff(dbs$name, default_names)
    
    # If keep_main is FALSE, we could theoretically detach other databases too
    # but we still can't detach the default database due to DuckDB limitations
    if (!keep_main) {
      # Even when keep_main=FALSE, we can't detach seq=0 databases
      # This is a DuckDB limitation, not our choice
      if (verbose && length(default_names) > 0) {
        message("Cannot detach default database(s): ", paste(default_names, collapse = ", "))
      }
    }
    n <- 0L
    for (alias in to_detach) {
      tryCatch({
        DBI::dbExecute(cn, sprintf("DETACH DATABASE %s;", alias))
        if (verbose) message("DETACHED ", alias)
        n <- n + 1L
      }, error = function(e) {
        if (verbose) {
          if (grepl("default database", e$message, ignore.case = TRUE)) {
            message("Skipped detaching default database: ", alias)
          } else {
            message("Failed to detach ", alias, ": ", e$message)
          }
        }
      })
    }
    n
  }

  total_detached <- 0L
  if (is.null(conn)) {
    # Search all objects in global environment
    objs <- ls(envir = .GlobalEnv)
    for (nm in objs) {
      obj <- get(nm, envir = .GlobalEnv, inherits = FALSE)
      if (inherits(obj, "duckdb_connection")) {
        total_detached <- total_detached + detach_one(obj)
      }
    }
  } else {
    total_detached <- detach_one(conn)
  }
  if (verbose) message("Total detached: ", total_detached)
  invisible(total_detached)
} 