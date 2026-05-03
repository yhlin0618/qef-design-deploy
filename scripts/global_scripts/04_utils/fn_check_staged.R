check_staged_db <- function(db_path = "data/local_data/staged_data.duckdb",
                            verbose = TRUE,
                            stop_on_missing = FALSE) {
  if (!requireNamespace("DBI", quietly = TRUE) || !requireNamespace("duckdb", quietly = TRUE)) {
    stop("DBI and duckdb packages are required to check staged_data.duckdb")
  }

  if (!file.exists(db_path)) {
    msg <- paste0("staged_data.duckdb not found at: ", db_path)
    if (isTRUE(stop_on_missing)) {
      stop(msg)
    }
    if (isTRUE(verbose)) message(msg)
    return(invisible(NULL))
  }

  con <- DBI::dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = TRUE)
  on.exit(DBI::dbDisconnect(con), add = TRUE)

  tables <- DBI::dbListTables(con)
  eby_tables <- tables[grep("eby", tables)]

  if (isTRUE(verbose)) {
    cat("Tables in staged_data.duckdb:\n")
    print(tables)
    cat("\neBay related tables:\n")
    print(eby_tables)
  }

  invisible(list(tables = tables, eby_tables = eby_tables))
}
