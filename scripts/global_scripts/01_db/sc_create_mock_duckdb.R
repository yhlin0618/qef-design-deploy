# ====================================================================
# sc_create_mock_duckdb.R
#
# Utility to create a mock DuckDB database under
# scripts/global_scripts/30_global_data for development and manual testing.
#
# Important:
# - This file is sourced during autoinit(), so it must not have side effects.
# - Call create_mock_duckdb() explicitly, or run this file with Rscript.
# ====================================================================

create_mock_duckdb <- function(root_dir = getwd(), verbose = TRUE) {
  if (!requireNamespace("DBI", quietly = TRUE)) {
    install.packages("DBI")
  }
  if (!requireNamespace("duckdb", quietly = TRUE)) {
    install.packages("duckdb")
  }

  library(DBI)
  library(duckdb)

  db_dir <- file.path(root_dir, "scripts", "global_scripts", "30_global_data")
  if (!dir.exists(db_dir)) {
    dir.create(db_dir, recursive = TRUE, showWarnings = FALSE)
    if (verbose) message("Created directory: ", db_dir)
  }

  db_path <- file.path(db_dir, "mock_data.duckdb")

  if (verbose) {
    message("DuckDB will be created at: ", db_path)
    cat("\n======== MOCK DUCKDB CREATION UTILITY ========\n")
    cat("Creating database at:", db_path, "\n\n")
    cat("Tables being created:\n")
    cat("- customer_profile: Basic customer information\n")
    cat("- orders: Sample order data\n\n")
    cat("To use this database in your code:\n")
    cat('con <- DBI::dbConnect(duckdb::duckdb(), dbdir = "', db_path, '")\n\n')
  }

  con <- dbConnect(duckdb::duckdb(), dbdir = db_path, read_only = FALSE)
  on.exit(dbDisconnect(con, shutdown = TRUE), add = TRUE)

  customer_profile <- data.frame(
    id = 1:3,
    name = c("Alice", "Bob", "Charlie"),
    signup_date = as.Date(c("2021-01-01", "2021-06-15", "2021-12-31"))
  )

  orders <- data.frame(
    order_id = 1001:1003,
    customer_id = c(1, 2, 3),
    amount = c(99.99, 149.50, 20.00),
    order_date = Sys.Date() - c(10, 5, 1)
  )

  dbWriteTable(con, "customer_profile", customer_profile, overwrite = TRUE)
  dbWriteTable(con, "orders", orders, overwrite = TRUE)

  if (verbose) {
    tables <- dbListTables(con)
    message("Created tables in ", db_path, ": ", paste(tables, collapse = ", "))
    print(dbGetQuery(con, "SELECT * FROM customer_profile LIMIT 5"))
    cat("\n======== ADDITIONAL NOTES ========\n")
    cat("The mock database has been successfully created and populated.\n")
    cat("Remember to disconnect when done: dbDisconnect(con, shutdown = TRUE)\n")
    cat("================================\n")
  }

  invisible(db_path)
}

if (sys.nframe() == 0L) {
  create_mock_duckdb()
}
