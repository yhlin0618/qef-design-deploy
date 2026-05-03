# Simple test for tbl2 with DuckDB ATTACH via dbplyr::in_schema()
# Creates temp databases and verifies the canonical pattern works end-to-end.
# Updated for issue #369 — tbl2.DBIConnection is now a pure passthrough.

# Initialize environment
library(DBI)
library(duckdb)
library(dplyr)
library(dbplyr)

# Set working directory
setwd("/Users/che/Library/CloudStorage/Dropbox/che_workspace/projects/ai_martech/l4_enterprise/WISER")

# Initialize
needgoogledrive <- TRUE
source(".Rprofile")
autoinit()

cat("=== Simple tbl2 + in_schema() ATTACH Test ===\n\n")

# Create temporary databases for testing
temp_dir <- tempdir()
main_db_path <- file.path(temp_dir, "main_test.duckdb")
attached_db_path <- file.path(temp_dir, "attached_test.duckdb")

tryCatch({
  # Create main database
  main_con <- dbConnect(duckdb(), main_db_path)

  # Create attached database with test data
  attached_con <- dbConnect(duckdb(), attached_db_path)

  # Create test data in attached database
  test_data <- data.frame(
    id = 1:5,
    name = c("Alice", "Bob", "Charlie", "David", "Eve"),
    product_line_id = c("jew", "sop", "jew", "sop", "jew"),
    included_competiter = c(TRUE, FALSE, TRUE, FALSE, TRUE)
  )

  dbWriteTable(attached_con, "test_table", test_data)
  dbDisconnect(attached_con)

  # Attach the database
  dbExecute(main_con, paste0("ATTACH '", attached_db_path, "' AS attached_db"))

  cat("Test 1: Verify database attachment\n")
  attached_tables <- dbGetQuery(main_con, "PRAGMA table_list('attached_db')")
  cat("✓ Found", nrow(attached_tables), "tables in attached_db\n")

  cat("\nTest 2: Regular tbl2 on main database\n")
  # Copy table to main database for regular test
  dbExecute(main_con, "CREATE TABLE main_table AS SELECT * FROM attached_db.test_table")

  regular_result <- tbl2(main_con, "main_table") %>%
    head(3) %>%
    collect()

  cat("✓ Regular tbl2 works:", nrow(regular_result), "rows\n")

  cat("\nTest 3: tbl2 with in_schema() for attached database\n")

  attached_ref <- in_schema("attached_db", "test_table")
  attached_result <- tbl2(main_con, attached_ref) %>%
    head(3) %>%
    collect()

  cat("✓ tbl2 + in_schema() works:", nrow(attached_result), "rows\n")

  cat("\nTest 4: dplyr operations over in_schema reference\n")

  # Test filtering
  filtered_result <- tbl2(main_con, attached_ref) %>%
    filter(included_competiter == TRUE) %>%
    collect()

  cat("✓ Filtering works:", nrow(filtered_result), "competitor records\n")

  # Test grouping
  grouped_result <- tbl2(main_con, attached_ref) %>%
    group_by(product_line_id) %>%
    summarise(count = n()) %>%
    collect()

  cat("✓ Grouping works:\n")
  for (i in seq_len(nrow(grouped_result))) {
    cat("  ", grouped_result$product_line_id[i], ":", grouped_result$count[i], "\n")
  }

  # Clean up
  dbDisconnect(main_con)
  unlink(main_db_path)
  unlink(attached_db_path)

  cat("\n=== All tests passed successfully! ===\n")

}, error = function(e) {
  cat("❌ ERROR:", e$message, "\n")

  # Clean up on error
  if (exists("main_con")) {
    try(dbDisconnect(main_con), silent = TRUE)
  }
  if (exists("attached_con")) {
    try(dbDisconnect(attached_con), silent = TRUE)
  }
  try(unlink(main_db_path), silent = TRUE)
  try(unlink(attached_db_path), silent = TRUE)
})
