# Test tbl2 with DuckDB ATTACH via dbplyr::in_schema()
# After issue #369, tbl2.DBIConnection is a pure passthrough to dplyr::tbl().
# Schema-qualified access uses the canonical dbplyr pattern `in_schema()`.

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

cat("=== Testing tbl2 with DuckDB ATTACH + in_schema() ===\n\n")

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
    id = 1:3,
    name = c("Alice", "Bob", "Charlie"),
    value = c(10, 20, 30)
  )

  dbWriteTable(attached_con, "test_table", test_data)
  dbDisconnect(attached_con)

  # Attach the database
  dbExecute(main_con, paste0("ATTACH '", attached_db_path, "' AS attached_db"))

  cat("Test 1: Verify database attachment\n")
  databases <- dbGetQuery(main_con, "PRAGMA database_list")
  attached_db_found <- "attached_db" %in% databases$name
  cat("✓ Attached database found:", attached_db_found, "\n")

  cat("\nTest 2: Direct SQL with attached database (baseline)\n")
  direct_sql_result <- dbGetQuery(main_con, "SELECT * FROM attached_db.test_table LIMIT 1")
  cat("✓ Direct SQL works:", nrow(direct_sql_result), "rows\n")

  cat("\nTest 3: tbl2 with in_schema() for attached database\n")

  attached_ref <- in_schema("attached_db", "test_table")
  tbl2_result <- tbl2(main_con, attached_ref) %>%
    head(2) %>%
    collect()

  cat("✓ tbl2 + in_schema() works:", nrow(tbl2_result), "rows\n")
  cat("  Columns:", paste(names(tbl2_result), collapse = ", "), "\n")

  cat("\nTest 4: dplyr filter via in_schema reference\n")

  filtered_result <- tbl2(main_con, attached_ref) %>%
    filter(value > 15) %>%
    collect()

  cat("✓ Filtering works:", nrow(filtered_result), "rows with value > 15\n")

  cat("\nTest 5: dplyr select via in_schema reference\n")

  selected_result <- tbl2(main_con, attached_ref) %>%
    select(name, value) %>%
    collect()

  cat("✓ Selection works:", ncol(selected_result), "columns selected\n")

  # Clean up
  dbDisconnect(main_con)
  unlink(main_db_path)
  unlink(attached_db_path)

  cat("\n=== All tests passed! ===\n")
  cat("✓ tbl2 + in_schema() correctly accesses DuckDB ATTACH tables\n")

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
