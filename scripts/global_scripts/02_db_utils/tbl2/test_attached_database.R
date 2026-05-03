# Test script for tbl2 attached database support
# Uses dbplyr::in_schema() for schema-qualified access (canonical pattern).
# Replaces the legacy dot-syntax tbl2(con, "db.table") which was removed
# in the tbl2.DBIConnection pure-passthrough refactor (issue #369).

# Initialize environment
library(DBI)
library(duckdb)
library(dplyr)
library(dbplyr)

# Set working directory and initialize
setwd("/Users/che/Library/CloudStorage/Dropbox/che_workspace/projects/ai_martech/l4_enterprise/WISER")

# Initialize using autoinit system
needgoogledrive <- TRUE
source(".Rprofile")
autoinit()

# tbl2 and dbAttachDuckdb are already loaded by autoinit()
# No need to source them again

cat("=== Testing tbl2 Attached Database Support ===\n\n")

# Test the attached database functionality
tryCatch({
  # Connect to comment_property_rating database
  comment_property_rating <- dbConnectDuckdb(db_path_list$comment_property_rating, read_only = TRUE)
  
  # Mount transformed_data database
  dbAttachDuckdb(
    con = comment_property_rating,
    path = db_path_list$transformed_data,
    alias = "transformed_data",
    read_only = TRUE
  )
  
  # Test 1: Check if we can list tables in attached database
  cat("Test 1: Checking attached database connection...\n")
  attached_tables <- dbGetQuery(comment_property_rating, "PRAGMA table_list('transformed_data')")
  cat("✓ Found", nrow(attached_tables), "tables in transformed_data\n")
  cat("  Tables:", paste(attached_tables$name, collapse = ", "), "\n\n")
  
  # Test 2: Test tbl2 with dbplyr::in_schema() for attached database
  cat("Test 2: Testing tbl2 with in_schema() for attached database...\n")

  # Try to access the transformed review table via in_schema()
  if ("df_amz_review___transformed" %in% attached_tables$name) {
    cat("  Attempting to access transformed_data.df_amz_review___transformed via in_schema()...\n")

    # Use canonical dbplyr pattern
    review_ref <- in_schema("transformed_data", "df_amz_review___transformed")
    result <- tbl2(comment_property_rating, review_ref) %>%
      head(3) %>%
      collect()

    cat("  ✓ SUCCESS: Retrieved", nrow(result), "rows\n")
    cat("  Columns:", paste(names(result)[1:min(5, length(names(result)))], collapse = ", "), "...\n")

    # Test specific filtering
    if ("included_competiter" %in% names(result)) {
      competitor_count <- tbl2(comment_property_rating, review_ref) %>%
        filter(included_competiter == TRUE) %>%
        count() %>%
        pull(n)
      cat("  ✓ Found", competitor_count, "competitor reviews\n")
    }

  } else {
    cat("  ⚠ WARNING: df_amz_review___transformed not found in attached database\n")
  }

  # Test 3: Test property table access
  if ("df_all_comment_property___transformed" %in% attached_tables$name) {
    cat("  Attempting to access transformed_data.df_all_comment_property___transformed via in_schema()...\n")

    property_ref <- in_schema("transformed_data", "df_all_comment_property___transformed")
    property_result <- tbl2(comment_property_rating, property_ref) %>%
      head(2) %>%
      collect()

    cat("  ✓ SUCCESS: Retrieved", nrow(property_result), "property records\n")
  }
  
  cat("\n")
  
  # Test 4: Test regular table access (should still work)
  cat("Test 3: Testing regular table access (backward compatibility)...\n")
  regular_tables <- dbListTables(comment_property_rating)
  if (length(regular_tables) > 0) {
    regular_result <- tbl2(comment_property_rating, regular_tables[1]) %>%
      head(1) %>%
      collect()
    cat("  ✓ SUCCESS: Regular table access works, got", nrow(regular_result), "rows from", regular_tables[1], "\n")
  }
  
  # Test 5: Test with dplyr chain
  cat("\nTest 4: Testing with dplyr chain operations...\n")
  if ("df_amz_review___transformed" %in% attached_tables$name) {
    chain_ref <- in_schema("transformed_data", "df_amz_review___transformed")
    chain_result <- tbl2(comment_property_rating, chain_ref) %>%
      filter(!is.na(product_line_id)) %>%
      group_by(product_line_id) %>%
      summarise(count = n()) %>%
      collect()
    
    cat("  ✓ SUCCESS: Dplyr chain operations work\n")
    cat("  Summary by product_line_id:\n")
    for (i in seq_len(nrow(chain_result))) {
      cat("    ", chain_result$product_line_id[i], ":", chain_result$count[i], "reviews\n")
    }
  }
  
  # Clean up
  dbDisconnect(comment_property_rating)
  cat("\n=== All tests completed successfully! ===\n")
  
}, error = function(e) {
  cat("❌ ERROR:", e$message, "\n")
  if (exists("comment_property_rating")) {
    try(dbDisconnect(comment_property_rating), silent = TRUE)
  }
})