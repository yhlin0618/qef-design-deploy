# Test script for tbl2 attached database support
# Uses dbplyr::in_schema() — replaces legacy dot-syntax after issue #369.
library(DBI)
library(duckdb)
library(dplyr)
library(dbplyr)

# Source the updated tbl2 function
source("scripts/global_scripts/02_db_utils/tbl2/fn_tbl2.R")

# Initialize environment
OPERATION_MODE <- "app"
source("scripts/global_scripts/22_initializations/sc_initialization_app_mode.R")

# Test the attached database functionality
tryCatch({
  # Connect to comment_property_rating database
  comment_property_rating <- dbConnectDuckdb(db_path_list$comment_property_rating, read_only = TRUE)

  # Mount transformed_data database
  source("scripts/global_scripts/02_db_utils/fn_dbAttachDuckdb.R")
  dbAttachDuckdb(
    con = comment_property_rating,
    path = db_path_list$transformed_data,
    alias = "transformed_data",
    read_only = TRUE
  )

  # Test 1: Check if we can list tables in attached database
  cat("Testing attached database connection...\n")
  attached_tables <- dbGetQuery(comment_property_rating, "PRAGMA table_list('transformed_data')")
  cat("Found", nrow(attached_tables), "tables in transformed_data\n")

  # Test 2: Test tbl2 with in_schema() for attached database
  cat("Testing tbl2 with in_schema() for attached database...\n")

  # Try to access the transformed review table
  if ("df_amz_review___transformed" %in% attached_tables$name) {
    cat("Attempting to access transformed_data.df_amz_review___transformed via in_schema()...\n")

    review_ref <- in_schema("transformed_data", "df_amz_review___transformed")
    result <- tbl2(comment_property_rating, review_ref) %>%
      head(3) %>%
      collect()

    cat("SUCCESS: Retrieved", nrow(result), "rows\n")
    cat("Columns:", paste(names(result), collapse = ", "), "\n")

    # Test specific filtering
    if ("included_competiter" %in% names(result)) {
      competitor_count <- tbl2(comment_property_rating, review_ref) %>%
        filter(included_competiter == TRUE) %>%
        count() %>%
        pull(n)
      cat("Found", competitor_count, "competitor reviews\n")
    }

  } else {
    cat("WARNING: df_amz_review___transformed not found in attached database\n")
  }

  # Test 3: Test regular table access (should still work)
  cat("Testing regular table access...\n")
  regular_tables <- dbListTables(comment_property_rating)
  if (length(regular_tables) > 0) {
    regular_result <- tbl2(comment_property_rating, regular_tables[1]) %>%
      head(1) %>%
      collect()
    cat("SUCCESS: Regular table access works, got", nrow(regular_result), "rows\n")
  }

  # Clean up
  dbDisconnect(comment_property_rating)
  cat("Test completed successfully!\n")

}, error = function(e) {
  cat("ERROR:", e$message, "\n")
  if (exists("comment_property_rating")) {
    dbDisconnect(comment_property_rating)
  }
})
