#!/usr/bin/env Rscript
#
# Database Documentation Script
# Implements MP43: Database Documentation Principle
#
# This script generates comprehensive documentation for all project databases,
# creating Markdown files that document their structure and sample data.
#

# Initialize script
cat("Starting database documentation process...\n")

# Source the database summarization utility
source_file <- file.path("update_scripts", "global_scripts", "utils", "fn_summarize_database.R")
if (!file.exists(source_file)) {
  stop("Database summarization utility not found at: ", source_file)
}
source(source_file)

# Create output directory if it doesn't exist
docs_dir <- file.path("docs", "database")
if (!dir.exists(docs_dir)) {
  dir.create(docs_dir, recursive = TRUE)
  cat("Created output directory:", docs_dir, "\n")
}

# Document all databases in the project
document_all_databases(
  app_db_path = "app_data.duckdb",
  raw_db_path = "data/raw_data.duckdb",
  output_dir = docs_dir
)

cat("Database documentation process completed successfully.\n")
cat("Documentation files can be found in:", docs_dir, "\n")