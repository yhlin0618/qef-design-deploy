#!/usr/bin/env Rscript
# Complete migration of drv references to df
# Handles: table names, variable names, documentation, comments
#
# CRITICAL: This script implements R119 principle
# - ALL table names use df_ prefix (data frame)
# - DRV layer name references are preserved
#
# Author: principle-product-manager
# Date: 2025-11-13

library(stringr)

# Define patterns to EXCLUDE (these should NOT be changed)
exclude_patterns <- c(
  "DRV/",                    # Directory name
  "DRV layer",               # Layer name
  "DRV scripts",             # Layer reference
  "update_scripts/DRV",      # Path
  "MP109.*DRV",              # Principle name
  "mp109.*drv",              # Principle reference
  "fn_validate_etl_drv_template",  # Function name (kept for backward compat)
  "validate_etl_drv",        # Validation script names
  "precision_etl_drv",       # Existing file names
  "for_drv_developers",      # YAML comment
  "validate_drv_directory",  # Function name
  "drv_dir",                 # Parameter name for directory paths
  "drv_files",               # Parameter name for file lists
  "drv_path",                # Variable for file paths
  "drv_ro",                  # DuckDB read-only driver variable
  "drv_old",                 # Old connection driver
  "drv_new"                  # New connection driver
)

# Define replacement patterns for table names and variables
# Pattern: "df_{domain}_{type}" -> "df_{domain}_{type}"
table_replacements <- list(
  # CBZ domain tables
  list(pattern = '"df_cbz_product_features"', replacement = '"df_cbz_product_features"'),
  list(pattern = "'df_cbz_product_features'", replacement = "'df_cbz_product_features'"),
  list(pattern = '"df_cbz_time_series"', replacement = '"df_cbz_time_series"'),
  list(pattern = "'df_cbz_time_series'", replacement = "'df_cbz_time_series'"),
  list(pattern = '"df_cbz_poisson_analysis"', replacement = '"df_cbz_poisson_analysis"'),
  list(pattern = "'df_cbz_poisson_analysis'", replacement = "'df_cbz_poisson_analysis'"),

  # Precision domain tables
  list(pattern = '"df_precision_features"', replacement = '"df_precision_features"'),
  list(pattern = "'df_precision_features'", replacement = "'df_precision_features'"),
  list(pattern = '"df_precision_time_series"', replacement = '"df_precision_time_series"'),
  list(pattern = "'df_precision_time_series'", replacement = "'df_precision_time_series'"),
  list(pattern = '"df_precision_poisson_analysis"', replacement = '"df_precision_poisson_analysis"'),
  list(pattern = "'df_precision_poisson_analysis'", replacement = "'df_precision_poisson_analysis'"),
  list(pattern = '"df_precision_feature_preparation"', replacement = '"df_precision_feature_preparation"'),
  list(pattern = "'df_precision_feature_preparation'", replacement = "'df_precision_feature_preparation'"),

  # EBY domain tables (if any)
  list(pattern = '"df_ebay_', replacement = '"df_ebay_'),
  list(pattern = "'df_ebay_", replacement = "'df_ebay_"),
  list(pattern = '"df_eby_', replacement = '"df_eby_'),
  list(pattern = "'df_eby_", replacement = "'df_eby_"),

  # Generic pattern for sprintf() calls
  list(pattern = 'sprintf\\("df_%s_', replacement = 'sprintf("df_%s_'),
  list(pattern = "sprintf\\('df_%s_", replacement = "sprintf('df_%s_"),

  # Markdown table references (backticks)
  list(pattern = '`df_cbz_product_features`', replacement = '`df_cbz_product_features`'),
  list(pattern = '`df_cbz_time_series`', replacement = '`df_cbz_time_series`'),
  list(pattern = '`df_cbz_poisson_analysis`', replacement = '`df_cbz_poisson_analysis`'),
  list(pattern = '`df_precision_features`', replacement = '`df_precision_features`'),
  list(pattern = '`df_precision_time_series`', replacement = '`df_precision_time_series`'),
  list(pattern = '`df_precision_poisson_analysis`', replacement = '`df_precision_poisson_analysis`')
)

# Variable name replacements (less common, but need to check)
variable_replacements <- list(
  list(pattern = "\\bdrv_features\\s*<-", replacement = "df_features <-"),
  list(pattern = "\\bdrv_results\\s*<-", replacement = "df_results <-"),
  list(pattern = "\\bdrv_data\\s*<-", replacement = "df_data <-"),
  list(pattern = "\\bdrv_analysis\\s*<-", replacement = "df_analysis <-"),
  list(pattern = "\\bdrv_output\\s*<-", replacement = "df_output <-"),
  list(pattern = "\\bdrv_analytics\\s*<-", replacement = "df_analytics <-")
)

# Documentation patterns
doc_replacements <- list(
  # Headers
  list(pattern = "^#### df_", replacement = "#### df_"),
  list(pattern = "^### df_", replacement = "### df_"),

  # Comments
  list(pattern = "# df_precision_features", replacement = "# df_precision_features"),
  list(pattern = "# df_precision_time_series", replacement = "# df_precision_time_series"),
  list(pattern = "# df_precision_poisson_analysis", replacement = "# df_precision_poisson_analysis"),
  list(pattern = "# df_cbz_", replacement = "# df_cbz_"),

  # FROM clauses in SQL
  list(pattern = "FROM df_precision_poisson_analysis", replacement = "FROM df_precision_poisson_analysis"),
  list(pattern = "FROM df_precision_time_series", replacement = "FROM df_precision_time_series"),
  list(pattern = "FROM df_precision_features", replacement = "FROM df_precision_features"),
  list(pattern = "FROM df_cbz_", replacement = "FROM df_cbz_"),

  # Table listings
  list(pattern = "Tables: df_", replacement = "Tables: df_"),
  list(pattern = "- df_", replacement = "- df_"),
  list(pattern = "\\| df_", replacement = "| df_")
)

# File extensions to process
file_extensions <- c("*.R", "*.r", "*.Rmd", "*.qmd", "*.md", "*.txt")

# Directories to exclude
exclude_dirs <- c(".git", "renv", "archive")

# Helper function to check if line should be excluded
should_exclude <- function(line) {
  for (pattern in exclude_patterns) {
    if (grepl(pattern, line, ignore.case = FALSE)) {
      return(TRUE)
    }
  }
  return(FALSE)
}

# Find all relevant files
cat("=== Finding files to process ===\n")
files_to_process <- c()
for (ext in file_extensions) {
  cmd <- sprintf(
    "find . -name '%s' -type f %s 2>/dev/null",
    ext,
    paste(sprintf("! -path '*/%s/*'", exclude_dirs), collapse = " ")
  )
  files <- system(cmd, intern = TRUE)
  files_to_process <- c(files_to_process, files)
}

cat(sprintf("Found %d files to scan\n\n", length(files_to_process)))

# Process each file
changes_summary <- list()
files_modified <- 0
total_changes <- 0

for (file in files_to_process) {
  tryCatch({
    # Read file
    content <- readLines(file, warn = FALSE, encoding = "UTF-8")
    original_content <- content

    # Track changes for this file
    file_changes <- 0

    # Process line by line to respect exclusions
    for (i in seq_along(content)) {
      line <- content[i]
      original_line <- line

      # Skip excluded lines
      if (should_exclude(line)) {
        next
      }

      # Apply table replacements
      for (repl in table_replacements) {
        line <- str_replace_all(line, fixed(repl$pattern), repl$replacement)
      }

      # Apply variable replacements
      for (repl in variable_replacements) {
        line <- str_replace_all(line, repl$pattern, repl$replacement)
      }

      # Apply documentation replacements
      for (repl in doc_replacements) {
        line <- str_replace_all(line, repl$pattern, repl$replacement)
      }

      # Update if changed
      if (line != original_line) {
        content[i] <- line
        file_changes <- file_changes + 1
      }
    }

    # If file changed, write it
    if (file_changes > 0) {
      # Backup original
      backup_file <- paste0(file, ".backup_drv_", format(Sys.time(), "%Y%m%d_%H%M%S"))
      writeLines(original_content, backup_file)

      # Write updated content
      writeLines(content, file, useBytes = TRUE)

      files_modified <- files_modified + 1
      total_changes <- total_changes + file_changes

      changes_summary[[file]] <- list(
        changes = file_changes,
        backup = backup_file
      )

      cat(sprintf("✓ %s (%d changes)\n", file, file_changes))
    }
  }, error = function(e) {
    cat(sprintf("✗ %s: %s\n", file, e$message))
  })
}

# Summary
cat("\n=== Migration Summary ===\n")
cat(sprintf("Files scanned: %d\n", length(files_to_process)))
cat(sprintf("Files modified: %d\n", files_modified))
cat(sprintf("Total changes: %d\n", total_changes))

# Save detailed report
report_file <- sprintf("DRV_TO_DF_MIGRATION_DETAILS_%s.txt",
                       format(Sys.time(), "%Y%m%d_%H%M%S"))

sink(report_file)
cat("=== DRV to DF Migration Details ===\n")
cat(sprintf("Date: %s\n\n", Sys.time()))
cat(sprintf("Total files scanned: %d\n", length(files_to_process)))
cat(sprintf("Total files modified: %d\n", files_modified))
cat(sprintf("Total line changes: %d\n\n", total_changes))

cat("Modified files:\n")
for (file in names(changes_summary)) {
  info <- changes_summary[[file]]
  cat(sprintf("  %s: %d changes (backup: %s)\n",
              file, info$changes, info$backup))
}
sink()

cat(sprintf("\nDetailed report saved to: %s\n", report_file))
cat("\n✓ Migration complete!\n")
