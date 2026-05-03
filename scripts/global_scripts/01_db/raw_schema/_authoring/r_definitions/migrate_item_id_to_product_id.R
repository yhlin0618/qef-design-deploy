#!/usr/bin/env Rscript

#' Migration Script: Rename item_id to product_id in df_position Table
#'
#' This script migrates the df_position table from using item_id to product_id
#' as the primary product identifier column.
#'
#' @principle MP058 Database Table Creation Strategy
#' @principle DM_R023 Universal DBI Approach

library(DBI)
library(duckdb)

#' Perform Migration from item_id to product_id
#'
#' @param con Database connection
#' @param table_name Name of the table to migrate (default: "df_position")
#' @param backup_table Name for backup table (default: "df_position_backup_[timestamp]")
#' @param dry_run If TRUE, only shows what would be done without executing (default: FALSE)
#' @param verbose If TRUE, shows detailed progress messages (default: TRUE)
#'
#' @return List with migration status and details
migrate_item_id_to_product_id <- function(
  con,
  table_name = "df_position",
  backup_table = NULL,
  dry_run = FALSE,
  verbose = TRUE
) {

  # Generate backup table name if not provided
  if (is.null(backup_table)) {
    backup_table <- paste0(table_name, "_backup_", format(Sys.time(), "%Y%m%d_%H%M%S"))
  }

  migration_result <- list(
    success = FALSE,
    original_table = table_name,
    backup_table = backup_table,
    steps_completed = character(),
    errors = character()
  )

  tryCatch({

    # Step 1: Check if table exists and has item_id column
    if (verbose) message("\n=== Migration: item_id → product_id ===\n")

    if (!dbExistsTable(con, table_name)) {
      stop(sprintf("Table '%s' does not exist", table_name))
    }

    columns <- dbListFields(con, table_name)
    has_item_id <- "item_id" %in% columns
    has_product_id <- "product_id" %in% columns

    if (!has_item_id) {
      if (has_product_id) {
        if (verbose) message("✓ Table already uses product_id. No migration needed.")
        migration_result$success <- TRUE
        migration_result$steps_completed <- c("Already migrated")
        return(migration_result)
      } else {
        stop("Table has neither item_id nor product_id column")
      }
    }

    if (verbose) {
      message(sprintf("Current table structure for '%s':", table_name))
      message(sprintf("  - Has item_id: %s", has_item_id))
      message(sprintf("  - Has product_id: %s", has_product_id))

      # Show sample data
      sample_query <- sprintf("SELECT * FROM %s LIMIT 3", table_name)
      sample_data <- dbGetQuery(con, sample_query)
      if (nrow(sample_data) > 0) {
        message("\nSample data (first 3 rows):")
        print(sample_data)
      }
    }

    if (dry_run) {
      message("\n--- DRY RUN MODE ---")
      message("The following steps would be performed:")
      message(sprintf("1. Create backup table: %s", backup_table))
      message("2. Create new table structure with product_id instead of item_id")
      message("3. Copy data from item_id to product_id column")
      message("4. Drop original table and rename new table")
      message("5. Recreate indexes")
      migration_result$success <- TRUE
      migration_result$steps_completed <- c("Dry run completed")
      return(migration_result)
    }

    # Step 2: Create backup
    if (verbose) message(sprintf("\nStep 1: Creating backup table '%s'...", backup_table))

    backup_query <- sprintf("CREATE TABLE %s AS SELECT * FROM %s", backup_table, table_name)
    dbExecute(con, backup_query)

    # Verify backup
    backup_count <- dbGetQuery(con, sprintf("SELECT COUNT(*) as n FROM %s", backup_table))$n
    original_count <- dbGetQuery(con, sprintf("SELECT COUNT(*) as n FROM %s", table_name))$n

    if (backup_count != original_count) {
      stop(sprintf("Backup verification failed: %d rows in backup vs %d in original",
                   backup_count, original_count))
    }

    if (verbose) message(sprintf("  ✓ Backup created with %d rows", backup_count))
    migration_result$steps_completed <- c(migration_result$steps_completed, "Backup created")

    # Step 3: Get current table structure
    if (verbose) message("\nStep 2: Analyzing current table structure...")

    # Get column information (we'll need to reconstruct this for DuckDB)
    table_info_query <- sprintf("SELECT column_name, data_type
                                FROM information_schema.columns
                                WHERE table_name = '%s'", table_name)

    # For DuckDB, we might need to use PRAGMA or recreate based on known structure
    # Using a more direct approach

    # Step 4: Create new table with product_id
    if (verbose) message("\nStep 3: Creating new table structure with product_id...")

    new_table_name <- paste0(table_name, "_new")

    # Build column list, replacing item_id with product_id
    if (has_product_id) {
      # If product_id exists as a virtual column, we need to handle it differently
      # Get all columns except product_id (which is virtual)
      create_query <- sprintf("
        CREATE TABLE %s AS
        SELECT
          %s
        FROM %s
      ",
      new_table_name,
      paste(sapply(columns[columns != "product_id"], function(col) {
        if (col == "item_id") {
          sprintf("%s AS product_id", col)
        } else {
          col
        }
      }), collapse = ", "),
      table_name)
    } else {
      # Simple rename of item_id to product_id
      create_query <- sprintf("
        CREATE TABLE %s AS
        SELECT
          %s
        FROM %s
      ",
      new_table_name,
      paste(sapply(columns, function(col) {
        if (col == "item_id") {
          sprintf("%s AS product_id", col)
        } else {
          col
        }
      }), collapse = ", "),
      table_name)
    }

    dbExecute(con, create_query)

    # Verify new table
    new_count <- dbGetQuery(con, sprintf("SELECT COUNT(*) as n FROM %s", new_table_name))$n
    if (new_count != original_count) {
      stop(sprintf("New table verification failed: %d rows in new table vs %d in original",
                   new_count, original_count))
    }

    if (verbose) message(sprintf("  ✓ New table created with %d rows", new_count))
    migration_result$steps_completed <- c(migration_result$steps_completed, "New table created")

    # Step 5: Drop original and rename new
    if (verbose) message("\nStep 4: Replacing original table...")

    dbExecute(con, sprintf("DROP TABLE %s", table_name))
    dbExecute(con, sprintf("ALTER TABLE %s RENAME TO %s", new_table_name, table_name))

    if (verbose) message("  ✓ Table replaced successfully")
    migration_result$steps_completed <- c(migration_result$steps_completed, "Table replaced")

    # Step 6: Recreate indexes
    if (verbose) message("\nStep 5: Recreating indexes...")

    # Create indexes on product_id
    index_queries <- c(
      sprintf("CREATE INDEX IF NOT EXISTS idx_%s_product ON %s(product_id)", table_name, table_name),
      sprintf("CREATE INDEX IF NOT EXISTS idx_%s_platform_product ON %s(platform_id, product_id)",
              table_name, table_name)
    )

    for (idx_query in index_queries) {
      tryCatch({
        dbExecute(con, idx_query)
        if (verbose) message(sprintf("  ✓ Index created: %s", sub(".*idx_", "idx_", idx_query)))
      }, error = function(e) {
        if (verbose) message(sprintf("  ⚠ Index creation skipped (may already exist): %s", e$message))
      })
    }

    migration_result$steps_completed <- c(migration_result$steps_completed, "Indexes created")

    # Step 7: Verify final structure
    if (verbose) message("\nStep 6: Verifying final structure...")

    final_columns <- dbListFields(con, table_name)
    final_has_product_id <- "product_id" %in% final_columns
    final_has_item_id <- "item_id" %in% final_columns

    if (!final_has_product_id) {
      stop("Migration failed: product_id column not found in final table")
    }

    if (final_has_item_id) {
      warning("Note: item_id column still exists in the table")
    }

    # Show sample of migrated data
    if (verbose) {
      final_sample <- dbGetQuery(con, sprintf("SELECT * FROM %s LIMIT 3", table_name))
      message("\nMigrated data sample (first 3 rows):")
      print(final_sample)
    }

    migration_result$success <- TRUE

    if (verbose) {
      message("\n=== Migration Completed Successfully ===")
      message(sprintf("✓ Table '%s' now uses 'product_id' instead of 'item_id'", table_name))
      message(sprintf("✓ Backup preserved in '%s'", backup_table))
      message("\nTo rollback if needed:")
      message(sprintf("  DROP TABLE %s;", table_name))
      message(sprintf("  ALTER TABLE %s RENAME TO %s;", backup_table, table_name))
    }

  }, error = function(e) {
    migration_result$errors <- c(migration_result$errors, e$message)

    if (verbose) {
      message("\n❌ Migration Failed!")
      message(sprintf("Error: %s", e$message))

      if (length(migration_result$steps_completed) > 0) {
        message("\nCompleted steps before failure:")
        for (step in migration_result$steps_completed) {
          message(sprintf("  - %s", step))
        }
      }

      message(sprintf("\nBackup table '%s' has been preserved", backup_table))
      message("Manual intervention may be required to complete or rollback the migration")
    }

    stop(e$message)
  })

  return(migration_result)
}

#' Rollback Migration
#'
#' @param con Database connection
#' @param table_name Name of the table to rollback
#' @param backup_table Name of the backup table to restore from
#' @param verbose If TRUE, shows detailed messages
#'
#' @return TRUE if successful
rollback_migration <- function(con, table_name, backup_table, verbose = TRUE) {
  if (verbose) message(sprintf("\nRolling back migration for '%s'...", table_name))

  tryCatch({
    # Drop current table if it exists
    if (dbExistsTable(con, table_name)) {
      dbExecute(con, sprintf("DROP TABLE %s", table_name))
      if (verbose) message(sprintf("  ✓ Dropped current table '%s'", table_name))
    }

    # Restore from backup
    dbExecute(con, sprintf("ALTER TABLE %s RENAME TO %s", backup_table, table_name))
    if (verbose) message(sprintf("  ✓ Restored table from backup '%s'", backup_table))

    if (verbose) message("✓ Rollback completed successfully")
    return(TRUE)

  }, error = function(e) {
    if (verbose) message(sprintf("❌ Rollback failed: %s", e$message))
    return(FALSE)
  })
}

# Main execution when run directly as a script (not sourced)
if (!interactive() && sys.nframe() == 0) {
  # Parse command line arguments
  args <- commandArgs(trailingOnly = TRUE)

  if (length(args) == 0) {
    cat("Usage: Rscript migrate_item_id_to_product_id.R <database_path> [--dry-run] [--rollback <backup_table>]\n")
    cat("Example: Rscript migrate_item_id_to_product_id.R app_data.duckdb\n")
    cat("Example: Rscript migrate_item_id_to_product_id.R app_data.duckdb --dry-run\n")
    cat("Example: Rscript migrate_item_id_to_product_id.R app_data.duckdb --rollback df_position_backup_20240128_143022\n")
    quit(status = 1)
  }

  db_path <- args[1]
  dry_run <- "--dry-run" %in% args
  rollback_table <- NULL

  # Check for rollback
  if ("--rollback" %in% args) {
    rollback_idx <- which(args == "--rollback")
    if (rollback_idx < length(args)) {
      rollback_table <- args[rollback_idx + 1]
    } else {
      stop("--rollback requires a backup table name")
    }
  }

  # Connect to database
  con <- dbConnect(duckdb::duckdb(), db_path)

  tryCatch({
    if (!is.null(rollback_table)) {
      # Perform rollback
      result <- rollback_migration(con, "df_position", rollback_table)
      quit(status = ifelse(result, 0, 1))
    } else {
      # Perform migration
      result <- migrate_item_id_to_product_id(con, dry_run = dry_run)
      quit(status = ifelse(result$success, 0, 1))
    }
  }, finally = {
    dbDisconnect(con)
  })
}