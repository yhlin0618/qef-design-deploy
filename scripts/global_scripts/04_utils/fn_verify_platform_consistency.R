################################################################################
# Platform Consistency Verification Function
# Principle: DM_R047 - Multi-Platform Data Synchronization
################################################################################
#
# PURPOSE:
#   Verify that all production platforms (CBZ, EBY) have consistent data
#   structures, metadata columns, and can support UI platform switching.
#
# USAGE:
#   source("scripts/global_scripts/04_utils/fn_verify_platform_consistency.R")
#
#   validation <- fn_verify_platform_consistency(
#     con = con,
#     table_base_name = "poisson_analysis",
#     platforms = c("cbz", "eby")
#   )
#
#   if (!validation$valid) {
#     stop(validation$error)
#   }
#
# PRINCIPLE COMPLIANCE:
#   DM_R047: Multi-Platform Data Synchronization
#   DM_R046: Variable Display Name Metadata
#   MP135 v2.0: Analytics Temporal Classification (Type B)
#
################################################################################

#' Verify Multi-Platform Schema Consistency (DM_R047)
#'
#' Checks that all production platforms have identical table schemas,
#' required metadata columns, and compatible data structures for
#' seamless UI platform switching.
#'
#' @param con DBI connection to app_data.duckdb
#' @param table_base_name Base name of tables to verify (default: "poisson_analysis")
#'   Full table names will be: df_{platform}_{table_base_name}_all
#' @param platforms Character vector of platform_ids to verify (default: c("cbz", "eby"))
#' @param required_metadata Character vector of required metadata column names
#'   Default includes DM_R046 display names + MP135 Type B metadata
#' @param verbose Logical, print detailed verification output (default: TRUE)
#'
#' @return List with validation results:
#'   \itemize{
#'     \item valid: Logical, TRUE if all checks pass
#'     \item platforms: Character vector of verified platforms
#'     \item column_count: Integer, number of columns in schema
#'     \item schema: Character vector of column names
#'     \item error: Character, error message if valid=FALSE (otherwise NULL)
#'     \item mismatches: List of schema mismatches if valid=FALSE (otherwise NULL)
#'     \item missing_metadata: List of missing metadata if valid=FALSE (otherwise NULL)
#'   }
#'
#' @examples
#' \dontrun{
#' library(DBI)
#' library(duckdb)
#'
#' con <- dbConnect(duckdb::duckdb(), "data/app_data/app_data.duckdb")
#'
#' # Verify Poisson analysis tables
#' validation <- fn_verify_platform_consistency(
#'   con = con,
#'   table_base_name = "poisson_analysis"
#' )
#'
#' if (!validation$valid) {
#'   stop("Platform consistency check failed: ", validation$error)
#' }
#'
#' # Verify with custom metadata requirements
#' validation <- fn_verify_platform_consistency(
#'   con = con,
#'   table_base_name = "customer_segmentation",
#'   required_metadata = c("segment_name", "segment_description")
#' )
#'
#' dbDisconnect(con, shutdown = TRUE)
#' }
#'
#' @export
fn_verify_platform_consistency <- function(
  con,
  table_base_name = "poisson_analysis",
  platforms = c("cbz", "eby"),
  required_metadata = c(
    # DM_R046: Variable Display Name Metadata
    "display_name",
    "display_name_en",
    "display_name_zh",
    "display_category",
    "display_description",
    # MP135 v2.0: Type B Analytics Metadata
    "computed_at",
    "data_version"
  ),
  verbose = TRUE
) {

  # Input validation
  if (!inherits(con, "DBIConnection")) {
    stop("Invalid database connection object")
  }

  if (length(platforms) < 2) {
    stop("At least 2 platforms required for consistency verification")
  }

  # Print header
  if (verbose) {
    cat("═══════════════════════════════════════════════════════════════════\n")
    cat("DM_R047: Multi-Platform Consistency Verification\n")
    cat("═══════════════════════════════════════════════════════════════════\n\n")
    cat(sprintf("Table base name: %s\n", table_base_name))
    cat(sprintf("Platforms to verify: %s\n", paste(toupper(platforms), collapse=", ")))
    cat(sprintf("Required metadata columns: %d\n", length(required_metadata)))
    cat("\n")
  }

  # -------------------------------------------------------------------------
  # Step 1: Get schema for each platform
  # -------------------------------------------------------------------------

  platform_schemas <- list()
  missing_tables <- c()

  for (platform in platforms) {
    table_name <- sprintf("df_%s_%s_all", platform, table_base_name)

    if (verbose) {
      cat(sprintf("Checking table: %s... ", table_name))
    }

    if (!DBI::dbExistsTable(con, table_name)) {
      if (verbose) cat("❌ NOT FOUND\n")
      missing_tables <- c(missing_tables, table_name)
    } else {
      platform_schemas[[platform]] <- DBI::dbListFields(con, table_name)
      if (verbose) {
        cat(sprintf("✅ Found (%d columns)\n", length(platform_schemas[[platform]])))
      }
    }
  }

  # Check for missing tables
  if (length(missing_tables) > 0) {
    if (verbose) {
      cat("\n❌ VERIFICATION FAILED: Missing Tables\n\n")
      cat("Tables not found:\n")
      for (table in missing_tables) {
        cat(sprintf("  - %s\n", table))
      }
      cat("\n")
    }

    return(list(
      valid = FALSE,
      error = sprintf("Missing tables: %s", paste(missing_tables, collapse=", ")),
      platforms = platforms,
      column_count = NA,
      schema = NULL,
      mismatches = NULL,
      missing_metadata = NULL
    ))
  }

  if (verbose) cat("\n")

  # -------------------------------------------------------------------------
  # Step 2: Check schema consistency
  # -------------------------------------------------------------------------

  if (verbose) {
    cat("Checking schema consistency...\n")
  }

  reference_platform <- platforms[1]
  reference_schema <- platform_schemas[[reference_platform]]
  mismatches <- list()

  for (i in 2:length(platforms)) {
    platform <- platforms[i]
    current_schema <- platform_schemas[[platform]]

    # Check if schemas are identical
    if (!setequal(reference_schema, current_schema)) {

      # Find differences
      missing_in_current <- setdiff(reference_schema, current_schema)
      extra_in_current <- setdiff(current_schema, reference_schema)

      mismatches[[platform]] <- list(
        reference = reference_platform,
        missing = missing_in_current,
        extra = extra_in_current
      )
    }
  }

  # Report schema mismatches
  if (length(mismatches) > 0) {

    if (verbose) {
      cat("\n❌ SCHEMA MISMATCH DETECTED\n\n")

      for (platform in names(mismatches)) {
        cat(sprintf("Platform: %s (compared to %s)\n",
                    toupper(platform),
                    toupper(mismatches[[platform]]$reference)))

        if (length(mismatches[[platform]]$missing) > 0) {
          cat("  Missing columns:\n")
          for (col in mismatches[[platform]]$missing) {
            cat(sprintf("    - %s\n", col))
          }
        }

        if (length(mismatches[[platform]]$extra) > 0) {
          cat("  Extra columns:\n")
          for (col in mismatches[[platform]]$extra) {
            cat(sprintf("    + %s\n", col))
          }
        }
        cat("\n")
      }
    }

    return(list(
      valid = FALSE,
      error = "Schema mismatch between platforms",
      platforms = platforms,
      column_count = length(reference_schema),
      schema = reference_schema,
      mismatches = mismatches,
      missing_metadata = NULL
    ))
  }

  if (verbose) {
    cat(sprintf("  ✅ All platforms have identical schemas (%d columns)\n\n", length(reference_schema)))
  }

  # -------------------------------------------------------------------------
  # Step 3: Check required metadata columns
  # -------------------------------------------------------------------------

  if (verbose) {
    cat("Checking required metadata columns...\n")
  }

  missing_metadata <- list()

  for (platform in platforms) {
    schema <- platform_schemas[[platform]]
    missing <- setdiff(required_metadata, schema)

    if (length(missing) > 0) {
      missing_metadata[[platform]] <- missing
    }
  }

  # Report missing metadata
  if (length(missing_metadata) > 0) {

    if (verbose) {
      cat("\n❌ MISSING REQUIRED METADATA\n\n")

      for (platform in names(missing_metadata)) {
        cat(sprintf("Platform: %s\n", toupper(platform)))
        cat("  Missing metadata columns:\n")
        for (col in missing_metadata[[platform]]) {
          cat(sprintf("    - %s\n", col))
        }
        cat("\n")
      }
    }

    return(list(
      valid = FALSE,
      error = "Missing required metadata columns",
      platforms = platforms,
      column_count = length(reference_schema),
      schema = reference_schema,
      mismatches = NULL,
      missing_metadata = missing_metadata
    ))
  }

  if (verbose) {
    cat(sprintf("  ✅ All required metadata columns present (%d/%d)\n\n",
                length(required_metadata), length(required_metadata)))
  }

  # -------------------------------------------------------------------------
  # Step 4: Check data volume (informational)
  # -------------------------------------------------------------------------

  if (verbose) {
    cat("Data volume by platform:\n")

    for (platform in platforms) {
      table_name <- sprintf("df_%s_%s_all", platform, table_base_name)
      row_count <- DBI::dbGetQuery(
        con,
        sprintf("SELECT COUNT(*) as n FROM %s", table_name)
      )$n

      cat(sprintf("  %s: %s rows\n",
                  toupper(platform),
                  format(row_count, big.mark=",")))
    }
    cat("\n")
  }

  # -------------------------------------------------------------------------
  # Success - All checks passed
  # -------------------------------------------------------------------------

  if (verbose) {
    cat("═══════════════════════════════════════════════════════════════════\n")
    cat("✅ VERIFICATION PASSED - All Platforms Consistent\n")
    cat("═══════════════════════════════════════════════════════════════════\n\n")

    cat("Summary:\n")
    cat(sprintf("  • Platforms verified: %s\n", paste(toupper(platforms), collapse=", ")))
    cat(sprintf("  • Schema columns: %d (identical across all platforms)\n", length(reference_schema)))
    cat(sprintf("  • Metadata columns: %d/%d present in all platforms\n",
                length(required_metadata), length(required_metadata)))
    cat(sprintf("  • UI platform switching: ✅ Ready\n"))
    cat("\n")

    cat("Schema columns:\n")
    for (i in seq_along(reference_schema)) {
      cat(sprintf("  [%2d] %s\n", i, reference_schema[i]))
    }
    cat("\n")

    cat("═══════════════════════════════════════════════════════════════════\n\n")
  }

  return(list(
    valid = TRUE,
    platforms = platforms,
    column_count = length(reference_schema),
    schema = reference_schema,
    error = NULL,
    mismatches = NULL,
    missing_metadata = NULL
  ))
}


################################################################################
# Validation Function for Display Name Enrichment (DM_R046 helper)
################################################################################

#' Validate Display Name Enrichment (DM_R046)
#'
#' Check if a data frame has been properly enriched with display name metadata.
#'
#' @param df Data frame to validate
#' @param required_columns Character vector of required display name columns
#'   Default: c("display_name", "display_name_en", "display_name_zh",
#'              "display_category", "display_description")
#'
#' @return List with validation results:
#'   \itemize{
#'     \item valid: Logical, TRUE if all required columns present
#'     \item error: Character, error message if valid=FALSE
#'     \item summary: List with category counts and coverage statistics
#'   }
#'
#' @export
fn_validate_display_name_enrichment <- function(
  df,
  required_columns = c("display_name", "display_name_en", "display_name_zh",
                       "display_category", "display_description")
) {

  # Check if data frame
  if (!is.data.frame(df)) {
    return(list(
      valid = FALSE,
      error = "Input is not a data frame",
      summary = NULL
    ))
  }

  # Check for required columns
  missing_cols <- setdiff(required_columns, names(df))

  if (length(missing_cols) > 0) {
    return(list(
      valid = FALSE,
      error = sprintf("Missing columns: %s", paste(missing_cols, collapse=", ")),
      summary = NULL
    ))
  }

  # Calculate coverage statistics
  total_rows <- nrow(df)

  coverage <- list()
  for (col in required_columns) {
    non_na <- sum(!is.na(df[[col]]))
    coverage[[col]] <- list(
      count = non_na,
      percentage = 100 * non_na / total_rows
    )
  }

  # Category distribution (if display_category exists)
  category_summary <- NULL
  if ("display_category" %in% names(df)) {
    category_summary <- table(df$display_category, useNA = "ifany")
  }

  return(list(
    valid = TRUE,
    error = NULL,
    summary = list(
      total_rows = total_rows,
      coverage = coverage,
      categories = as.list(category_summary)
    )
  ))
}
