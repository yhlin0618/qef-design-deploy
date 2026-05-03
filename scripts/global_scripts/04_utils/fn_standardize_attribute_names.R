#!/usr/bin/env Rscript
# ==============================================================================
# Attribute Name Standardization Utility Function
# ==============================================================================
#
# Function: fn_standardize_attribute_names
# Purpose: Standardize variable/attribute names to consistent format
# Location: scripts/global_scripts/04_utils/fn_standardize_attribute_names.R
#
# Principle Compliance:
# - MP102: Standardization Principle (consistent naming across all data)
# - R078: Column Naming for Operations (snake_case with clear prefixes)
# - MP108: ETL Stage Separation (designed for 1ST stage usage)
#
# Author: principle-product-manager
# Date: 2025-11-12
# Version: 1.0
# ==============================================================================

#' Standardize Attribute Names
#'
#' Implements MP102 standardization for variable/attribute names
#' Converts to snake_case and applies consistent naming conventions
#'
#' @param data Data frame with attributes to standardize
#' @param preserve_original Logical, preserve original names in metadata (default: TRUE)
#'
#' @return Data frame with standardized column names and optional original_names attribute
#'
#' @examples
#' # Basic usage
#' products <- data.frame(
#'   `Product ID` = c("A001", "A002"),
#'   `Product Brand` = c("Brand A", "Brand B"),
#'   `Product-Title` = c("Title 1", "Title 2"),
#'   Rating = c(4.5, 4.8)
#' )
#' result <- fn_standardize_attribute_names(products)
#' # Result has: product_id, product_brand, product_title, rating
#'
#' @details
#' Standardization Rules (MP102):
#' 1. Convert to snake_case (lowercase with underscores)
#' 2. Replace spaces and hyphens with underscores
#' 3. Remove special characters (except underscores)
#' 4. Standardize common abbreviations (id, num, qty, amt, etc.)
#' 5. Remove redundant words (data, value, field, etc.)
#'
#' Column Name Patterns (R078):
#' - product_*: Product attributes (product_id, product_brand, product_title)
#' - customer_*: Customer attributes (customer_id, customer_name)
#' - order_*: Order attributes (order_id, order_date, order_total)
#' - price_*: Price attributes (price_usd, price_original, price_discount)
#' - rating_*: Rating attributes (rating_average, rating_count)
#'
fn_standardize_attribute_names <- function(data, preserve_original = TRUE) {

  # ==============================================================================
  # Input Validation
  # ==============================================================================

  if (!is.data.frame(data)) {
    stop("MP102 VIOLATION: 'data' must be a data frame")
  }

  # ==============================================================================
  # Preserve Original Names (if requested)
  # ==============================================================================

  original_names <- names(data)

  if (preserve_original) {
    attr(data, "original_names") <- original_names
  }

  # ==============================================================================
  # Standardization Process
  # ==============================================================================

  new_names <- names(data)

  # Step 1: Convert to lowercase
  new_names <- tolower(new_names)

  # Step 2: Replace spaces and hyphens with underscores
  new_names <- gsub("[ -]+", "_", new_names)

  # Step 3: Remove special characters (keep only alphanumeric and underscores)
  new_names <- gsub("[^a-z0-9_]", "", new_names)

  # Step 4: Remove leading/trailing underscores
  new_names <- gsub("^_+|_+$", "", new_names)

  # Step 5: Collapse multiple consecutive underscores
  new_names <- gsub("_+", "_", new_names)

  # Step 6: Standardize common patterns
  new_names <- gsub("^id$", "id", new_names)  # Keep id as-is
  new_names <- gsub("_id$", "_id", new_names)  # Keep _id suffix
  new_names <- gsub("_ids$", "_ids", new_names)  # Keep _ids suffix

  # Standardize number/count patterns
  new_names <- gsub("_number$", "_num", new_names)
  new_names <- gsub("_quantity$", "_qty", new_names)
  new_names <- gsub("_amount$", "_amt", new_names)

  # Standardize date/time patterns
  new_names <- gsub("_date$", "_date", new_names)
  new_names <- gsub("_datetime$", "_datetime", new_names)
  new_names <- gsub("_timestamp$", "_timestamp", new_names)

  # Remove redundant words
  new_names <- gsub("_value$", "", new_names)
  new_names <- gsub("_data$", "", new_names)
  new_names <- gsub("_field$", "", new_names)

  # Standardize rating/review patterns
  new_names <- gsub("^rating$", "rating", new_names)
  new_names <- gsub("^review_count$", "review_count", new_names)
  new_names <- gsub("^reviews$", "review_count", new_names)

  # ==============================================================================
  # Handle Duplicate Names
  # ==============================================================================

  # If duplicates exist, add numeric suffixes
  if (any(duplicated(new_names))) {
    duplicated_names <- unique(new_names[duplicated(new_names)])

    for (dup_name in duplicated_names) {
      indices <- which(new_names == dup_name)
      for (i in seq_along(indices)) {
        if (i > 1) {
          new_names[indices[i]] <- paste0(dup_name, "_", i)
        }
      }
    }

    warning(sprintf(
      "MP102 WARNING: Duplicate names detected and resolved: %s",
      paste(duplicated_names, collapse = ", ")
    ))
  }

  # ==============================================================================
  # Apply New Names
  # ==============================================================================

  names(data) <- new_names

  # ==============================================================================
  # Report Changes
  # ==============================================================================

  changed_count <- sum(original_names != new_names)

  if (changed_count > 0) {
    message(sprintf("✓ MP102: Standardized %d of %d column names",
                   changed_count, length(original_names)))

    # Show first few changes as examples
    changes <- data.frame(
      original = original_names[original_names != new_names],
      standardized = new_names[original_names != new_names]
    )

    if (nrow(changes) > 0) {
      n_show <- min(5, nrow(changes))
      message("  Example changes:")
      for (i in 1:n_show) {
        message(sprintf("    %s → %s", changes$original[i], changes$standardized[i]))
      }

      if (nrow(changes) > n_show) {
        message(sprintf("    ... and %d more changes", nrow(changes) - n_show))
      }
    }
  } else {
    message("✓ MP102: All column names already standardized")
  }

  return(data)
}

# ==============================================================================
# Enhanced Standardization with Domain Knowledge
# ==============================================================================

#' Standardize Attribute Names with Domain-Specific Rules
#'
#' Extended version that applies domain-specific naming conventions
#'
#' @param data Data frame with attributes to standardize
#' @param domain Domain context: "product", "customer", "order", "general" (default: "general")
#' @param preserve_original Logical, preserve original names (default: TRUE)
#'
#' @return Data frame with standardized column names
#'
fn_standardize_attribute_names_domain <- function(data,
                                                  domain = "general",
                                                  preserve_original = TRUE) {

  # First apply general standardization
  data <- fn_standardize_attribute_names(data, preserve_original)

  # Then apply domain-specific rules
  new_names <- names(data)

  if (domain == "product") {
    # Product domain standardization
    new_names <- gsub("^brand$", "product_brand", new_names)
    new_names <- gsub("^title$", "product_title", new_names)
    new_names <- gsub("^name$", "product_name", new_names)
    new_names <- gsub("^category$", "product_category", new_names)
    new_names <- gsub("^sku$", "product_sku", new_names)
    new_names <- gsub("^asin$", "product_asin", new_names)

  } else if (domain == "customer") {
    # Customer domain standardization
    new_names <- gsub("^name$", "customer_name", new_names)
    new_names <- gsub("^email$", "customer_email", new_names)
    new_names <- gsub("^phone$", "customer_phone", new_names)

  } else if (domain == "order") {
    # Order domain standardization
    new_names <- gsub("^date$", "order_date", new_names)
    new_names <- gsub("^total$", "order_total", new_names)
    new_names <- gsub("^status$", "order_status", new_names)
  }

  names(data) <- new_names

  message(sprintf("✓ MP102: Applied domain-specific standardization (domain: %s)", domain))

  return(data)
}

# ==============================================================================
# Validation Function
# ==============================================================================

#' Validate Attribute Name Standardization
#'
#' Checks if attribute names follow standardization rules
#'
#' @param data Data frame to validate
#' @return TRUE if validation passes, FALSE otherwise
#'
validate_attribute_names <- function(data) {

  col_names <- names(data)

  # Check 1: All lowercase
  has_uppercase <- any(grepl("[A-Z]", col_names))
  if (has_uppercase) {
    invalid_names <- col_names[grepl("[A-Z]", col_names)]
    warning(sprintf(
      "MP102 WARNING: Column names contain uppercase: %s",
      paste(invalid_names, collapse = ", ")
    ))
    return(FALSE)
  }

  # Check 2: No spaces or special characters (except underscores)
  has_special <- any(grepl("[^a-z0-9_]", col_names))
  if (has_special) {
    invalid_names <- col_names[grepl("[^a-z0-9_]", col_names)]
    warning(sprintf(
      "MP102 WARNING: Column names contain special characters: %s",
      paste(invalid_names, collapse = ", ")
    ))
    return(FALSE)
  }

  # Check 3: No leading/trailing underscores
  has_edge_underscores <- any(grepl("^_|_$", col_names))
  if (has_edge_underscores) {
    invalid_names <- col_names[grepl("^_|_$", col_names)]
    warning(sprintf(
      "MP102 WARNING: Column names have leading/trailing underscores: %s",
      paste(invalid_names, collapse = ", ")
    ))
    return(FALSE)
  }

  # Check 4: No consecutive underscores
  has_double_underscores <- any(grepl("__", col_names))
  if (has_double_underscores) {
    invalid_names <- col_names[grepl("__", col_names)]
    warning(sprintf(
      "MP102 WARNING: Column names have consecutive underscores: %s",
      paste(invalid_names, collapse = ", ")
    ))
    return(FALSE)
  }

  message("✅ MP102 Validation: All attribute names are standardized")
  return(TRUE)
}

# ==============================================================================
# Example Usage (for testing)
# ==============================================================================

if (FALSE) {
  # Example 1: Basic standardization
  test_data <- data.frame(
    `Product ID` = c("A001", "A002"),
    `Product-Brand` = c("Brand A", "Brand B"),
    `Product Title` = c("Title 1", "Title 2"),
    Rating = c(4.5, 4.8),
    `Review Count` = c(100, 200),
    `Price (USD)` = c(29.99, 39.99)
  )

  result <- fn_standardize_attribute_names(test_data)
  print(names(result))

  # Validate result
  validate_attribute_names(result)

  # Example 2: Domain-specific standardization
  result2 <- fn_standardize_attribute_names_domain(test_data, domain = "product")
  print(names(result2))
}
