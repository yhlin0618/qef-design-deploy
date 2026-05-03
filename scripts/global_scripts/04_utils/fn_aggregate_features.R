#!/usr/bin/env Rscript
#' Aggregate Features Across Products
#'
#' Aggregates product features for market-level analysis in the DRV layer.
#' Implements MP109: DRV Derivation Layer principles.
#'
#' This function takes product-level features and aggregates them by specified
#' grouping dimensions (e.g., product_line, country) to create market-level
#' summary statistics. This is a core DRV (Derived) operation that creates
#' composite business entities from ETL outputs.
#'
#' @param data Data frame with product features
#' @param group_cols Grouping columns (e.g., c("product_line", "country"))
#' @param feature_cols Feature columns to aggregate
#' @param agg_functions Aggregation functions (default: c("mean", "median", "sd", "min", "max"))
#' @param include_prevalence Calculate prevalence for binary/dummy features (default: TRUE)
#' @param add_metadata Add metadata columns (aggregation_level, timestamp) per MP102 (default: TRUE)
#'
#' @return Data frame with aggregated features
#'
#' @examples
#' # Aggregate product features by product_line and country
#' aggregated <- fn_aggregate_features(
#'   data = product_data,
#'   group_cols = c("product_line", "country"),
#'   feature_cols = c("price_usd", "rating", "review_count"),
#'   agg_functions = c("mean", "median", "sd")
#' )
#'
#' # Aggregate with prevalence calculation for dummy variables
#' aggregated_with_prevalence <- fn_aggregate_features(
#'   data = product_data,
#'   group_cols = c("product_line"),
#'   feature_cols = c("price_usd", "has_discount", "has_warranty"),
#'   include_prevalence = TRUE
#' )
#'
#' @export
fn_aggregate_features <- function(data,
                                   group_cols = c("product_line"),
                                   feature_cols,
                                   agg_functions = c("mean", "median", "sd", "min", "max"),
                                   include_prevalence = TRUE,
                                   add_metadata = TRUE) {

  # Load required packages
  require(dplyr, quietly = TRUE)
  require(tidyr, quietly = TRUE)
  require(rlang, quietly = TRUE)

  # === VALIDATION ===

  message("═══════════════════════════════════════════════════════════")
  message("fn_aggregate_features: Starting feature aggregation")
  message("═══════════════════════════════════════════════════════════")

  # Validate inputs
  if (!is.data.frame(data)) {
    stop("Input 'data' must be a data frame")
  }

  if (nrow(data) == 0) {
    stop("Input data is empty (0 rows)")
  }

  if (length(feature_cols) == 0) {
    stop("Must specify at least one feature column in 'feature_cols'")
  }

  # Check if group_cols exist in data
  if (!all(group_cols %in% names(data))) {
    missing_cols <- setdiff(group_cols, names(data))
    stop(sprintf("Group columns not found in data: %s",
                 paste(missing_cols, collapse = ", ")))
  }

  # Check if feature_cols exist in data
  if (!all(feature_cols %in% names(data))) {
    missing_cols <- setdiff(feature_cols, names(data))
    stop(sprintf("Feature columns not found in data: %s",
                 paste(missing_cols, collapse = ", ")))
  }

  # Validate agg_functions
  valid_functions <- c("mean", "median", "sd", "min", "max", "sum", "n", "first", "last")
  invalid_functions <- setdiff(agg_functions, valid_functions)
  if (length(invalid_functions) > 0) {
    stop(sprintf("Invalid aggregation functions: %s\nValid options: %s",
                 paste(invalid_functions, collapse = ", "),
                 paste(valid_functions, collapse = ", ")))
  }

  message(sprintf("[Step 1/5] Validation complete"))
  message(sprintf("  - Input rows: %d", nrow(data)))
  message(sprintf("  - Group columns: %s", paste(group_cols, collapse = ", ")))
  message(sprintf("  - Feature columns: %d (%s)",
                  length(feature_cols),
                  paste(head(feature_cols, 3), collapse = ", ")))
  message(sprintf("  - Aggregation functions: %s", paste(agg_functions, collapse = ", ")))

  # === STEP 1: Identify Feature Types ===

  message("\n[Step 2/5] Identifying feature types...")

  # Determine which features are binary/dummy (0/1) for prevalence calculation
  binary_features <- c()
  continuous_features <- c()

  for (feat in feature_cols) {
    unique_vals <- unique(data[[feat]][!is.na(data[[feat]])])

    # Check if feature is binary (only 0 and 1)
    if (all(unique_vals %in% c(0, 1))) {
      binary_features <- c(binary_features, feat)
    } else {
      continuous_features <- c(continuous_features, feat)
    }
  }

  message(sprintf("  - Binary features: %d", length(binary_features)))
  message(sprintf("  - Continuous features: %d", length(continuous_features)))

  # === STEP 2: Build Aggregation Expressions for Continuous Features ===

  message("\n[Step 3/5] Building aggregation expressions...")

  agg_exprs <- list()

  for (feat in continuous_features) {
    for (func in agg_functions) {

      # Generate column name: feature_function (e.g., price_usd_mean)
      col_name <- sprintf("%s_%s", feat, func)

      # Build expression based on function type
      if (func == "mean") {
        agg_exprs[[col_name]] <- expr(mean(!!sym(feat), na.rm = TRUE))

      } else if (func == "median") {
        agg_exprs[[col_name]] <- expr(median(!!sym(feat), na.rm = TRUE))

      } else if (func == "sd") {
        agg_exprs[[col_name]] <- expr(sd(!!sym(feat), na.rm = TRUE))

      } else if (func == "min") {
        agg_exprs[[col_name]] <- expr(min(!!sym(feat), na.rm = TRUE))

      } else if (func == "max") {
        agg_exprs[[col_name]] <- expr(max(!!sym(feat), na.rm = TRUE))

      } else if (func == "sum") {
        agg_exprs[[col_name]] <- expr(sum(!!sym(feat), na.rm = TRUE))

      } else if (func == "n") {
        # Count non-NA values
        col_name <- sprintf("%s_count", feat)
        agg_exprs[[col_name]] <- expr(sum(!is.na(!!sym(feat))))

      } else if (func == "first") {
        agg_exprs[[col_name]] <- expr(first(!!sym(feat)))

      } else if (func == "last") {
        agg_exprs[[col_name]] <- expr(last(!!sym(feat)))
      }
    }
  }

  message(sprintf("  - Aggregation expressions created: %d", length(agg_exprs)))

  # === STEP 3: Perform Aggregation ===

  message("\n[Step 4/5] Performing aggregation...")

  # Group and summarize
  aggregated_data <- data %>%
    group_by(across(all_of(group_cols))) %>%
    summarise(
      !!!agg_exprs,
      n_products = n(),  # Count of products in each group
      .groups = "drop"
    )

  message(sprintf("  - Aggregated groups: %d", nrow(aggregated_data)))

  # === STEP 4: Calculate Prevalence for Binary Features ===

  if (include_prevalence && length(binary_features) > 0) {
    message("\n[Step 5/5] Calculating feature prevalence...")

    prevalence_data <- data %>%
      group_by(across(all_of(group_cols))) %>%
      summarise(
        across(
          all_of(binary_features),
          ~mean(., na.rm = TRUE),
          .names = "{.col}_prevalence"
        ),
        .groups = "drop"
      )

    # Join prevalence with aggregated data
    aggregated_data <- aggregated_data %>%
      left_join(prevalence_data, by = group_cols)

    message(sprintf("  - Prevalence calculated for %d binary features", length(binary_features)))
  }

  # === STEP 5: Add Metadata (MP102 Completeness) ===

  if (add_metadata) {
    message("\n[Metadata] Adding MP102-compliant metadata columns...")

    aggregated_data$aggregation_level <- paste(group_cols, collapse = "_")
    aggregated_data$aggregation_timestamp <- Sys.time()
    aggregated_data$aggregation_method <- paste(agg_functions, collapse = ",")

    message("  ✓ Metadata columns added")
  }

  # === FINAL SUMMARY ===

  message("\n═══════════════════════════════════════════════════════════")
  message("✅ Feature Aggregation Complete")
  message("═══════════════════════════════════════════════════════════")
  message(sprintf("  Input products:      %d", nrow(data)))
  message(sprintf("  Output groups:       %d", nrow(aggregated_data)))
  message(sprintf("  Features aggregated: %d", length(feature_cols)))
  message(sprintf("    - Continuous:      %d", length(continuous_features)))
  message(sprintf("    - Binary:          %d", length(binary_features)))
  message(sprintf("  Output columns:      %d", ncol(aggregated_data)))
  message(sprintf("  Grouping:            %s", paste(group_cols, collapse = " + ")))
  message("═══════════════════════════════════════════════════════════\n")

  return(aggregated_data)
}


#' Aggregate Features with Custom Expressions
#'
#' Advanced version allowing custom aggregation expressions for complex calculations.
#'
#' @param data Data frame with features
#' @param group_cols Grouping columns
#' @param custom_exprs Named list of custom expressions (e.g., list(avg_price = expr(mean(price))))
#' @param add_metadata Add metadata columns (default: TRUE)
#'
#' @return Data frame with aggregated features
#'
#' @examples
#' # Custom aggregation with complex expressions
#' custom_agg <- fn_aggregate_features_custom(
#'   data = product_data,
#'   group_cols = c("product_line"),
#'   custom_exprs = list(
#'     weighted_rating = expr(sum(rating * review_count) / sum(review_count)),
#'     price_range = expr(max(price_usd) - min(price_usd)),
#'     high_rated_pct = expr(mean(rating >= 4.0))
#'   )
#' )
#'
#' @export
fn_aggregate_features_custom <- function(data,
                                         group_cols = c("product_line"),
                                         custom_exprs,
                                         add_metadata = TRUE) {

  require(dplyr, quietly = TRUE)
  require(rlang, quietly = TRUE)

  # Validate inputs
  if (!is.list(custom_exprs) || is.null(names(custom_exprs))) {
    stop("custom_exprs must be a named list of expressions")
  }

  message("═══════════════════════════════════════════════════════════")
  message("fn_aggregate_features_custom: Custom aggregation")
  message("═══════════════════════════════════════════════════════════")
  message(sprintf("  Custom expressions: %d", length(custom_exprs)))

  # Perform aggregation with custom expressions
  aggregated_data <- data %>%
    group_by(across(all_of(group_cols))) %>%
    summarise(
      !!!custom_exprs,
      n_products = n(),
      .groups = "drop"
    )

  # Add metadata if requested
  if (add_metadata) {
    aggregated_data$aggregation_level <- paste(group_cols, collapse = "_")
    aggregated_data$aggregation_timestamp <- Sys.time()
    aggregated_data$aggregation_method <- "custom"
  }

  message(sprintf("✅ Custom aggregation complete: %d groups", nrow(aggregated_data)))
  message("═══════════════════════════════════════════════════════════\n")

  return(aggregated_data)
}
