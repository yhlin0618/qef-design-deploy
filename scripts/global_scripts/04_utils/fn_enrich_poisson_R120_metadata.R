#' Enrich Poisson Analysis Table with R120 Range Metadata
#'
#' This function adds R120-compliant range metadata to Poisson regression tables,
#' correctly handling dummy-coded variables, continuous variables, and time features.
#'
#' @param poisson_data data.frame - Poisson analysis results table
#' @param source_data data.frame - Original time series or feature data (optional)
#' @return data.frame - Enriched table with R120 metadata columns
#'
#' @details
#' Variable Type Detection (pattern-based):
#' 1. Dummy variables (0/1):
#'    - month_1, month_2, ..., month_12
#'    - monday, tuesday, ..., sunday
#'    - Categorical levels: seller_*, location_*, brand_*, color_*
#'    - Binary flags: *_is_missing, can_use_diesel_x0, etc.
#'
#' 2. Continuous variables:
#'    - price, rating, customer_ratings
#'    - Numeric features without dummy patterns
#'
#' 3. Time features:
#'    - year, day, quarter, week (may be continuous or dummy-coded)
#'
#' @compliance R120 (Range Metadata Requirement), MP029 (No Fake Data)
#' @author principle-product-manager
#' @date 2025-11-13

fn_enrich_poisson_R120_metadata <- function(poisson_data, source_data = NULL) {
  
  # Pattern-based variable type detection
  detect_variable_type <- function(predictor_name) {
    
    # 1. Check for dummy-coded categorical levels (suffix pattern)
    dummy_patterns <- c(
      "^month_[0-9]+$",        # month_1, month_2, ...
      "^(monday|tuesday|wednesday|thursday|friday|saturday|sunday)$",  # weekdays
      "^seller_",               # seller_mambatek, seller_uk_mambatek
      "^location_",             # location_台中, location_uk
      "^nation_",               # nation_us, nation_uk
      "^brand\\.",              # brand.x, brand.y
      "^color_options_",        # color_options_藍色
      "^applicable_vehicles_",  # applicable_vehicles_*
      "^kit_components_",       # kit_components_*
      "^special_features_",     # special_features_*
      "^material_",             # material_鋁合金
      "^pressure_range_",       # pressure_range_high_x0_8
      "^spring_pressure_",      # spring_pressure_range_*
      "_NA$",                   # Categorical NA levels
      "_x[0-9]+$",              # Numeric levels: x0, x1
      "_is_missing$",           # Missing indicators
      "^can_use_diesel_",       # Binary flags
      "^spring_pre_installed_"  # Binary flags
    )
    
    for (pattern in dummy_patterns) {
      if (grepl(pattern, predictor_name, ignore.case = FALSE)) {
        return("dummy")
      }
    }
    
    # 2. Check for continuous numeric features
    continuous_patterns <- c(
      "^price",                 # price_us_dollar
      "^rating",                # rating, customer_ratings
      "^customer_ratings"       # customer_ratings
    )
    
    for (pattern in continuous_patterns) {
      if (grepl(pattern, predictor_name, ignore.case = FALSE)) {
        return("continuous")
      }
    }
    
    # 3. Check for time features (may be continuous or dummy)
    time_patterns <- c(
      "^year$",                 # year (could be continuous 2023-2025 or dummy)
      "^day$",                  # day (continuous 1-31)
      "^quarter$",              # quarter (continuous 1-4)
      "^week$"                  # week (continuous 1-53)
    )
    
    for (pattern in time_patterns) {
      if (grepl(pattern, predictor_name, ignore.case = FALSE)) {
        return("time_continuous")
      }
    }
    
    # 4. Check for factor columns (encoded as factor type)
    # These should be treated case-by-case
    factor_patterns <- c(
      "^eby_item_id$",
      "^cbz_item_id$",
      "^product_line_id",
      "^product_name$",
      "^source_table$",
      "^processing_version$"
    )
    
    for (pattern in factor_patterns) {
      if (grepl(pattern, predictor_name, ignore.case = FALSE)) {
        return("categorical_id")
      }
    }
    
    # 5. Default: Assume numeric continuous (conservative)
    return("numeric_unknown")
  }
  
  # Calculate ranges based on variable type
  calculate_range_metadata <- function(predictor_name, var_type, source_data) {
    
    if (var_type == "dummy") {
      # Dummy variables are always 0-1
      return(list(
        predictor_min = 0,
        predictor_max = 1,
        predictor_range = 1,
        track_multiplier = 100,  # Scale to 0-100 for display
        predictor_is_binary = TRUE,
        predictor_is_categorical = TRUE,
        r120_method = "dummy_pattern_detection"
      ))
    }
    
    if (var_type == "time_continuous") {
      # Time features: Use actual data if available
      if (!is.null(source_data) && predictor_name %in% names(source_data)) {
        values <- source_data[[predictor_name]]
        return(list(
          predictor_min = min(values, na.rm = TRUE),
          predictor_max = max(values, na.rm = TRUE),
          predictor_range = max(values, na.rm = TRUE) - min(values, na.rm = TRUE),
          track_multiplier = ifelse(
            max(values, na.rm = TRUE) - min(values, na.rm = TRUE) > 0,
            100 / (max(values, na.rm = TRUE) - min(values, na.rm = TRUE)),
            1
          ),
          predictor_is_binary = FALSE,
          predictor_is_categorical = FALSE,
          r120_method = "calculated_from_source_data"
        ))
      }
      
      # No defaults allowed without real data (MP029)
      return(list(
        predictor_min = NA, predictor_max = NA, predictor_range = NA,
        track_multiplier = NA, predictor_is_binary = FALSE,
        predictor_is_categorical = FALSE, r120_method = "no_source_data_available"
      ))
    }
    
    if (var_type == "continuous") {
      # Continuous variables: Use actual data if available
      if (!is.null(source_data) && predictor_name %in% names(source_data)) {
        values <- source_data[[predictor_name]]
        return(list(
          predictor_min = min(values, na.rm = TRUE),
          predictor_max = max(values, na.rm = TRUE),
          predictor_range = max(values, na.rm = TRUE) - min(values, na.rm = TRUE),
          track_multiplier = ifelse(
            max(values, na.rm = TRUE) - min(values, na.rm = TRUE) > 0,
            100 / (max(values, na.rm = TRUE) - min(values, na.rm = TRUE)),
            1
          ),
          predictor_is_binary = FALSE,
          predictor_is_categorical = FALSE,
          r120_method = "calculated_from_source_data"
        ))
      }
      
      # Fallback: Cannot determine without data
      return(list(
        predictor_min = NA, predictor_max = NA, predictor_range = NA,
        track_multiplier = NA, predictor_is_binary = FALSE,
        predictor_is_categorical = FALSE, r120_method = "no_source_data_available"
      ))
    }
    
    if (var_type %in% c("categorical_id", "numeric_unknown")) {
      # Unknown types: Mark as needing manual review
      return(list(
        predictor_min = NA, predictor_max = NA, predictor_range = NA,
        track_multiplier = 1, predictor_is_binary = FALSE,
        predictor_is_categorical = FALSE, r120_method = "type_unknown_needs_review"
      ))
    }
    
    # Should never reach here
    return(list(
      predictor_min = NA, predictor_max = NA, predictor_range = NA,
      track_multiplier = 1, predictor_is_binary = FALSE,
      predictor_is_categorical = FALSE, r120_method = "unhandled_case"
    ))
  }
  
  # Main enrichment logic - process row by row
  enriched_rows <- list()

  for (i in 1:nrow(poisson_data)) {
    row_data <- poisson_data[i, ]
    predictor_name <- row_data$predictor

    # Detect variable type
    var_type <- detect_variable_type(predictor_name)

    # Calculate range metadata
    range_meta <- calculate_range_metadata(predictor_name, var_type, source_data)

    # Add metadata to row
    row_data$predictor_min <- range_meta$predictor_min
    row_data$predictor_max <- range_meta$predictor_max
    row_data$predictor_range <- range_meta$predictor_range
    row_data$track_multiplier <- range_meta$track_multiplier
    row_data$predictor_is_binary <- range_meta$predictor_is_binary
    row_data$predictor_is_categorical <- range_meta$predictor_is_categorical
    row_data$r120_enrichment_method <- range_meta$r120_method
    row_data$r120_enrichment_date <- as.character(Sys.Date())

    enriched_rows[[i]] <- row_data
  }

  # Combine all rows
  enriched_data <- bind_rows(enriched_rows)

  return(enriched_data)
}
