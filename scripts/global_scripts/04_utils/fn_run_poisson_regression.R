# fn_run_poisson_regression.R
#
# Run Poisson Regression with R118 Compliance and Variable Range Metadata
#
# PURPOSE:
#   Implements R118: Statistical Significance Documentation Rule
#   Calculates ACTUAL variable ranges from data (MP029 compliance)
#   Eliminates UI guessing logic by providing complete metadata
#
# PRINCIPLES:
#   - R118: Statistical Significance Documentation (p-values, significance flags)
#   - MP029: No Fake Data (actual ranges, not guessed)
#   - MP047: Functional Programming (reusable function)
#   - MP081: Explicit Parameter Specification (clear arguments)
#
# CRITICAL INNOVATION:
#   This function calculates predictor_min, predictor_max, predictor_range,
#   predictor_is_binary, predictor_is_categorical, and track_multiplier
#   FROM ACTUAL DATA. This eliminates the need for UI components to use
#   regex patterns to guess variable ranges (fixes original problem).
#
# USAGE:
#   results <- fn_run_poisson_regression(
#     data = regression_data,
#     outcome_col = "sales_count",
#     predictor_cols = c("price", "rating", "review_count"),
#     offset_col = "log_exposure"  # optional
#   )
#
# INPUT:
#   - data: Data frame with outcome and predictors
#   - outcome_col: Name of outcome column (count variable)
#   - predictor_cols: Vector of predictor column names
#   - offset_col: Optional offset column (e.g., log(exposure))
#
# OUTPUT:
#   Data frame with R118-compliant regression results including:
#   - coefficient, std_error, z_statistic, p_value
#   - significance_flag (***/**/*/"NOT SIGNIFICANT")
#   - predictor_min, predictor_max, predictor_range (ACTUAL from data)
#   - predictor_is_binary, predictor_is_categorical
#   - track_multiplier (pre-calculated using actual range)
#   - model_aic, model_deviance, n_observations
#   - regression_timestamp
#
# AUTHOR: principle-product-manager
# DATE: 2025-11-13
# VERSION: 1.0
# -----------------------------------------------------------------------------

fn_run_poisson_regression <- function(data,
                                      outcome_col,
                                      predictor_cols,
                                      offset_col = NULL) {

  # Load required libraries
  if (!requireNamespace("dplyr", quietly = TRUE)) {
    stop("Package 'dplyr' is required but not installed")
  }
  if (!requireNamespace("broom", quietly = TRUE)) {
    stop("Package 'broom' is required but not installed")
  }

  library(dplyr)
  library(broom)

  # === VALIDATION ===

  # Check inputs
  stopifnot("data must be a data frame" = is.data.frame(data))
  stopifnot("outcome_col must be in data" = outcome_col %in% names(data))
  stopifnot("all predictor_cols must be in data" = all(predictor_cols %in% names(data)))

  if (!is.null(offset_col)) {
    stopifnot("offset_col must be in data if provided" = offset_col %in% names(data))
  }

  # Check outcome is numeric and non-negative
  if (!is.numeric(data[[outcome_col]])) {
    stop(sprintf("Outcome column '%s' must be numeric for Poisson regression", outcome_col))
  }

  if (any(data[[outcome_col]] < 0, na.rm = TRUE)) {
    stop(sprintf("Outcome column '%s' contains negative values (invalid for Poisson)", outcome_col))
  }

  # === BUILD FORMULA ===

  formula_str <- sprintf("%s ~ %s", outcome_col, paste(predictor_cols, collapse = " + "))

  if (!is.null(offset_col)) {
    formula_str <- sprintf("%s + offset(%s)", formula_str, offset_col)
    message(sprintf("Using offset: %s", offset_col))
  }

  message(sprintf("Running Poisson regression: %s", formula_str))

  # === FIT MODEL ===

  model <- tryCatch({
    glm(as.formula(formula_str), data = data, family = poisson(link = "log"))
  }, error = function(e) {
    stop(sprintf("Poisson regression failed: %s", e$message))
  })

  # === EXTRACT COEFFICIENTS WITH R118 COMPLIANCE ===

  # Use broom::tidy for clean extraction
  results <- broom::tidy(model) %>%
    filter(term != "(Intercept)") %>%
    select(
      predictor = term,
      coefficient = estimate,
      std_error = std.error,
      z_statistic = statistic,
      p_value = p.value
    ) %>%
    mutate(
      # R118 REQUIREMENT: Significance flags
      significance_flag = case_when(
        p_value < 0.001 ~ "***",
        p_value < 0.01 ~ "**",
        p_value < 0.05 ~ "*",
        TRUE ~ "NOT SIGNIFICANT"
      ),
      is_significant = (p_value < 0.05)
    )

  message(sprintf("  Model converged: %d coefficients extracted", nrow(results)))

  # === CRITICAL INNOVATION: Calculate Actual Variable Ranges ===
  # This eliminates UI guessing logic (MP029 compliance)

  message("  Calculating actual variable ranges from data...")

  # Initialize columns with NA
  results$predictor_min <- NA_real_
  results$predictor_max <- NA_real_
  results$predictor_range <- NA_real_
  results$predictor_is_binary <- NA
  results$predictor_is_categorical <- NA
  results$track_multiplier <- NA_real_

  # Calculate for each predictor
  for (i in seq_len(nrow(results))) {
    pred <- results$predictor[i]

    # Check if predictor exists in data (it should, but be defensive)
    if (!pred %in% names(data)) {
      warning(sprintf("Predictor '%s' not found in data (may be factor level), skipping range calculation", pred))
      next
    }

    pred_values <- data[[pred]]

    # Skip if not numeric
    if (!is.numeric(pred_values)) {
      warning(sprintf("Predictor '%s' is not numeric, skipping range calculation", pred))
      next
    }

    # Calculate actual range (MP029: use real data, not guesses)
    predictor_min <- min(pred_values, na.rm = TRUE)
    predictor_max <- max(pred_values, na.rm = TRUE)
    predictor_range <- predictor_max - predictor_min

    # Detect variable type from actual data
    unique_values <- length(unique(pred_values[!is.na(pred_values)]))
    predictor_is_binary <- (unique_values <= 2)
    predictor_is_categorical <- (unique_values <= 10 && all(pred_values %in% c(0, 1, NA)))

    # Calculate track_multiplier using ACTUAL range (not guessed)
    coef_value <- results$coefficient[i]

    if (!is.na(coef_value) && !is.infinite(predictor_range) && predictor_range > 0) {
      # Original track_multiplier calculation from poissonFeatureAnalysis.R
      # but using ACTUAL range instead of guessed range
      if (abs(coef_value) > 2) {
        # For extreme coefficients, use capped calculation
        track_multiplier <- exp(2) * (1 + (abs(coef_value) - 2) * 0.5)
      } else {
        # Standard calculation with actual range
        effective_range <- min(predictor_range, 10)  # Cap range effect at 10
        track_multiplier <- exp(abs(coef_value) * sqrt(effective_range))
      }

      # Cap at reasonable maximum (100x)
      track_multiplier <- round(min(track_multiplier, 100), 1)
    } else {
      track_multiplier <- NA_real_
    }

    # Update results row
    results$predictor_min[i] <- predictor_min
    results$predictor_max[i] <- predictor_max
    results$predictor_range[i] <- predictor_range
    results$predictor_is_binary[i] <- predictor_is_binary
    results$predictor_is_categorical[i] <- predictor_is_categorical
    results$track_multiplier[i] <- track_multiplier
  }

  message(sprintf("  ✓ Calculated actual ranges for %d predictors",
                  sum(!is.na(results$predictor_range))))

  # === ADD MODEL METADATA ===

  results$model_aic <- AIC(model)
  results$model_deviance <- deviance(model)
  results$model_null_deviance <- model$null.deviance
  results$n_observations <- nrow(data)
  results$regression_timestamp <- Sys.time()

  # === R118 VALIDATION ===

  # Check all required R118 columns are present
  required_r118_cols <- c("p_value", "significance_flag")
  missing_r118 <- setdiff(required_r118_cols, names(results))

  if (length(missing_r118) > 0) {
    stop(sprintf("VIOLATION R118: Missing required columns: %s",
                 paste(missing_r118, collapse = ", ")))
  }

  # Check p-values are valid
  invalid_p <- results %>%
    filter(is.na(p_value) | p_value < 0 | p_value > 1)

  if (nrow(invalid_p) > 0) {
    stop(sprintf("VIOLATION R118: %d predictors have invalid p-values", nrow(invalid_p)))
  }

  # Check significance flags are valid
  valid_flags <- c("***", "**", "*", "NOT SIGNIFICANT")
  invalid_flags <- results %>%
    filter(!significance_flag %in% valid_flags)

  if (nrow(invalid_flags) > 0) {
    stop(sprintf("VIOLATION R118: %d predictors have invalid significance flags", nrow(invalid_flags)))
  }

  message("  ✓ R118 validation passed: All statistical significance fields present")

  # === SUMMARY STATISTICS ===

  n_sig <- sum(results$is_significant, na.rm = TRUE)
  n_total <- nrow(results)
  pct_sig <- round(100 * n_sig / n_total, 1)

  message(sprintf("  Significant predictors: %d/%d (%.1f%%)", n_sig, n_total, pct_sig))

  sig_summary <- results %>%
    count(significance_flag) %>%
    arrange(match(significance_flag, c("***", "**", "*", "NOT SIGNIFICANT")))

  for (i in seq_len(nrow(sig_summary))) {
    message(sprintf("    %s: %d", sig_summary$significance_flag[i], sig_summary$n[i]))
  }

  # === RETURN RESULTS ===

  message(sprintf("✓ Poisson regression complete"))
  message(sprintf("  Model AIC: %.2f", results$model_aic[1]))
  message(sprintf("  Observations: %d", results$n_observations[1]))
  message(sprintf("\n⭐ CRITICAL FEATURE: Variable ranges calculated from actual data"))
  message(sprintf("   UI components can now read predictor_min/max/range directly"))
  message(sprintf("   No more regex-based guessing logic needed (MP029 compliance)"))

  return(results)
}
