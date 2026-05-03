# =============================================================================
# SCHEMA_001: Poisson Analysis Table Definition
# =============================================================================
# Purpose: Define the standard schema for Poisson regression analysis results
# Used by: WISER, MAMBA applications for time series analysis
# Critical: This table stores coefficient results from Poisson regression models
# =============================================================================

# Schema Definition
SCHEMA_poisson_analysis <- list(

  table_name = "df_{prefix}_poisson_analysis_{product_line}",

  description = "Stores Poisson regression analysis results for time series sales data",

  columns = list(

    # =========================================================================
    # IDENTIFIER COLUMNS (Required)
    # =========================================================================

    product_line_id = list(
      type = "VARCHAR",
      required = TRUE,
      description = "Product line identifier (e.g., 'alf', 'irf', 'pre', 'rek', 'tur', 'wak')",
      example = "alf"
    ),

    platform = list(
      type = "VARCHAR",
      required = TRUE,
      description = "Sales platform identifier (e.g., 'cbz', 'eby')",
      example = "cbz"
    ),

    predictor = list(
      type = "VARCHAR",
      required = TRUE,
      description = "Name of the predictor variable in the model",
      example = "year"
    ),

    predictor_type = list(
      type = "VARCHAR",
      required = TRUE,
      description = "Business classification for UI module routing (DM_R043 v2.0)",
      example = "time_feature",
      valid_values = c("time_feature", "product_attribute", "comment_attribute", "structural"),
      ui_module_mapping = list(
        time_feature = "poissonTimeAnalysis",
        product_attribute = "poissonFeatureAnalysis",
        comment_attribute = "poissonCommentAnalysis",
        structural = "EXCLUDED"
      )
    ),

    data_type = list(
      type = "VARCHAR",
      required = TRUE,
      description = "Statistical data type classification (DM_R043 v2.0)",
      example = "binary",
      valid_values = c("binary", "numerical", "dummy"),
      classification_rules = list(
        binary = "Original 0/1 variables (source_variable = NA)",
        numerical = "Continuous values with range > 1",
        dummy = "Dummy-coded from categorical (source_variable has value)"
      )
    ),

    source_variable = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Original categorical variable for dummy-coded columns (DM_R043 v2.0)",
      example = "brand",
      nullable = TRUE,
      purpose = "Enables grouping related dummy variables in UI displays"
    ),

    # =========================================================================
    # COEFFICIENT COLUMNS (Required)
    # =========================================================================

    coefficient = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Regression coefficient (beta) from the Poisson model",
      example = 0.7368,
      critical = "REQUIRED for analysis - missing this causes errors"
    ),

    incidence_rate_ratio = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Exponentiated coefficient (exp(beta)) representing multiplicative effect",
      example = 2.089,
      calculation = "exp(coefficient)"
    ),

    std_error = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Standard error of the coefficient estimate",
      example = 0.3429
    ),

    z_value = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Z-statistic for hypothesis testing (coefficient / std_error)",
      example = 2.15,
      calculation = "coefficient / std_error"
    ),

    p_value = list(
      type = "DOUBLE",
      required = TRUE,
      description = "P-value for statistical significance testing",
      example = 0.0316,
      interpretation = "< 0.05 indicates statistical significance"
    ),

    # =========================================================================
    # CONFIDENCE INTERVAL COLUMNS (Required)
    # =========================================================================

    conf_low = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Lower bound of 95% confidence interval for coefficient",
      example = 0.08,
      calculation = "coefficient - 1.96 * std_error"
    ),

    conf_high = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Upper bound of 95% confidence interval for coefficient",
      example = 1.4379,
      calculation = "coefficient + 1.96 * std_error"
    ),

    irr_conf_low = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Lower bound of 95% CI for incidence rate ratio",
      example = 1.08,
      calculation = "exp(conf_low)"
    ),

    irr_conf_high = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Upper bound of 95% CI for incidence rate ratio",
      example = 4.21,
      calculation = "exp(conf_high)"
    ),

    # =========================================================================
    # RANGE METADATA COLUMNS (Required - R120)
    # =========================================================================
    # R120: Variable Range Transparency
    # Purpose: Eliminate regex-based range guessing with actual data-driven ranges
    # Benefit: 100% accuracy vs 0-94% error rates from pattern matching
    # Innovation: DRV layer calculates these from real data, UI components consume

    predictor_min = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Minimum value of predictor in actual data (R120)",
      example = 0.0,
      critical = "REQUIRED for UI slider configuration - calculated from real data"
    ),

    predictor_max = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Maximum value of predictor in actual data (R120)",
      example = 2047.5,
      critical = "REQUIRED for UI slider configuration - calculated from real data"
    ),

    predictor_range = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Range of predictor (max - min) (R120)",
      example = 2047.5,
      calculation = "predictor_max - predictor_min",
      critical = "Used to determine track_multiplier for UI scaling"
    ),

    predictor_is_binary = list(
      type = "BOOLEAN",
      required = TRUE,
      description = "Whether predictor is binary (0/1) (R120)",
      example = FALSE,
      detection_rule = "TRUE if predictor_min = 0 AND predictor_max = 1"
    ),

    predictor_is_categorical = list(
      type = "BOOLEAN",
      required = FALSE,
      description = "Whether predictor is categorical (R120)",
      example = FALSE,
      detection_rule = "TRUE if predictor_type contains 'categorical'"
    ),

    track_multiplier = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Scaling factor for UI slider (R120)",
      example = 0.04884005,
      calculation = "100 / predictor_range",
      critical = "REQUIRED for UI slider step calculation - ensures smooth user interaction"
    ),

    # =========================================================================
    # DISPLAY NAME METADATA COLUMNS (Required - DM_R046)
    # =========================================================================
    # DM_R046: Variable Display Name Metadata Rule
    # Purpose: Provide user-friendly display names for technical variable names
    # Benefit: Improved UX - users see "5月" instead of "month_5"
    # Innovation: Three-layer system: technical name → display name → tooltip

    display_name = list(
      type = "VARCHAR",
      required = TRUE,
      description = "User-friendly display name in current locale (DM_R046)",
      example = "5月",
      critical = "REQUIRED for UI display - eliminates need for users to understand technical naming"
    ),

    display_name_en = list(
      type = "VARCHAR",
      required = FALSE,
      description = "English display name (DM_R046)",
      example = "May"
    ),

    display_name_zh = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Chinese display name (DM_R046)",
      example = "5月"
    ),

    display_category = list(
      type = "VARCHAR",
      required = TRUE,
      description = "Variable category for UI grouping (DM_R046)",
      example = "time",
      valid_values = c("time", "product_attribute", "seller", "location", "derived", "other")
    ),

    display_description = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Brief explanation of variable meaning (DM_R046)",
      example = "Sales in May"
    ),

    # =========================================================================
    # MODEL FIT COLUMNS (Required)
    # =========================================================================

    deviance = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Model deviance (goodness of fit measure)",
      example = 204
    ),

    aic = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Akaike Information Criterion for model comparison",
      example = 226,
      interpretation = "Lower AIC indicates better model fit"
    ),

    sample_size = list(
      type = "INTEGER",
      required = TRUE,
      description = "Number of observations used in the model",
      example = 1516
    ),

    # =========================================================================
    # METADATA COLUMNS (Required)
    # =========================================================================

    convergence = list(
      type = "VARCHAR",
      required = TRUE,
      description = "Model convergence status",
      example = "converged",
      valid_values = c("converged", "not_converged", "warning")
    ),

    analysis_date = list(
      type = "DATE",
      required = TRUE,
      description = "Date when the analysis was performed",
      example = "2025-06-10"
    ),

    analysis_version = list(
      type = "VARCHAR",
      required = TRUE,
      description = "Version of the analysis methodology",
      example = "v1.0"
    )
  ),

  # =========================================================================
  # TABLE CONSTRAINTS
  # =========================================================================

  constraints = list(
    primary_key = c("product_line_id", "platform", "predictor", "analysis_date"),

    indexes = c("product_line_id", "platform", "predictor_type"),

    foreign_keys = list(
      product_line = "References product_line master table"
    )
  ),

  # =========================================================================
  # DATA QUALITY RULES
  # =========================================================================

  quality_rules = list(
    coefficient_range = "coefficient should typically be between -10 and 10",
    irr_positive = "incidence_rate_ratio must be > 0",
    p_value_range = "p_value must be between 0 and 1",
    sample_size_min = "sample_size should be >= 30 for reliable estimates",
    ci_ordering = "conf_low < coefficient < conf_high",
    irr_ci_ordering = "irr_conf_low < incidence_rate_ratio < irr_conf_high"
  ),

  # =========================================================================
  # USAGE EXAMPLES
  # =========================================================================

  examples = list(

    create_table = '
    CREATE TABLE df_cbz_poisson_analysis_alf (
      product_line_id VARCHAR,
      platform VARCHAR,
      predictor VARCHAR,
      predictor_type VARCHAR,
      coefficient DOUBLE,
      incidence_rate_ratio DOUBLE,
      std_error DOUBLE,
      z_value DOUBLE,
      p_value DOUBLE,
      conf_low DOUBLE,
      conf_high DOUBLE,
      irr_conf_low DOUBLE,
      irr_conf_high DOUBLE,
      deviance DOUBLE,
      aic DOUBLE,
      sample_size INTEGER,
      convergence VARCHAR,
      analysis_date DATE,
      analysis_version VARCHAR
    )',

    query_significant = "
    SELECT predictor, coefficient, incidence_rate_ratio, p_value
    FROM df_cbz_poisson_analysis_alf
    WHERE p_value < 0.05
    ORDER BY abs(coefficient) DESC
    ",

    validate_data = "
    -- Check for missing coefficients
    SELECT COUNT(*) as missing_count
    FROM df_cbz_poisson_analysis_alf
    WHERE coefficient IS NULL
    "
  )
)

# =============================================================================
# VALIDATION FUNCTION
# =============================================================================

validate_poisson_analysis_table <- function(con, table_name) {

  # Check if table exists
  if (!dbExistsTable(con, table_name)) {
    return(list(
      valid = FALSE,
      error = paste("Table", table_name, "does not exist")
    ))
  }

  # Get actual columns
  actual_cols <- dbListFields(con, table_name)

  # Required columns (updated for DM_R043 v2.0)
  required_cols <- c(
    "product_line_id", "platform", "predictor", "predictor_type",
    "data_type",  # NEW: DM_R043 v2.0
    # source_variable is optional (nullable)
    "coefficient", "incidence_rate_ratio", "std_error", "z_value", "p_value",
    "conf_low", "conf_high", "irr_conf_low", "irr_conf_high",
    "predictor_min", "predictor_max", "predictor_range", "predictor_is_binary",
    "track_multiplier",
    "display_name", "display_category",
    "deviance", "aic", "sample_size", "convergence",
    "analysis_date", "analysis_version"
  )

  # Check for missing required columns
  missing_cols <- setdiff(required_cols, actual_cols)

  if (length(missing_cols) > 0) {
    return(list(
      valid = FALSE,
      error = paste("Missing required columns:", paste(missing_cols, collapse = ", ")),
      missing_columns = missing_cols
    ))
  }

  return(list(valid = TRUE, message = "Schema validation passed"))
}

# =============================================================================
# ETL FUNCTION
# =============================================================================

prepare_poisson_analysis_data <- function(model_results, product_line_id, platform,
                                          known_categorical = NULL) {

  # Extract coefficients from model
  coef_summary <- summary(model_results)$coefficients

  # Get predictor names
  predictor_names <- rownames(coef_summary)

  # Infer source_variable for dummy-coded columns (DM_R043 v2.0)
  source_var <- if (!is.null(known_categorical)) {
    infer_source_variable(predictor_names, known_categorical)
  } else {
    infer_source_variable(predictor_names)  # Use default known categoricals
  }

  # Prepare data frame with new DM_R043 v2.0 fields
  df <- data.frame(
    product_line_id = product_line_id,
    platform = platform,
    predictor = predictor_names,
    predictor_type = classify_predictor_type(predictor_names),
    source_variable = source_var,  # NEW: DM_R043 v2.0
    coefficient = coef_summary[, "Estimate"],
    std_error = coef_summary[, "Std. Error"],
    z_value = coef_summary[, "z value"],
    p_value = coef_summary[, "Pr(>|z|)"],
    stringsAsFactors = FALSE
  )

  # Calculate derived columns
  df$incidence_rate_ratio <- exp(df$coefficient)
  df$conf_low <- df$coefficient - 1.96 * df$std_error
  df$conf_high <- df$coefficient + 1.96 * df$std_error
  df$irr_conf_low <- exp(df$conf_low)
  df$irr_conf_high <- exp(df$conf_high)

  # Add model fit statistics
  df$deviance <- deviance(model_results)
  df$aic <- AIC(model_results)
  df$sample_size <- nobs(model_results)

  # Add metadata
  df$convergence <- ifelse(model_results$converged, "converged", "not_converged")
  df$analysis_date <- Sys.Date()
  df$analysis_version <- "v2.0"  # Updated for DM_R043 v2.0

  # Add data_type classification (DM_R043 v2.0)
  # NOTE: This is a placeholder - actual min/max should be calculated from data
  # In production DRV scripts, use classify_data_type() with actual data ranges
  df$data_type <- classify_data_type(
    predictor_names = df$predictor,
    predictor_min = 0,  # Placeholder - replace with actual data
    predictor_max = 1,  # Placeholder - replace with actual data
    source_variable = df$source_variable
  )

  return(df)
}

# =============================================================================
# CLASSIFICATION FUNCTIONS (DM_R043 v2.0)
# =============================================================================

#' Classify predictor_type (Business classification for UI routing)
#' @param predictor_names Character vector of predictor names
#' @return Character vector of predictor_type values
classify_predictor_type <- function(predictor_names) {
  dplyr::case_when(
    # Time features: temporal dummy variables and year
    grepl("^(month_|monday|tuesday|wednesday|thursday|friday|saturday|sunday|year$|day$|is_holiday|is_weekend)",
          predictor_names, ignore.case = TRUE) ~ "time_feature",

    # Structural: identifiers and names (EXCLUDE from analysis)
    grepl("_name$|_name[A-Z]|_id$|_code$|_series_name|^sku$|^asin$|^product_name|^seller_name|^brand\\.",
          predictor_names, ignore.case = TRUE) ~ "structural",

    # Comment attributes: review and sentiment related
    grepl("rating|sentiment|review|comment|stars|feedback",
          predictor_names, ignore.case = TRUE) ~ "comment_attribute",

    # Default: product attributes
    TRUE ~ "product_attribute"
  )
}

#' Classify data_type (Statistical type)
#' @param predictor_names Character vector of predictor names
#' @param predictor_min Numeric vector of minimum values
#' @param predictor_max Numeric vector of maximum values
#' @param source_variable Character vector of source variable names (NA for non-dummy)
#' @return Character vector of data_type values
classify_data_type <- function(predictor_names, predictor_min, predictor_max,
                               source_variable = NA_character_) {
  dplyr::case_when(
    # Dummy: has source_variable reference
    !is.na(source_variable) ~ "dummy",

    # Binary: only 0 and 1 values
    predictor_min == 0 & predictor_max == 1 ~ "binary",

    # Numerical: everything else
    TRUE ~ "numerical"
  )
}

#' Infer source_variable for dummy-coded columns
#' @param predictor_names Character vector of predictor names
#' @param known_categorical Character vector of known categorical variable prefixes
#' @return Character vector of source variable names (NA for non-dummy)
infer_source_variable <- function(predictor_names,
                                  known_categorical = c("brand", "color", "material",
                                                        "category", "design", "style",
                                                        "size", "type", "model")) {
  # Create pattern from known categoricals
  pattern <- paste0("^(", paste(known_categorical, collapse = "|"), ")_")

  # Extract source variable if matches pattern
  ifelse(
    grepl(pattern, predictor_names, ignore.case = TRUE),
    sub("_.*$", "", predictor_names),  # Extract prefix before first underscore
    NA_character_
  )
}

# Legacy alias for backward compatibility
classify_predictor <- classify_predictor_type