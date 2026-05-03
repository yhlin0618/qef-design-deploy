# ============================================================================
# Dual Coefficient Interpretation System for Poisson Regression
# 雙軌係數解釋系統：同時提供原始單位和標準化解釋
# 
# Following principles:
# - MP122: Statistical Interpretation Transparency (proposed)
# - R092: Universal DBI access patterns
# - MP064: ETL-Derivation separation
# 
# Purpose: Solve ISSUE_108 - provide both intuitive and comparable interpretations
# ============================================================================

#' Determine reasonable increment for a variable
#' 
#' @description
#' Intelligently selects an appropriate increment based on variable characteristics
#' 
#' @param var_data Numeric vector of variable values
#' @param var_name Character string of variable name for context
#' @return Numeric value representing a reasonable increment
determine_reasonable_increment <- function(var_data, var_name = NULL) {
  # Remove NA values
  var_data <- var_data[!is.na(var_data)]
  
  if (length(var_data) == 0) return(1)
  
  var_range <- diff(range(var_data))
  var_mean <- mean(var_data)
  n_unique <- length(unique(var_data))
  
  # Check if variable is likely discrete (e.g., ratings)
  if (n_unique <= 10 && all(var_data == round(var_data))) {
    return(1)  # Use 1 unit for discrete variables
  }
  
  # For continuous variables, use 10% of range or round numbers
  if (var_range < 1) {
    return(round(var_range * 0.1, 2))  # 10% of small range
  } else if (var_range < 10) {
    return(1)  # 1 unit for medium range
  } else if (var_range < 100) {
    return(10)  # 10 units for large range
  } else if (var_range < 1000) {
    return(100)  # 100 units for very large range
  } else {
    return(round(var_range * 0.1, -2))  # 10% rounded to nearest 100
  }
}

#' Format increment for display
#' 
#' @param increment Numeric increment value
#' @param var_name Variable name for context-specific formatting
#' @return Character string with formatted increment
format_increment <- function(increment, var_name = NULL) {
  # Special formatting for known variable types
  if (!is.null(var_name)) {
    if (grepl("price|cost|revenue", var_name, ignore.case = TRUE)) {
      return(paste0("$", format(increment, big.mark = ",")))
    }
    if (grepl("rating|score", var_name, ignore.case = TRUE)) {
      return(paste0(increment, " 分"))
    }
    if (grepl("quantity|count|number", var_name, ignore.case = TRUE)) {
      return(paste0(increment, " 個"))
    }
    if (grepl("percentage|percent|rate", var_name, ignore.case = TRUE)) {
      return(paste0(increment, "%"))
    }
  }
  
  # Default formatting
  if (increment < 1) {
    return(as.character(increment))
  } else {
    return(format(increment, big.mark = ","))
  }
}

#' Dual interpretation of Poisson regression coefficients
#' 
#' @description
#' Provides both actual unit and standardized interpretations of coefficients
#' to balance intuitive understanding with statistical comparability
#' 
#' @param var_name Character string of variable name
#' @param coef Numeric coefficient from Poisson regression
#' @param se Standard error of coefficient
#' @param p_val P-value of coefficient
#' @param var_data Numeric vector of variable values
#' @param cap_multiplier Numeric maximum multiplier to prevent extreme values (default: 100)
#' 
#' @return List containing multiple interpretation formats
#' @export
interpret_coefficient_dual <- function(var_name, coef, se, p_val, var_data,
                                     cap_multiplier = 100) {
  
  # Handle missing data
  var_data <- var_data[!is.na(var_data)]
  
  if (length(var_data) == 0) {
    return(list(
      variable = var_name,
      coefficient = NA,
      error_message = "Insufficient data for interpretation"
    ))
  }
  
  # Calculate variable statistics
  var_range <- range(var_data)
  var_sd <- sd(var_data)
  var_mean <- mean(var_data)
  var_median <- median(var_data)
  n_unique <- length(unique(var_data))
  
  # Determine reasonable increment
  unit_increment <- determine_reasonable_increment(var_data, var_name)
  
  # Calculate confidence interval
  ci_lower <- coef - 1.96 * se
  ci_upper <- coef + 1.96 * se
  
  # Create comprehensive interpretation
  result <- list(
    # Basic information
    variable = var_name,
    coefficient = round(coef, 4),
    std_error = round(se, 4),
    p_value = round(p_val, 4),
    sample_size = length(var_data),
    
    # Variable characteristics
    variable_info = list(
      mean = round(var_mean, 2),
      median = round(var_median, 2),
      sd = round(var_sd, 2),
      min = round(var_range[1], 2),
      max = round(var_range[2], 2),
      unique_values = n_unique
    ),
    
    # Method 1: Actual unit interpretation (intuitive)
    actual_unit = list(
      increment = unit_increment,
      increment_display = format_increment(unit_increment, var_name),
      effect_pct = round(pmin((exp(coef * unit_increment) - 1) * 100, 
                              cap_multiplier * 100), 1),
      ci_lower_pct = round((exp(ci_lower * unit_increment) - 1) * 100, 1),
      ci_upper_pct = round((exp(ci_upper * unit_increment) - 1) * 100, 1),
      interpretation = sprintf(
        "每增加 %s，銷量%s %.1f%%",
        format_increment(unit_increment, var_name),
        ifelse(coef > 0, "增加", "減少"),
        abs(round(pmin((exp(abs(coef) * unit_increment) - 1) * 100,
                      cap_multiplier * 100), 1))
      )
    ),
    
    # Method 2: Standardized interpretation (comparable)
    standardized = list(
      one_sd = round(var_sd, 2),
      effect_pct = round(pmin((exp(coef * var_sd) - 1) * 100,
                             cap_multiplier * 100), 1),
      ci_lower_pct = round((exp(ci_lower * var_sd) - 1) * 100, 1),
      ci_upper_pct = round((exp(ci_upper * var_sd) - 1) * 100, 1),
      interpretation = sprintf(
        "增加 1 個標準差 (%.2f)，銷量%s %.1f%%",
        var_sd,
        ifelse(coef > 0, "增加", "減少"),
        abs(round(pmin((exp(abs(coef) * var_sd) - 1) * 100,
                      cap_multiplier * 100), 1))
      )
    ),
    
    # Method 3: Full range effect (strategic view)
    full_range = list(
      from = round(var_range[1], 2),
      to = round(var_range[2], 2),
      range_width = round(diff(var_range), 2),
      multiplier = round(pmin(exp(diff(var_range) * abs(coef)), 
                             cap_multiplier), 1),
      interpretation = sprintf(
        "從最%s (%.2f) 到最%s (%.2f)，銷量相差 %.1f 倍",
        ifelse(coef > 0, "低", "高"),
        ifelse(coef > 0, var_range[1], var_range[2]),
        ifelse(coef > 0, "高", "低"),
        ifelse(coef > 0, var_range[2], var_range[1]),
        pmin(exp(diff(var_range) * abs(coef)), cap_multiplier)
      )
    ),
    
    # Method 4: Percentile comparison (practical view)
    percentile = list(
      p25 = quantile(var_data, 0.25),
      p75 = quantile(var_data, 0.75),
      iqr_effect = round(pmin((exp(coef * IQR(var_data)) - 1) * 100,
                             cap_multiplier * 100), 1),
      interpretation = sprintf(
        "從第 25 百分位到第 75 百分位，銷量%s %.1f%%",
        ifelse(coef > 0, "增加", "減少"),
        abs(round(pmin((exp(abs(coef) * IQR(var_data)) - 1) * 100,
                      cap_multiplier * 100), 1))
      )
    ),
    
    # Statistical significance
    significance = list(
      is_significant = p_val < 0.05,
      significance_level = case_when(
        p_val < 0.001 ~ "***",
        p_val < 0.01 ~ "**",
        p_val < 0.05 ~ "*",
        TRUE ~ "n.s."
      ),
      confidence_interval = sprintf("[%.4f, %.4f]", ci_lower, ci_upper),
      interpretation = case_when(
        p_val >= 0.05 ~ "統計上不顯著，結果僅供參考",
        p_val < 0.001 ~ "高度顯著，證據非常強",
        p_val < 0.01 ~ "顯著，證據較強",
        p_val < 0.05 ~ "顯著，有統計證據支持"
      )
    ),
    
    # Business impact classification
    business_impact = classify_business_impact(coef, var_sd, p_val),
    
    # Warning flags
    warnings = check_interpretation_warnings(coef, se, p_val, var_data)
  )
  
  return(result)
}

#' Classify business impact based on effect size and significance
#' 
#' @param coef Coefficient value
#' @param var_sd Standard deviation of variable
#' @param p_val P-value
#' @return List with impact classification
classify_business_impact <- function(coef, var_sd, p_val) {
  effect_size <- abs((exp(coef * var_sd) - 1) * 100)
  
  if (p_val >= 0.05) {
    return(list(
      level = "不確定",
      color = "gray",
      recommendation = "需要更多數據來確認影響"
    ))
  }
  
  if (effect_size < 5) {
    return(list(
      level = "微小",
      color = "lightblue",
      recommendation = "影響很小，優先級較低"
    ))
  } else if (effect_size < 20) {
    return(list(
      level = "中等",
      color = "yellow",
      recommendation = "有一定影響，可考慮優化"
    ))
  } else if (effect_size < 50) {
    return(list(
      level = "重要",
      color = "orange",
      recommendation = "重要影響因素，應重點關注"
    ))
  } else {
    return(list(
      level = "關鍵",
      color = "red",
      recommendation = "極重要因素，是核心競爭力"
    ))
  }
}

#' Check for interpretation warnings
#' 
#' @param coef Coefficient value
#' @param se Standard error
#' @param p_val P-value
#' @param var_data Variable data
#' @return Character vector of warnings
check_interpretation_warnings <- function(coef, se, p_val, var_data) {
  warnings <- character()
  
  # Check for extreme coefficient
  if (abs(coef) > 5) {
    warnings <- c(warnings, "係數值極大，解釋時請謹慎")
  }
  
  # Check for high uncertainty
  if (abs(se / coef) > 0.5 && !is.na(se) && coef != 0) {
    warnings <- c(warnings, "標準誤差較大，估計不確定性高")
  }
  
  # Check for limited data
  if (length(unique(var_data)) < 5) {
    warnings <- c(warnings, "變數取值較少，模型可能不穩定")
  }
  
  # Check for outliers
  if (length(boxplot.stats(var_data)$out) / length(var_data) > 0.1) {
    warnings <- c(warnings, "存在較多異常值，可能影響結果")
  }
  
  return(warnings)
}

#' Generate interpretation table for multiple variables
#' 
#' @param model_results Data frame with model results
#' @param data Original data for variable statistics
#' @param display_mode Character: "compact", "detailed", or "comparison"
#' @return Data frame formatted for display
#' @export
generate_interpretation_table <- function(model_results, data, 
                                        display_mode = "compact") {
  
  interpretations <- list()
  
  for (i in seq_len(nrow(model_results))) {
    var_name <- model_results$predictor[i]
    
    if (var_name %in% names(data)) {
      interp <- interpret_coefficient_dual(
        var_name = var_name,
        coef = model_results$coefficient[i],
        se = model_results$std_error[i],
        p_val = model_results$p_value[i],
        var_data = data[[var_name]]
      )
      interpretations[[var_name]] <- interp
    }
  }
  
  # Format based on display mode
  if (display_mode == "compact") {
    # Compact view for dashboard
    result_df <- do.call(rbind, lapply(interpretations, function(x) {
      data.frame(
        變數 = x$variable,
        實際效果 = x$actual_unit$interpretation,
        標準化效果 = x$standardized$interpretation,
        顯著性 = x$significance$significance_level,
        影響等級 = x$business_impact$level,
        stringsAsFactors = FALSE
      )
    }))
  } else if (display_mode == "detailed") {
    # Detailed view with all statistics
    result_df <- do.call(rbind, lapply(interpretations, function(x) {
      data.frame(
        變數 = x$variable,
        係數 = x$coefficient,
        標準誤 = x$std_error,
        P值 = x$p_value,
        每單位效果 = paste0(x$actual_unit$effect_pct, "%"),
        標準差效果 = paste0(x$standardized$effect_pct, "%"),
        全範圍倍數 = paste0(x$full_range$multiplier, "倍"),
        顯著性 = x$significance$significance_level,
        信賴區間 = x$significance$confidence_interval,
        警告 = paste(x$warnings, collapse = "; "),
        stringsAsFactors = FALSE
      )
    }))
  } else {
    # Comparison view for decision making
    result_df <- do.call(rbind, lapply(interpretations, function(x) {
      data.frame(
        變數 = x$variable,
        單位增量 = x$actual_unit$increment_display,
        單位效果 = paste0(x$actual_unit$effect_pct, "%"),
        標準差 = round(x$variable_info$sd, 2),
        標準化效果 = paste0(x$standardized$effect_pct, "%"),
        範圍 = paste0("[", x$variable_info$min, ", ", x$variable_info$max, "]"),
        賽道倍數 = paste0(x$full_range$multiplier, "倍"),
        業務影響 = x$business_impact$level,
        建議 = x$business_impact$recommendation,
        stringsAsFactors = FALSE
      )
    }))
  }
  
  return(result_df)
}

# Test function for verification
test_dual_interpretation <- function() {
  # Create test data
  set.seed(123)
  test_data <- data.frame(
    price = runif(100, 10, 100),
    rating = sample(1:5, 100, replace = TRUE),
    quantity = rpois(100, lambda = 20)
  )
  
  # Simulate coefficients
  test_results <- data.frame(
    predictor = c("price", "rating", "quantity"),
    coefficient = c(-0.02, 0.35, 0.015),
    std_error = c(0.005, 0.08, 0.003),
    p_value = c(0.0001, 0.0001, 0.0001)
  )
  
  # Generate interpretation table
  interp_table <- generate_interpretation_table(
    test_results, 
    test_data, 
    display_mode = "comparison"
  )
  
  print(interp_table)
  
  # Test individual interpretation
  price_interp <- interpret_coefficient_dual(
    var_name = "price",
    coef = -0.02,
    se = 0.005,
    p_val = 0.0001,
    var_data = test_data$price
  )
  
  cat("\n詳細解釋範例 (price):\n")
  cat("實際單位:", price_interp$actual_unit$interpretation, "\n")
  cat("標準化:", price_interp$standardized$interpretation, "\n")
  cat("全範圍:", price_interp$full_range$interpretation, "\n")
  cat("業務影響:", price_interp$business_impact$recommendation, "\n")
}