# =============================================================================
# fn_rsv_classification.R
# Shared RSV Classification + Marketing Strategy Assignment
# Following: SO_R007 (One Function One File), DEV_R001 (Vectorized Operations)
#
# All labels use English keys. Use translate() at the UI layer for Chinese.
# Strategy display content loaded from 30_global_data/parameters/marketing_strategies.yaml
# Following DEV_R050: no hardcoded display data in R functions
# Issue: #219
# =============================================================================

# ---- Module-level cache for marketing strategies YAML ----
.mkt_strat_cache <- new.env(parent = emptyenv())

.get_marketing_strategies <- function() {
  if (!is.null(.mkt_strat_cache$strategies)) return(.mkt_strat_cache$strategies)

  yaml_rel <- file.path("30_global_data", "parameters", "marketing_strategies.yaml")
  yaml_path <- NULL

  # Method 1: GLOBAL_DIR (available in both pipeline and app contexts)
  if (exists("GLOBAL_DIR", envir = .GlobalEnv)) {
    candidate <- file.path(get("GLOBAL_DIR", envir = .GlobalEnv), yaml_rel)
    if (file.exists(candidate)) yaml_path <- candidate
  }

  # Method 2: common relative paths
  if (is.null(yaml_path)) {
    for (prefix in c("scripts/global_scripts", "global_scripts")) {
      candidate <- file.path(prefix, yaml_rel)
      if (file.exists(candidate)) { yaml_path <- candidate; break }
    }
  }

  if (!is.null(yaml_path) && file.exists(yaml_path)) {
    data <- yaml::read_yaml(yaml_path)
    .mkt_strat_cache$strategies <- data$strategies
    message("[RSV] Loaded marketing strategies from YAML: ", length(data$strategies), " strategies")
    return(data$strategies)
  }

  warning("[RSV] marketing_strategies.yaml not found - strategy key used as fallback")
  NULL
}

#' Classify customers into RSV (Risk-Stability-Value) segments
#' and assign marketing strategies based on multi-tag combinations.
#'
#' @param df Data frame with DNA columns from df_dna_by_customer
#' @return Data frame with added RSV classification and marketing strategy columns
classify_rsv_and_strategy <- function(df) {
  if (is.null(df) || nrow(df) == 0) return(df)

  # ---- RSV Classification ----

  # R (Risk / Dormancy Risk) from nrec_prob (0-1 churn probability)
  # Higher nrec_prob = higher risk
  df$r_level <- ifelse(
    is.na(df$nrec_prob), "Mid",
    ifelse(df$nrec_prob > 0.7, "High",
           ifelse(df$nrec_prob >= 0.3, "Mid", "Low"))
  )


  # S (Stability / Transaction Regularity) from cri_ecdf
  # Lower CRI = more regular = higher stability
  # cri_ecdf: lower percentile = lower CRI = higher stability
  df$s_level <- ifelse(
    is.na(df$cri_ecdf), "Mid",
    ifelse(df$cri_ecdf < 0.33, "High",
           ifelse(df$cri_ecdf <= 0.67, "Mid", "Low"))
  )

  # V (Value / Customer Lifetime Value) from clv
  # Dynamic quantile thresholds
  clv_vals <- df$clv_value[!is.na(df$clv_value)]
  if (length(clv_vals) >= 5) {
    v_p20 <- quantile(clv_vals, 0.20, na.rm = TRUE)
    v_p80 <- quantile(clv_vals, 0.80, na.rm = TRUE)
  } else {
    v_p20 <- 0
    v_p80 <- Inf
  }
  df$v_level <- ifelse(
    is.na(df$clv_value), "Mid",
    ifelse(df$clv_value > v_p80, "High",
           ifelse(df$clv_value >= v_p20, "Mid", "Low"))
  )

  # RSV composite key
  df$rsv_key <- paste(df$r_level, df$s_level, df$v_level, sep = "-")

  # ---- 27 Customer Types (English keys) ----
  rsv_type_map <- c(
    "Low-High-High"  = "Diamond Customer",
    "Low-High-Mid"   = "Growing Loyal Customer",
    "Low-Mid-High"   = "Quality Potential Customer",
    "Low-Mid-Mid"    = "Stable Base Customer",
    "Low-Low-High"   = "High-Value New Customer",
    "Low-Low-Mid"    = "Regular New Customer",
    "Low-Low-Low"    = "Marginal Observer",
    "Low-High-Low"   = "Loyal Low-Spend Customer",
    "Mid-High-High"  = "Silent VIP",
    "Mid-High-Mid"   = "At-Risk Loyal Customer",
    "Mid-High-Low"   = "Downgraded Loyal Customer",
    "Mid-Mid-High"   = "Alert High-Value Customer",
    "Mid-Mid-Mid"    = "Potential Group",
    "Mid-Mid-Low"    = "Undeveloped Group",
    "Mid-Low-High"   = "Unstable High-Value Customer",
    "Mid-Low-Mid"    = "Unstable Regular Customer",
    "Mid-Low-Low"    = "Low-Activity Marginal Customer",
    "High-High-High" = "Lost Diamond Customer",
    "High-High-Mid"  = "At-Risk Core Customer",
    "High-High-Low"  = "Lost Loyal Customer",
    "High-Mid-High"  = "Lost High-Value Customer",
    "High-Mid-Mid"   = "Lost Regular Customer",
    "High-Mid-Low"   = "Lost Low-Value Customer",
    "High-Low-High"  = "Lost High-Value Customer",
    "High-Low-Mid"   = "Lost Regular Customer",
    "High-Low-Low"   = "Dormant Customer"
  )

  df$customer_type <- ifelse(
    df$rsv_key %in% names(rsv_type_map),
    rsv_type_map[df$rsv_key],
    "Other Customer"
  )

  # RSV action suggestions (English keys)
  rsv_action_map <- c(
    "Low-High-High"  = "Exclusive Service / New Product Priority / Brand Ambassador",
    "Low-High-Mid"   = "Bundle Offers / Upgrade Incentives / Loyalty Missions",
    "Low-Mid-High"   = "Increase Purchase Frequency / Regular Recommendations",
    "Low-Mid-Mid"    = "Steady Operations / Continuous Engagement",
    "Low-Low-High"   = "Build Purchase Habits / New Customer Exclusive Offers",
    "Low-Low-Mid"    = "Cultivate Interest / Gamified Marketing",
    "Low-Low-Low"    = "Trial Coupons / Entry Offers / Cross-Brand Events",
    "Low-High-Low"   = "Increase AOV / Recommend High-Value Products",
    "Mid-High-High"  = "Brand Care / Birthday Gift / Event Invitation",
    "Mid-High-Mid"   = "Repurchase Reminder / Exclusive Offers",
    "Mid-High-Low"   = "Small Coupons / Stay in Touch",
    "Mid-Mid-High"   = "Repurchase Reminder / VIP Wake-Up Gift / Retargeting",
    "Mid-Mid-Mid"    = "New Product Recommendations / Brand Story / Mission Rewards",
    "Mid-Mid-Low"    = "Small Trial / Entry Recommendations",
    "Mid-Low-High"   = "Build Purchase Regularity / Regular Reminders",
    "Mid-Low-Mid"    = "Cultivate Habits / Gamified Interaction",
    "Mid-Low-Low"    = "Low-Cost Exposure / Re-Registration Incentives",
    "High-High-High" = "Priority Win-Back / Exclusive Offers / Phone Outreach",
    "High-High-Mid"  = "Countdown Offers / Satisfaction Survey",
    "High-High-Low"  = "Small Wake-Up / Brand Care",
    "High-Mid-High"  = "Retargeting Ads / Exclusive Offers / Phone Outreach",
    "High-Mid-Mid"   = "Retargeting / Return Offers",
    "High-Mid-Low"   = "Low-Cost Retargeting",
    "High-Low-High"  = "Retargeting Ads / Exclusive Offers / Phone Outreach",
    "High-Low-Mid"   = "Retargeting / Return Offers",
    "High-Low-Low"   = "Low-Cost Exposure / Re-Registration Incentives"
  )

  df$rsv_action <- ifelse(
    df$rsv_key %in% names(rsv_action_map),
    rsv_action_map[df$rsv_key],
    "Basic Maintenance"
  )

  # ---- RFM Score (3-15) ----
  # Compute from ECDF-based scores (0-1 range)
  safe_score <- function(x) {
    s <- ceiling(x * 5)
    s[is.na(s)] <- 3L
    pmax(1L, pmin(5L, s))
  }

  if (all(c("dna_r_score", "dna_f_score", "dna_m_score") %in% names(df))) {
    df$rfm_score <- safe_score(df$dna_r_score) +
                    safe_score(df$dna_f_score) +
                    safe_score(df$dna_m_score)
  } else {
    df$rfm_score <- NA_integer_
  }

  # ---- CLV Level (for marketing strategy) ----
  clv_level_vals <- df$clv_value[!is.na(df$clv_value)]
  if (length(clv_level_vals) >= 5) {
    clv_p20 <- quantile(clv_level_vals, 0.20, na.rm = TRUE)
    clv_p80 <- quantile(clv_level_vals, 0.80, na.rm = TRUE)
  } else {
    clv_p20 <- 0
    clv_p80 <- Inf
  }
  df$clv_level <- ifelse(
    is.na(df$clv_value), "Mid",
    ifelse(df$clv_value > clv_p80, "High",
           ifelse(df$clv_value >= clv_p20, "Mid", "Low"))
  )

  # ---- CAI Threshold ----
  df$cai_low <- !is.na(df$cai_value) & df$cai_value < -0.2

  # ---- NES Status Normalization ----
  df$nes_norm <- toupper(trimws(as.character(df$nes_status)))
  df$nes_norm[is.na(df$nes_norm) | df$nes_norm == ""] <- "E0"

  # ---- 13 Marketing Strategies (Priority-based, data-driven from YAML) ----
  # Following DEV_R050: display content from 30_global_data/parameters/marketing_strategies.yaml
  n <- nrow(df)
  df$marketing_strategy      <- rep(NA_character_, n)
  df$marketing_purpose       <- rep(NA_character_, n)
  df$marketing_recommendation <- rep(NA_character_, n)

  strategies_data <- .get_marketing_strategies()

  # Helper: set strategy for unassigned rows matching condition
  # Looks up purpose/recommendation from YAML data by strategy key
  assign_strat <- function(df, cond, strategy_key) {
    idx <- which(is.na(df$marketing_strategy) & cond)
    if (length(idx) > 0) {
      df$marketing_strategy[idx] <- strategy_key
      strat <- if (!is.null(strategies_data)) strategies_data[[strategy_key]] else NULL
      if (!is.null(strat)) {
        df$marketing_purpose[idx] <- strat$purpose
        df$marketing_recommendation[idx] <- strat$recommendation
      } else {
        df$marketing_purpose[idx] <- strategy_key
        df$marketing_recommendation[idx] <- ""
      }
    }
    df
  }

  # Priority 1: Sleeping customers (S1/S2/S3) -> Awakening/Return
  df <- assign_strat(df, df$nes_norm %in% c("S1", "S2", "S3"), "Awakening / Return")

  # Priority 2: Low activity + mid/high risk -> Relationship Repair
  df <- assign_strat(df, df$cai_low & df$r_level %in% c("Mid", "High"), "Relationship Repair")

  # Priority 3: High risk -> Cost Control
  df <- assign_strat(df, df$r_level == "High", "Cost Control")

  # Priority 4: New customers (N) -> Onboarding
  df <- assign_strat(df, df$nes_norm == "N", "New Customer Nurturing")

  # Priority 5: Low RFM + Low CLV -> Low-cost Nurturing
  rfm_valid <- !is.na(df$rfm_score)
  df <- assign_strat(df, rfm_valid & df$rfm_score <= 5 & df$clv_level == "Low", "Low-Cost Nurturing")

  # Priority 6: Mid RFM (5-10), split by S-level
  mid_rfm <- rfm_valid & df$rfm_score > 5 & df$rfm_score <= 10
  df <- assign_strat(df, mid_rfm & df$s_level == "Low", "Standard Nurturing (Conservative)")
  df <- assign_strat(df, mid_rfm & df$s_level == "Mid", "Standard Nurturing (Core)")
  df <- assign_strat(df, mid_rfm & df$s_level == "High", "Standard Nurturing (Advanced)")

  # Priority 7: High RFM (>10), split by S-level
  high_rfm <- rfm_valid & df$rfm_score > 10
  df <- assign_strat(df, high_rfm & df$s_level == "Low", "VIP Maintenance (Low Stability)")
  df <- assign_strat(df, high_rfm & df$s_level == "Mid", "VIP Maintenance (Mid Stability)")
  df <- assign_strat(df, high_rfm & df$s_level == "High", "VIP Maintenance (High Stability)")

  # Priority 8: Active + High CLV -> Premium Retention
  df <- assign_strat(df, df$nes_norm == "E0" & df$clv_level == "High", "Premium Retention")

  # Priority 9: All others -> Basic Retention
  df <- assign_strat(df, rep(TRUE, n), "Basic Maintenance")

  # ---- CAI Text Label ----
  df$cai_text <- ifelse(
    is.na(df$ni_count) | df$ni_count < 4, NA_character_,
    ifelse(is.na(df$cai_value), NA_character_,
           ifelse(df$cai_value < -0.2, "Trending Inactive",
                  ifelse(df$cai_value > 0.2, "Trending Active", "Stable")))
  )

  # Clean up temp columns
  df$cai_low   <- NULL
  df$nes_norm  <- NULL

  df
}


#' Get the 19-column export definition for Customer Export
#'
#' @param df Classified data frame (output of classify_rsv_and_strategy)
#' @return Data frame with standardized 19 export columns
prepare_customer_export <- function(df) {
  if (is.null(df) || nrow(df) == 0) return(df)

  export_df <- data.frame(
    customer_id        = df$customer_id,
    rfm_score          = df$rfm_score,
    r_value            = df$r_value,
    f_value            = df$f_value,
    m_value            = df$m_value,
    cai_activity       = df$cai_text,
    nes_status         = df$nes_status,
    churn_prob         = round(df$nrec_prob, 3),
    transaction_count  = df$ni_count,
    avg_purchase_interval = ifelse(
      is.na(df$ni_count) | df$ni_count <= 1, NA_real_, round(df$ipt_mean, 1)
    ),
    total_spent        = round(df$spent_total, 0),
    clv                = round(df$clv_value, 0),
    r_level            = df$r_level,
    s_level            = df$s_level,
    v_level            = df$v_level,
    customer_type      = df$customer_type,
    marketing_strategy = df$marketing_strategy,
    marketing_purpose  = df$marketing_purpose,
    marketing_recommendation = df$marketing_recommendation,
    stringsAsFactors   = FALSE
  )

  export_df
}
