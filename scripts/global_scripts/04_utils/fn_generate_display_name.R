# =============================================================================
# FUNCTION: fn_generate_display_name
# =============================================================================
# Purpose: Generate user-friendly display names for technical variable names
# Principle: DM_R046 - Variable Display Name Metadata Rule
#            MP054 - No Fake Data (AI translation is language conversion, not data fabrication)
#            MP137 - No Hardcoded Project-Specific Content
#            SO_P015 - No Hardcoded Lookup Tables
#            DM_R004 Section 6 - Three-Tier Data Architecture
#            UI_P013 - Category-Level Language Override
# Layer: ETL (1ST) - Generate metadata for downstream enrichment
# Version: 3.3 (2025-12-26) - Added category_languages support (UI_P013)
#          3.2 (2025-12-14) - Phase 5: Simplified to single connection (DM_R004 Section 6)
#          3.1 (2025-12-14) - Added global_con for 30_global_data lookup
#          3.0 (2025-12-14) - Added database-first lookup (MP137, SO_P015)
#          2.0 (2025-11-14) - Added AI translation fallback
# =============================================================================
#
# ARCHITECTURE (MP137, DM_R004 Section 6 Compliance):
#   1. Company database lookup FIRST (cache_con -> df_metadata_turbo)
#   2. Universal database lookup (global_con -> df_metadata_common, if provided)
#   3. Rule-based fallback (DEPRECATED - to be migrated in Phase 2)
#   4. AI translation fallback (if enabled, uses cache_con for caching)
#   5. Technical name formatting
#
# The hardcoded mappings in this file are DEPRECATED and will be migrated
# to database tables in a future phase. See SO_P015 for migration guide.
#
# CONNECTION PATTERN (DM_R004 Section 6 - Three-Tier Architecture):
#   - cache_con:  Connection to {company}/data/app_data/app_data.duckdb (PRIMARY)
#                 Used for: Company-specific metadata (df_metadata_turbo for MAMBA)
#                           AI translation caching
#   - global_con: Connection to global_scripts/30_global_data/global_scd_type1.duckdb (OPTIONAL)
#                 Used for: Universal metadata (df_metadata_common for all companies)
#                 Note: Only needed if querying data shared by ALL companies
#
# IMPORTANT: df_metadata_turbo is MAMBA-specific and belongs in app_data, not 30_global_data.
#            30_global_data/ is for data shared by ALL companies (VitalSigns, BrandEdge, etc.)
#
# CATEGORY LANGUAGE OVERRIDE (UI_P013):
#   - category_languages parameter allows different display languages per category
#   - Example: product_attribute -> "en" (English), time -> "zh_TW" (Chinese)
#   - If category_languages is NULL, uses locale parameter as default
#   - Categories: product_attribute, time, location, seller, derived, other
#
# =============================================================================

# -----------------------------------------------------------------------------
# HELPER: Get effective language for a display category (UI_P013)
# -----------------------------------------------------------------------------
get_category_language <- function(display_category,
                                   category_languages,
                                   default_locale) {
  # Return default if no overrides configured
  if (is.null(category_languages)) {
    return(default_locale)
  }

  # Check for category-specific override
  category_lang <- category_languages[[display_category]]

  # If no override or empty, use default
  if (is.null(category_lang) || category_lang == "") {
    return(default_locale)
  }

  # Normalize language code
  if (category_lang %in% c("en", "en_US", "english", "English")) {
    return("en")
  } else if (category_lang %in% c("zh_TW", "zh-TW", "chinese", "Chinese")) {
    return("zh_TW")
  } else if (category_lang %in% c("zh_CN", "zh-CN")) {
    return("zh_CN")
  }

  return(category_lang)
}

fn_generate_display_name <- function(
  predictor,
  locale = "zh_TW",
  use_ai = FALSE,      # Enable AI fallback for unknown variables
  global_con = NULL,   # DM_R004 Section 6: 30_global_data connection (for df_metadata_common - universal)
  cache_con = NULL,    # DM_R004 Section 6: app_data connection (PRIMARY - for df_metadata_turbo + AI cache)
  category_languages = NULL  # UI_P013: Category language overrides from app_config.yaml
) {

  # UI_P013: Load category_languages from app_configs if not provided
  if (is.null(category_languages) && exists("app_configs", envir = globalenv())) {
    app_configs <- get("app_configs", envir = globalenv())
    if (!is.null(app_configs$localization$category_languages)) {
      category_languages <- app_configs$localization$category_languages
    }
  }

  # ==========================================================================
  # PRIORITY 1: Company Database Lookup (MP137, SO_P015, DM_R004 Section 6)
  # ==========================================================================
  # Check COMPANY database (app_data) for company-specific metadata FIRST
  #
  # Per DM_R004 Section 6 (Three-Tier Architecture):
  # - df_metadata_turbo is MAMBA-specific → belongs in app_data (company tier)
  # - 30_global_data/ is only for data shared by ALL companies
  #
  # cache_con connects to {company}/data/app_data/app_data.duckdb

  if (!is.null(cache_con) && inherits(cache_con, "DBIConnection")) {

    # Try df_metadata_turbo (MAMBA turbocharger product attributes)
    # Use PREFIX matching: predictor like "seller_nameEiji-Takada" should match "seller_name"
    # Also support Chinese prefix: "壓縮葉輪高流量設計TRUE" matches "壓縮葉輪高流量設計"
    db_result <- tryCatch({
      # Escape single quotes in predictor for SQL safety
      safe_predictor <- gsub("'", "''", predictor)
      query <- sprintf(
        "SELECT *, LENGTH(predictor_pattern) as pattern_len
         FROM df_metadata_turbo
         WHERE '%s' LIKE predictor_pattern || '%%'
            OR '%s' LIKE display_name_zh || '%%'
         ORDER BY pattern_len DESC
         LIMIT 1",
        safe_predictor, safe_predictor
      )
      result <- DBI::dbGetQuery(cache_con, query)
      if (nrow(result) > 0) result else NULL
    }, error = function(e) NULL)

    if (!is.null(db_result) && nrow(db_result) > 0) {
      # Extract the value part (what comes after the matched prefix)
      matched_pattern <- db_result$predictor_pattern[1]
      matched_zh <- db_result$display_name_zh[1]

      # Determine which pattern matched and extract the value
      value_part <- ""
      if (nchar(matched_pattern) > 0 && startsWith(predictor, matched_pattern)) {
        value_part <- substring(predictor, nchar(matched_pattern) + 1)
      } else if (nchar(matched_zh) > 0 && startsWith(predictor, matched_zh)) {
        value_part <- substring(predictor, nchar(matched_zh) + 1)
      }

      # Format display name with value if present
      display_zh <- db_result$display_name_zh[1]
      display_en <- db_result$display_name_en[1]
      display_cat <- db_result$display_category[1]

      if (nchar(value_part) > 0 && value_part != "TRUE" && value_part != "FALSE") {
        display_zh <- paste0(display_zh, ": ", value_part)
        display_en <- paste0(display_en, ": ", value_part)
      } else if (value_part == "TRUE") {
        # For boolean attributes, just use the attribute name
        display_zh <- display_zh
        display_en <- display_en
      } else if (value_part == "FALSE") {
        display_zh <- paste0("非", display_zh)
        display_en <- paste0("Not ", display_en)
      }

      # UI_P013: Determine effective language based on category override
      effective_locale <- get_category_language(
        display_category = display_cat,
        category_languages = category_languages,
        default_locale = locale
      )

      # Select display name based on effective locale
      display_name <- if (effective_locale == "en") display_en else display_zh

      return(list(
        display_name = display_name,
        display_name_en = display_en,
        display_name_zh = display_zh,
        display_category = display_cat,
        effective_locale = effective_locale,  # UI_P013: Track which language was used
        display_description = if (effective_locale == "zh_TW")
          paste0("產品屬性: ", display_zh)
        else
          paste0("Product attribute: ", display_en),
        translation_method = "database_metadata"
      ))
    }
  }

  # ==========================================================================
  # PRIORITY 2: Universal Database Lookup (global_con - if provided)
  # ==========================================================================
  # Check UNIVERSAL database (30_global_data) for data shared by ALL companies
  # This is optional - only needed for df_metadata_common, etc.

  if (!is.null(global_con) && inherits(global_con, "DBIConnection")) {

    # Future: Try df_metadata_common (time, colors, materials, etc.)
    # This will be added when df_metadata_common is created
    # db_result <- tryCatch({
    #   query <- sprintf(
    #     "SELECT * FROM df_metadata_common WHERE predictor_pattern = '%s' LIMIT 1",
    #     predictor
    #   )
    #   result <- DBI::dbGetQuery(global_con, query)
    #   if (nrow(result) > 0) result else NULL
    # }, error = function(e) NULL)
  }

  # ==========================================================================
  # CATEGORY 1: Time Variables (DEPRECATED - migrate to df_metadata_common)
  # ==========================================================================

  # Month variables (month_1 to month_12)
  if (grepl("^month_([0-9]+)$", predictor)) {
    month_num <- as.integer(sub("^month_([0-9]+)$", "\\1", predictor))
    display_zh <- paste0(month_num, "月")
    display_en <- month.name[month_num]

    # UI_P013: Determine effective language
    effective_locale <- get_category_language("time", category_languages, locale)

    return(list(
      display_name = if (effective_locale == "en") display_en else display_zh,
      display_name_en = display_en,
      display_name_zh = display_zh,
      display_category = "time",
      effective_locale = effective_locale,
      display_description = if (effective_locale == "zh_TW")
        paste0(month_num, "月份銷售")
      else
        paste0("Sales in ", display_en)
    ))
  }

  # Day of week variables
  weekday_map <- list(
    monday = list(zh = "星期一", en = "Monday"),
    tuesday = list(zh = "星期二", en = "Tuesday"),
    wednesday = list(zh = "星期三", en = "Wednesday"),
    thursday = list(zh = "星期四", en = "Thursday"),
    friday = list(zh = "星期五", en = "Friday"),
    saturday = list(zh = "星期六", en = "Saturday"),
    sunday = list(zh = "星期日", en = "Sunday")
  )

  if (predictor %in% names(weekday_map)) {
    display_zh <- weekday_map[[predictor]]$zh
    display_en <- weekday_map[[predictor]]$en

    # UI_P013: Determine effective language
    effective_locale <- get_category_language("time", category_languages, locale)

    return(list(
      display_name = if (effective_locale == "en") display_en else display_zh,
      display_name_en = display_en,
      display_name_zh = display_zh,
      display_category = "time",
      effective_locale = effective_locale,
      display_description = if (effective_locale == "zh_TW")
        paste0(display_zh, "銷售")
      else
        paste0("Sales on ", display_en)
    ))
  }

  # Other time features
  time_features <- list(
    year = list(zh = "年份", en = "Year", desc_zh = "銷售年份", desc_en = "Sales year"),
    day = list(zh = "日期", en = "Day of Month", desc_zh = "月份中的日期", desc_en = "Day of the month"),
    quarter = list(zh = "季度", en = "Quarter", desc_zh = "銷售季度", desc_en = "Sales quarter"),
    week = list(zh = "週", en = "Week", desc_zh = "銷售週", desc_en = "Sales week")
  )

  if (predictor %in% names(time_features)) {
    display_zh <- time_features[[predictor]]$zh
    display_en <- time_features[[predictor]]$en

    # UI_P013: Determine effective language
    effective_locale <- get_category_language("time", category_languages, locale)

    return(list(
      display_name = if (effective_locale == "en") display_en else display_zh,
      display_name_en = display_en,
      display_name_zh = display_zh,
      display_category = "time",
      effective_locale = effective_locale,
      display_description = if (effective_locale == "zh_TW")
        time_features[[predictor]]$desc_zh
      else
        time_features[[predictor]]$desc_en
    ))
  }

  # ==========================================================================
  # CATEGORY 2: Product Attributes
  # ==========================================================================

  # Price variables (including avg_price, original_price)
  if (grepl("^price|^avg_price|^original_price", predictor)) {
    # UI_P013: Determine effective language for product_attribute
    effective_locale <- get_category_language("product_attribute", category_languages, locale)

    if (predictor == "price_us_dollar") {
      return(list(
        display_name = if (effective_locale == "en") "Price (USD)" else "價格 (美金)",
        display_name_en = "Price (USD)",
        display_name_zh = "價格 (美金)",
        display_category = "product_attribute",
        effective_locale = effective_locale,
        display_description = if (effective_locale == "zh_TW") "產品價格（美金）" else "Product price in US dollars"
      ))
    } else if (predictor == "avg_price") {
      return(list(
        display_name = if (effective_locale == "en") "Average Price" else "平均價格",
        display_name_en = "Average Price",
        display_name_zh = "平均價格",
        display_category = "product_attribute",
        effective_locale = effective_locale,
        display_description = if (effective_locale == "zh_TW") "產品平均價格" else "Average product price"
      ))
    } else if (predictor == "original_price") {
      return(list(
        display_name = if (effective_locale == "en") "Original Price" else "原價",
        display_name_en = "Original Price",
        display_name_zh = "原價",
        display_category = "product_attribute",
        effective_locale = effective_locale,
        display_description = if (effective_locale == "zh_TW") "產品原價" else "Original product price"
      ))
    }
  }

  # Rating variables
  if (grepl("^(customer_ratings|rating)", predictor)) {
    # UI_P013: Determine effective language for product_attribute
    effective_locale <- get_category_language("product_attribute", category_languages, locale)

    return(list(
      display_name = if (effective_locale == "en") "Customer Ratings" else "顧客評分",
      display_name_en = "Customer Ratings",
      display_name_zh = "顧客評分",
      display_category = "product_attribute",
      effective_locale = effective_locale,
      display_description = if (effective_locale == "zh_TW") "顧客產品評分" else "Customer product ratings"
    ))
  }

  # Color options (e.g., color_options_藍色)
  if (grepl("^color_options_", predictor)) {
    color <- sub("^color_options_", "", predictor)
    display_zh <- paste0("顏色: ", color)
    display_en <- paste0("Color: ", color)

    # UI_P013: Determine effective language for product_attribute
    effective_locale <- get_category_language("product_attribute", category_languages, locale)

    return(list(
      display_name = if (effective_locale == "en") display_en else display_zh,
      display_name_en = display_en,
      display_name_zh = display_zh,
      display_category = "product_attribute",
      effective_locale = effective_locale,
      display_description = if (effective_locale == "zh_TW")
        paste0("產品顏色為", color)
      else
        paste0("Product color is ", color)
    ))
  }

  # Material (e.g., material_鋁合金)
  if (grepl("^material_", predictor)) {
    material <- sub("^material_", "", predictor)
    display_zh <- paste0("材質: ", material)
    display_en <- paste0("Material: ", material)

    # UI_P013: Determine effective language for product_attribute
    effective_locale <- get_category_language("product_attribute", category_languages, locale)

    return(list(
      display_name = if (effective_locale == "en") display_en else display_zh,
      display_name_en = display_en,
      display_name_zh = display_zh,
      display_category = "product_attribute",
      effective_locale = effective_locale,
      display_description = if (effective_locale == "zh_TW")
        paste0("產品材質為", material)
      else
        paste0("Product material is ", material)
    ))
  }

  # ==========================================================================
  # CATEGORY 3: Seller/Location Variables
  # ==========================================================================

  # Seller variables
  if (grepl("^seller_", predictor)) {
    seller <- sub("^seller_", "", predictor)
    # Capitalize seller name
    seller_display <- paste0(toupper(substring(seller, 1, 1)), substring(seller, 2))
    seller_display <- gsub("_", " ", seller_display)

    display_zh <- paste0("賣家: ", seller_display)
    display_en <- paste0("Seller: ", seller_display)

    # UI_P013: Determine effective language for seller
    effective_locale <- get_category_language("seller", category_languages, locale)

    return(list(
      display_name = if (effective_locale == "en") display_en else display_zh,
      display_name_en = display_en,
      display_name_zh = display_zh,
      display_category = "seller",
      effective_locale = effective_locale,
      display_description = if (effective_locale == "zh_TW")
        paste0("賣家為 ", seller_display)
      else
        paste0("Seller is ", seller_display)
    ))
  }

  # Location variables
  if (grepl("^location_", predictor)) {
    location <- sub("^location_", "", predictor)
    display_zh <- paste0("地點: ", location)
    display_en <- paste0("Location: ", location)

    # UI_P013: Determine effective language for location
    effective_locale <- get_category_language("location", category_languages, locale)

    return(list(
      display_name = if (effective_locale == "en") display_en else display_zh,
      display_name_en = display_en,
      display_name_zh = display_zh,
      display_category = "location",
      effective_locale = effective_locale,
      display_description = if (effective_locale == "zh_TW")
        paste0("銷售地點為", location)
      else
        paste0("Sales location is ", location)
    ))
  }

  # Nation variables
  if (grepl("^nation_", predictor)) {
    nation <- sub("^nation_", "", predictor)
    nation_map <- list(
      us = list(zh = "美國", en = "USA"),
      uk = list(zh = "英國", en = "UK"),
      cn = list(zh = "中國", en = "China"),
      tw = list(zh = "台灣", en = "Taiwan")
    )

    nation_display <- if (nation %in% names(nation_map)) {
      nation_map[[nation]]
    } else {
      list(zh = toupper(nation), en = toupper(nation))
    }

    display_zh <- paste0("國家: ", nation_display$zh)
    display_en <- paste0("Country: ", nation_display$en)

    # UI_P013: Determine effective language for location
    effective_locale <- get_category_language("location", category_languages, locale)

    return(list(
      display_name = if (effective_locale == "en") display_en else display_zh,
      display_name_en = display_en,
      display_name_zh = display_zh,
      display_category = "location",
      effective_locale = effective_locale,
      display_description = if (effective_locale == "zh_TW")
        paste0("銷售國家為", nation_display$zh)
      else
        paste0("Sales country is ", nation_display$en)
    ))
  }

  # ==========================================================================
  # CATEGORY 4: Derived/Binary Variables
  # ==========================================================================

  # Missing indicators
  if (grepl("_is_missing$", predictor)) {
    base_var <- sub("_is_missing$", "", predictor)
    base_display <- fn_generate_display_name(base_var, locale, category_languages = category_languages)

    display_zh <- paste0(base_display$display_name_zh, " 缺失")
    display_en <- paste0(base_display$display_name_en, " Missing")

    # UI_P013: Determine effective language for derived
    effective_locale <- get_category_language("derived", category_languages, locale)

    return(list(
      display_name = if (effective_locale == "en") display_en else display_zh,
      display_name_en = display_en,
      display_name_zh = display_zh,
      display_category = "derived",
      effective_locale = effective_locale,
      display_description = if (effective_locale == "zh_TW")
        paste0(base_display$display_name_zh, "資料缺失")
      else
        paste0(base_display$display_name_en, " data is missing")
    ))
  }

  # NA categorical levels
  if (grepl("_NA$", predictor)) {
    base_var <- sub("_NA$", "", predictor)
    base_display <- fn_generate_display_name(base_var, locale, category_languages = category_languages)

    display_zh <- paste0(base_display$display_name_zh, " 缺失")
    display_en <- paste0(base_display$display_name_en, " N/A")

    # UI_P013: Determine effective language for derived
    effective_locale <- get_category_language("derived", category_languages, locale)

    return(list(
      display_name = if (effective_locale == "en") display_en else display_zh,
      display_name_en = display_en,
      display_name_zh = display_zh,
      display_category = "derived",
      effective_locale = effective_locale,
      display_description = if (effective_locale == "zh_TW")
        paste0(base_display$display_name_zh, "類別缺失")
      else
        paste0(base_display$display_name_en, " category is N/A")
    ))
  }

  # Brand variables (brand.x, brand.y)
  if (grepl("^brand\\.", predictor)) {
    brand_letter <- sub("^brand\\.", "", predictor)
    display_zh <- paste0("品牌 ", toupper(brand_letter))
    display_en <- paste0("Brand ", toupper(brand_letter))

    # UI_P013: Determine effective language for product_attribute
    effective_locale <- get_category_language("product_attribute", category_languages, locale)

    return(list(
      display_name = if (effective_locale == "en") display_en else display_zh,
      display_name_en = display_en,
      display_name_zh = display_zh,
      display_category = "product_attribute",
      effective_locale = effective_locale,
      display_description = if (effective_locale == "zh_TW")
        paste0("產品品牌為", toupper(brand_letter))
      else
        paste0("Product brand is ", toupper(brand_letter))
    ))
  }

  # ==========================================================================
  # DEFAULT: AI Translation Fallback (if enabled) or Technical Name
  # ==========================================================================

  # If use_ai is TRUE and predictor is unknown, use AI translation
  if (use_ai) {
    message(
      "No rule-based translation found for '", predictor, "'. ",
      "Using AI translation fallback..."
    )

    # Source AI translation function
    ai_func_path <- file.path(
      dirname(dirname(rstudioapi::getActiveDocumentContext()$path)),
      "04_utils",
      "fn_translate_display_name_with_ai.R"
    )

    # Fallback path if RStudio API not available
    if (!file.exists(ai_func_path)) {
      ai_func_path <- file.path(
        "scripts", "global_scripts", "04_utils",
        "fn_translate_display_name_with_ai.R"
      )
    }

    if (file.exists(ai_func_path)) {
      source(ai_func_path)

      # Call AI translation
      ai_result <- tryCatch({
        fn_translate_display_name_with_ai(
          predictor = predictor,
          target_locale = locale,
          cache_con = cache_con
        )
      }, error = function(e) {
        warning("AI translation failed: ", e$message, "\nUsing fallback.")
        NULL
      })

      # If AI translation succeeded, return it
      if (!is.null(ai_result)) {
        # UI_P013: Determine effective language based on AI-detected category
        ai_category <- ai_result$display_category %||% "other"
        effective_locale <- get_category_language(ai_category, category_languages, locale)

        return(list(
          display_name = if (effective_locale == "en")
            ai_result$display_name_en
          else
            ai_result$display_name_zh,
          display_name_en = ai_result$display_name_en,
          display_name_zh = ai_result$display_name_zh,
          display_category = ai_category,
          effective_locale = effective_locale,
          display_description = if (effective_locale == "zh_TW") {
            paste0("AI翻譯: ", ai_result$display_name_zh)
          } else {
            paste0("AI-translated: ", ai_result$display_name_en)
          },
          translation_method = "ai_generated",  # Mark as AI-generated
          translation_metadata = ai_result$translation_metadata
        ))
      }
    } else {
      warning(
        "AI translation function not found at: ", ai_func_path, "\n",
        "Falling back to technical name formatting."
      )
    }
  }

  # ==========================================================================
  # FINAL FALLBACK: Return technical name with minimal formatting
  # ==========================================================================

  # Capitalize and replace underscores with spaces
  default_display <- gsub("_", " ", predictor)
  default_display <- paste0(toupper(substring(default_display, 1, 1)),
                           substring(default_display, 2))

  # UI_P013: Determine effective language for "other" category
  effective_locale <- get_category_language("other", category_languages, locale)

  return(list(
    display_name = if (effective_locale == "en") default_display else predictor,
    display_name_en = default_display,
    display_name_zh = predictor,
    display_category = "other",
    effective_locale = effective_locale,
    display_description = if (effective_locale == "zh_TW")
      paste0("變數: ", predictor)
    else
      paste0("Variable: ", predictor),
    translation_method = "fallback"  # Mark as fallback
  ))
}

# =============================================================================
# BATCH PROCESSING FUNCTION
# =============================================================================

fn_generate_all_display_names <- function(
  predictors,
  locale = "zh_TW",
  use_ai = FALSE,      # Enable AI fallback for batch processing
  cache_con = NULL,    # Database connection for AI caching
  category_languages = NULL  # UI_P013: Category language overrides
) {

  if (!requireNamespace("dplyr", quietly = TRUE)) library(dplyr)

  # Generate display names for all predictors
  results <- lapply(predictors, function(pred) {
    display_info <- fn_generate_display_name(
      pred,
      locale = locale,
      use_ai = use_ai,
      cache_con = cache_con,
      category_languages = category_languages
    )

    tibble::tibble(
      predictor = pred,
      display_name = display_info$display_name,
      display_name_en = display_info$display_name_en,
      display_name_zh = display_info$display_name_zh,
      display_category = display_info$display_category,
      effective_locale = display_info$effective_locale %||% locale,  # UI_P013
      display_description = display_info$display_description,
      translation_method = display_info$translation_method %||% "rule_based",
      metadata_source = "fn_generate_display_name",
      metadata_date = Sys.Date()
    )
  })

  # Combine into single tibble
  dplyr::bind_rows(results)
}

# Null coalescing operator
`%||%` <- function(a, b) if (is.null(a)) b else a
