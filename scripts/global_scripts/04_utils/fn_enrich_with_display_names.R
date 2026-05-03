# =============================================================================
# FUNCTION: fn_enrich_with_display_names
# =============================================================================
# Purpose: Enrich Poisson analysis results with user-friendly display names
# Principle: DM_R046 - Variable Display Name Metadata Rule
#            UI_P013 - Category-Level Language Override
# Layer: DRV (3) - Enrich analysis results before UI consumption
# Version: 1.3 (2025-12-26) - Simplified to use fn_generate_all_display_names
# =============================================================================

fn_enrich_with_display_names <- function(analysis_results,
                                         con = NULL,
                                         metadata_table = "tbl_variable_display_names",
                                         locale = "zh_TW",
                                         category_languages = NULL) {

  # UI_P013: Load category_languages if not provided
  if (is.null(category_languages)) {
    # Try 1: Load from app_configs in global environment
    if (exists("app_configs", envir = globalenv())) {
      app_configs <- get("app_configs", envir = globalenv())
      if (!is.null(app_configs$localization$category_languages)) {
        category_languages <- app_configs$localization$category_languages
        message("[UI_P013] Loaded category_languages from app_configs: ",
                paste(names(category_languages), "=", unlist(category_languages), collapse = ", "))
      }
    }
    # Try 2: Load directly from app_config.yaml (for DRV scripts)
    if (is.null(category_languages) && file.exists("app_config.yaml")) {
      app_config <- yaml::read_yaml("app_config.yaml")
      if (!is.null(app_config$localization$category_languages)) {
        category_languages <- app_config$localization$category_languages
        message("[UI_P013] Loaded category_languages from app_config.yaml: ",
                paste(names(category_languages), "=", unlist(category_languages), collapse = ", "))
      }
    }
  }

  # Load required packages
  require(dplyr)
  require(tibble)

  # ==========================================================================
  # STEP 1: Generate Display Names (Pattern Matching + UI_P013)
  # ==========================================================================
  # fn_generate_display_name.R handles:
  # - Pattern extraction: "seller_nameEiji" → "seller_name"
  # - Database lookup: Queries df_metadata_turbo for translations
  # - Category language override (UI_P013): product_attribute → "en"
  # - AI translation fallback (if enabled)

  # Get unique predictors from analysis results
  unique_predictors <- unique(analysis_results$predictor)

  # Load display name generator
  source("scripts/global_scripts/04_utils/fn_generate_display_name.R")

  # Generate display names with:
  # - cache_con: Database connection for metadata lookup (df_metadata_turbo)
  # - category_languages: Per-category language overrides (UI_P013)
  display_names <- fn_generate_all_display_names(
    unique_predictors,
    locale = locale,
    use_ai = FALSE,
    cache_con = con,  # Pass database connection for metadata lookup
    category_languages = category_languages  # UI_P013
  )

  message("[DM_R046] Generated ", nrow(display_names), " display names")

  # ==========================================================================
  # STEP 3: Enrich Analysis Results with Display Names
  # ==========================================================================

  # Define expected columns for join
  join_columns <- c("predictor", "display_name", "display_name_en", "display_name_zh",
                    "display_category", "effective_locale", "display_description")

  enriched_results <- analysis_results %>%
    left_join(
      display_names %>%
        select(any_of(join_columns)),  # UI_P013: Use any_of() for safe column selection
      by = "predictor"
    )

  # ==========================================================================
  # STEP 4: Handle Missing Display Names (Fallback)
  # ==========================================================================

  # Count rows with missing display names
  missing_count <- sum(is.na(enriched_results$display_name))

  if (missing_count > 0) {
    warning("[DM_R046] ", missing_count,
            " predictors missing display names. Using fallback (predictor name).")

    # Fallback: Use predictor name with basic formatting
    enriched_results <- enriched_results %>%
      mutate(
        display_name = dplyr::coalesce(display_name, predictor),
        display_name_en = dplyr::coalesce(display_name_en, predictor),
        display_name_zh = dplyr::coalesce(display_name_zh, predictor),
        display_category = dplyr::coalesce(display_category, "other"),
        effective_locale = dplyr::coalesce(effective_locale, locale),  # UI_P013
        display_description = dplyr::coalesce(display_description,
                                       paste0("Variable: ", predictor))
      )
  }

  # ==========================================================================
  # STEP 5: Validation
  # ==========================================================================

  # Verify all rows have display names
  final_missing <- sum(is.na(enriched_results$display_name))

  if (final_missing > 0) {
    stop("[DM_R046] CRITICAL: ", final_missing,
         " rows still missing display_name after fallback. This should never happen!")
  }

  # Log summary
  message("[DM_R046] Enrichment complete:")
  message("  - Total rows: ", nrow(enriched_results))
  message("  - Unique predictors: ", length(unique(enriched_results$predictor)))
  message("  - Display names populated: ", sum(!is.na(enriched_results$display_name)))
  message("  - Categories: ", paste(unique(enriched_results$display_category), collapse = ", "))

  # ==========================================================================
  # STEP 6: Return Enriched Results
  # ==========================================================================

  return(enriched_results)
}

# =============================================================================
# VALIDATION FUNCTION
# =============================================================================

fn_validate_display_name_enrichment <- function(enriched_data) {

  # Check 1: display_name column exists
  if (!"display_name" %in% colnames(enriched_data)) {
    return(list(
      valid = FALSE,
      error = "Missing required column: display_name"
    ))
  }

  # Check 2: display_category column exists
  if (!"display_category" %in% colnames(enriched_data)) {
    return(list(
      valid = FALSE,
      error = "Missing required column: display_category"
    ))
  }

  # Check 3: No NULL display names
  null_count <- sum(is.na(enriched_data$display_name))
  if (null_count > 0) {
    return(list(
      valid = FALSE,
      error = paste0(null_count, " rows have NULL display_name")
    ))
  }

  # Check 4: No NULL display categories
  null_cat_count <- sum(is.na(enriched_data$display_category))
  if (null_cat_count > 0) {
    return(list(
      valid = FALSE,
      error = paste0(null_cat_count, " rows have NULL display_category")
    ))
  }

  # Check 5: Valid category values
  valid_categories <- c("time", "product_attribute", "seller", "location",
                       "derived", "other")
  invalid_categories <- setdiff(unique(enriched_data$display_category),
                                valid_categories)

  if (length(invalid_categories) > 0) {
    return(list(
      valid = FALSE,
      error = paste0("Invalid categories found: ",
                    paste(invalid_categories, collapse = ", "))
    ))
  }

  # All checks passed
  return(list(
    valid = TRUE,
    message = "DM_R046 validation passed",
    summary = list(
      total_rows = nrow(enriched_data),
      unique_predictors = length(unique(enriched_data$predictor)),
      categories = table(enriched_data$display_category)
    )
  ))
}

# =============================================================================
# USAGE EXAMPLE
# =============================================================================

# Example usage in DRV script:
#
# # Load Poisson analysis results (without display names)
# poisson_results <- dbReadTable(con, "df_cbz_poisson_analysis_alf")
#
# # Enrich with display names
# source("scripts/global_scripts/04_utils/fn_enrich_with_display_names.R")
# poisson_enriched <- fn_enrich_with_display_names(
#   poisson_results,
#   con = con,
#   metadata_table = "tbl_variable_display_names",
#   locale = "zh_TW"
# )
#
# # Validate enrichment
# validation <- fn_validate_display_name_enrichment(poisson_enriched)
# if (!validation$valid) {
#   stop("DM_R046 VIOLATION: ", validation$error)
# }
#
# # Write enriched results back to database
# dbWriteTable(con, "df_cbz_poisson_analysis_alf", poisson_enriched,
#              overwrite = TRUE)
