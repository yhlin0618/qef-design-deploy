# =============================================================================
# FUNCTION: fn_translate_display_name_with_ai
# =============================================================================
# Purpose: Translate technical variable names to user-friendly display names
#          using OpenAI API for unknown variables
# Principle: DM_R046 - Variable Display Name Metadata Rule
#            MP054 - No Fake Data (AI translation is language conversion, not data fabrication)
#            MP123 - AI Prompt Configuration Management
# Layer: ETL/DRV - Generate metadata for unknown variables
# =============================================================================

#' Translate Display Name with AI
#'
#' Uses OpenAI API to translate technical variable names to user-friendly
#' display names when rule-based translation is not available.
#'
#' **Compliance with MP054 (No Fake Data)**:
#' - This function performs language translation, NOT data fabrication
#' - It converts technical names to human-readable format
#' - It does not create non-existent facts or data
#' - All translations are marked as "ai_generated" for review
#'
#' @param predictor Character. Technical variable name to translate.
#' @param target_locale Character. Target locale (default: "zh_TW").
#' @param cache_con DBI connection. Database connection for caching (optional).
#' @param api_key Character. OpenAI API key (default: Sys.getenv("OPENAI_API_KEY")).
#' @param model Character. OpenAI model to use (default: "gpt-5-nano-2025-08-07").
#' @param force_refresh Logical. Force API call even if cached (default: FALSE).
#'
#' @return List with display_name, display_name_en, display_name_zh,
#'         display_category, translation_method, and translation_metadata.
#'
#' @examples
#' \dontrun{
#' # Basic usage (no caching)
#' result <- fn_translate_display_name_with_ai(
#'   "customer_lifetime_value",
#'   target_locale = "zh_TW"
#' )
#' # Returns: list(display_name = "客戶終身價值", ...)
#'
#' # With caching
#' con <- dbConnect(duckdb::duckdb(), "app_data.duckdb")
#' result <- fn_translate_display_name_with_ai(
#'   "customer_churn_probability",
#'   target_locale = "zh_TW",
#'   cache_con = con
#' )
#' }
#'
#' @export
fn_translate_display_name_with_ai <- function(
  predictor,
  target_locale = "zh_TW",
  cache_con = NULL,
  api_key = Sys.getenv("OPENAI_API_KEY"),
  model = "gpt-5-nano-2025-08-07",
  force_refresh = FALSE
) {

  # ==========================================================================
  # VALIDATION: Check required parameters
  # ==========================================================================

  if (is.null(predictor) || is.na(predictor) || predictor == "") {
    stop("predictor cannot be NULL, NA, or empty string")
  }

  if (api_key == "") {
    stop(
      "OpenAI API key not found. ",
      "Please set OPENAI_API_KEY environment variable. ",
      "See MP123: AI Prompt Configuration Management"
    )
  }

  # ==========================================================================
  # STEP 1: Check cache (avoid redundant API calls)
  # ==========================================================================

  if (!is.null(cache_con) && !force_refresh) {
    tryCatch({
      # Query cache table
      query <- "
        SELECT display_name, display_name_en, display_name_zh,
               display_category, created_at, api_model
        FROM df_ai_translation_cache
        WHERE predictor = ? AND locale = ?
      "

      cached <- DBI::dbGetQuery(
        cache_con,
        query,
        params = list(predictor, target_locale)
      )

      if (nrow(cached) > 0) {
        message("Using cached translation for: ", predictor)
        return(list(
          display_name = cached$display_name[1],
          display_name_en = cached$display_name_en[1],
          display_name_zh = cached$display_name_zh[1],
          display_category = cached$display_category[1],
          translation_method = "ai_generated",
          translation_metadata = list(
            source = "cache",
            cached_at = cached$created_at[1],
            api_model = cached$api_model[1]
          )
        ))
      }
    }, error = function(e) {
      warning("Cache lookup failed: ", e$message, "\nProceeding with API call.")
    })
  }

  # ==========================================================================
  # STEP 2: Call OpenAI API for translation
  # ==========================================================================

  message("Calling OpenAI API to translate: ", predictor)

  # Load required packages
  if (!requireNamespace("httr2", quietly = TRUE)) library(httr2)
  if (!requireNamespace("jsonlite", quietly = TRUE)) library(jsonlite)
  if (!requireNamespace("glue", quietly = TRUE)) library(glue)

  # Build prompt based on target locale
  locale_instructions <- if (target_locale == "zh_TW") {
    "Use Traditional Chinese (繁體中文) characters"
  } else if (target_locale == "zh_CN") {
    "Use Simplified Chinese (简体中文) characters"
  } else if (target_locale == "en_US") {
    "Use English"
  } else {
    paste0("Use ", target_locale, " locale")
  }

  prompt <- glue::glue(
    "Translate the following technical variable name to a user-friendly ",
    "display name suitable for end users in a business analytics dashboard.\n\n",
    "Variable: {predictor}\n\n",
    "Requirements:\n",
    "1. Provide ONLY the translated display name (no explanations)\n",
    "2. {locale_instructions}\n",
    "3. Use natural language appropriate for non-technical users\n",
    "4. Keep it concise (maximum 20 characters)\n",
    "5. If the variable suggests a category (time/product/customer/etc), maintain that context\n",
    "6. Output format: Just the display name, nothing else\n\n",
    "Examples:\n",
    "- 'customer_lifetime_value' → '客戶終身價值' (for zh_TW)\n",
    "- 'month_5' → '5月' (for zh_TW)\n",
    "- 'price_us_dollar' → '價格 (美金)' (for zh_TW)"
  )

  # Define rate limiting handler (from fn_rate_comments.R pattern)
  openai_after <- function(resp) {
    retry_after <- httr2::resp_header(resp, "Retry-After")
    if (!is.na(retry_after)) {
      return(as.numeric(retry_after))
    }
    reset_unix <- httr2::resp_header(resp, "x-ratelimit-reset-requests")
    if (!is.na(reset_unix)) {
      return(as.numeric(reset_unix) - unclass(Sys.time()))
    }
    NA  # Default to backoff
  }

  # Create and send API request
  req <- httr2::request("https://api.openai.com/v1/chat/completions") |>
    httr2::req_auth_bearer_token(api_key) |>
    httr2::req_body_json(list(
      model = model,
      messages = list(
        list(
          role = "system",
          content = "You are a professional translator specializing in technical data science terminology. You provide concise, accurate translations for variable names in analytics dashboards."
        ),
        list(role = "user", content = prompt)
      ),
      max_tokens = 50,
      temperature = 0.3  # Lower temperature for more consistent translations
    )) |>
    httr2::req_retry(
      is_transient = \(resp) httr2::resp_status(resp) %in% c(429, 500:599),
      after = openai_after,
      max_tries = 3
    ) |>
    httr2::req_timeout(30)  # 30 second timeout

  # Execute request with error handling
  tryCatch({
    resp <- httr2::req_perform(req)
    result <- httr2::resp_body_json(resp)

    # Extract display name
    display_name_translated <- trimws(result$choices[[1]]$message$content)

    # Remove any quotation marks that might be included
    display_name_translated <- gsub("^['\"]|['\"]$", "", display_name_translated)

  }, error = function(e) {
    warning("OpenAI API call failed: ", e$message)
    # Fallback to technical name with formatting
    display_name_translated <<- gsub("_", " ", predictor)
    display_name_translated <<- paste0(
      toupper(substring(display_name_translated, 1, 1)),
      substring(display_name_translated, 2)
    )
  })

  # ==========================================================================
  # STEP 3: Generate bilingual versions
  # ==========================================================================

  # If zh_TW was requested, also get English version (and vice versa)
  display_name_en <- if (target_locale == "zh_TW") {
    # Call again for English
    tryCatch({
      en_req <- httr2::request("https://api.openai.com/v1/chat/completions") |>
        httr2::req_auth_bearer_token(api_key) |>
        httr2::req_body_json(list(
          model = model,
          messages = list(
            list(
              role = "system",
              content = "You are a professional translator. Provide only the English translation."
            ),
            list(
              role = "user",
              content = glue::glue(
                "Translate '{predictor}' to English display name (max 20 chars):"
              )
            )
          ),
          max_tokens = 30,
          temperature = 0.3
        )) |>
        httr2::req_timeout(20)

      en_resp <- httr2::req_perform(en_req)
      en_result <- httr2::resp_body_json(en_resp)
      en_name <- trimws(en_result$choices[[1]]$message$content)
      gsub("^['\"]|['\"]$", "", en_name)
    }, error = function(e) {
      # Fallback
      gsub("_", " ", predictor) |>
        (\(x) paste0(toupper(substring(x, 1, 1)), substring(x, 2)))()
    })
  } else {
    display_name_translated
  }

  display_name_zh <- if (target_locale == "zh_TW") {
    display_name_translated
  } else {
    predictor  # Fallback to technical name
  }

  # ==========================================================================
  # STEP 4: Infer category from variable name
  # ==========================================================================

  display_category <- if (grepl("^(month|day|week|year|quarter|date|time)", predictor)) {
    "time"
  } else if (grepl("^(price|cost|revenue|sales|amount)", predictor)) {
    "product_attribute"
  } else if (grepl("^(customer|user|client)", predictor)) {
    "customer"
  } else if (grepl("^(seller|vendor|supplier)", predictor)) {
    "seller"
  } else if (grepl("^(location|region|country|city|nation)", predictor)) {
    "location"
  } else {
    "other"
  }

  # ==========================================================================
  # STEP 5: Save to cache (if connection provided)
  # ==========================================================================

  if (!is.null(cache_con)) {
    tryCatch({
      # Ensure cache table exists
      if (!DBI::dbExistsTable(cache_con, "df_ai_translation_cache")) {
        DBI::dbExecute(cache_con, "
          CREATE TABLE df_ai_translation_cache (
            predictor VARCHAR PRIMARY KEY,
            locale VARCHAR NOT NULL,
            display_name VARCHAR NOT NULL,
            display_name_en VARCHAR,
            display_name_zh VARCHAR,
            display_category VARCHAR,
            translation_method VARCHAR DEFAULT 'ai_generated',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            api_model VARCHAR,
            api_cost DOUBLE,
            is_approved BOOLEAN DEFAULT FALSE,
            UNIQUE(predictor, locale)
          )
        ")
      }

      # Insert or replace
      DBI::dbExecute(
        cache_con,
        "INSERT OR REPLACE INTO df_ai_translation_cache
         (predictor, locale, display_name, display_name_en, display_name_zh,
          display_category, translation_method, api_model, created_at)
         VALUES (?, ?, ?, ?, ?, ?, 'ai_generated', ?, ?)",
        params = list(
          predictor,
          target_locale,
          if (target_locale == "zh_TW") display_name_zh else display_name_en,
          display_name_en,
          display_name_zh,
          display_category,
          model,
          Sys.time()
        )
      )

      message("Translation cached for: ", predictor)
    }, error = function(e) {
      warning("Failed to cache translation: ", e$message)
    })
  }

  # ==========================================================================
  # STEP 6: Return result with metadata
  # ==========================================================================

  return(list(
    display_name = if (target_locale == "zh_TW") display_name_zh else display_name_en,
    display_name_en = display_name_en,
    display_name_zh = display_name_zh,
    display_category = display_category,
    translation_method = "ai_generated",
    translation_metadata = list(
      source = "openai_api",
      api_model = model,
      locale = target_locale,
      timestamp = Sys.time(),
      is_approved = FALSE  # Requires human review
    )
  ))
}

# =============================================================================
# HELPER FUNCTION: Batch Translation
# =============================================================================

#' Batch Translate Display Names with AI
#'
#' Translates multiple predictor names in batch, with progress reporting.
#'
#' @param predictors Character vector. Technical variable names to translate.
#' @param target_locale Character. Target locale (default: "zh_TW").
#' @param cache_con DBI connection. Database connection for caching (optional).
#' @param show_progress Logical. Show progress bar (default: TRUE).
#'
#' @return Tibble with columns: predictor, display_name, display_name_en,
#'         display_name_zh, display_category, translation_method.
#'
#' @export
fn_translate_display_names_batch <- function(
  predictors,
  target_locale = "zh_TW",
  cache_con = NULL,
  show_progress = TRUE
) {

  if (!requireNamespace("dplyr", quietly = TRUE)) library(dplyr)
  if (!requireNamespace("purrr", quietly = TRUE)) library(purrr)

  if (show_progress && requireNamespace("progress", quietly = TRUE)) {
    pb <- progress::progress_bar$new(
      format = "  Translating [:bar] :percent eta: :eta",
      total = length(predictors),
      clear = FALSE
    )
  }

  # Translate each predictor
  results <- purrr::map_dfr(predictors, function(pred) {
    if (show_progress && exists("pb")) pb$tick()

    result <- fn_translate_display_name_with_ai(
      pred,
      target_locale = target_locale,
      cache_con = cache_con
    )

    tibble::tibble(
      predictor = pred,
      display_name = result$display_name,
      display_name_en = result$display_name_en,
      display_name_zh = result$display_name_zh,
      display_category = result$display_category,
      translation_method = result$translation_method,
      metadata_source = "fn_translate_display_name_with_ai",
      metadata_date = Sys.Date()
    )
  })

  return(results)
}
