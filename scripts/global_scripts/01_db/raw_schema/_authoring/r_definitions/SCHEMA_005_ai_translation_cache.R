# =============================================================================
# SCHEMA DEFINITION: AI Translation Cache
# =============================================================================
# Schema ID: SCHEMA_005
# Table: df_ai_translation_cache
# Purpose: Cache AI-generated display name translations
# Principle: DM_R046 - Variable Display Name Metadata Rule
#            MP054 - No Fake Data (AI translation is language conversion)
#            MP123 - AI Prompt Configuration Management
# Created: 2025-11-14
# =============================================================================

SCHEMA_005_ai_translation_cache <- list(
  schema_id = "SCHEMA_005",
  schema_name = "AI Translation Cache",
  table_name = "df_ai_translation_cache",
  description = "Caches AI-generated translations of technical variable names to user-friendly display names",
  version = "1.0",
  created_date = "2025-11-14",
  last_updated = "2025-11-14",

  # ============================================================================
  # COLUMN DEFINITIONS
  # ============================================================================

  columns = list(

    # ---- PRIMARY KEY ----
    predictor = list(
      type = "VARCHAR",
      required = TRUE,
      primary_key = TRUE,
      description = "Technical variable name (e.g., 'customer_lifetime_value')",
      example = "customer_churn_probability",
      validation = "Non-empty string, no spaces"
    ),

    # ---- LOCALE IDENTIFIER ----
    locale = list(
      type = "VARCHAR",
      required = TRUE,
      description = "Target locale for translation",
      example = "zh_TW",
      valid_values = c("zh_TW", "zh_CN", "en_US", "ja_JP", "ko_KR"),
      note = "Combination of (predictor, locale) must be unique"
    ),

    # ---- DISPLAY NAMES ----
    display_name = list(
      type = "VARCHAR",
      required = TRUE,
      description = "User-friendly display name in target locale",
      example = "客戶流失機率",
      note = "This is the primary user-facing label"
    ),

    display_name_en = list(
      type = "VARCHAR",
      required = FALSE,
      description = "English display name (for reference)",
      example = "Customer Churn Probability"
    ),

    display_name_zh = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Traditional Chinese display name",
      example = "客戶流失機率"
    ),

    # ---- CATEGORIZATION ----
    display_category = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Variable category for UI grouping",
      example = "customer",
      valid_values = c(
        "time", "product_attribute", "customer", "seller",
        "location", "derived", "other"
      )
    ),

    # ---- TRANSLATION METADATA ----
    translation_method = list(
      type = "VARCHAR",
      required = TRUE,
      default = "ai_generated",
      description = "Method used to generate translation",
      valid_values = c("ai_generated", "manual_override"),
      note = "Always 'ai_generated' for this cache table"
    ),

    # ---- TIMESTAMPS ----
    created_at = list(
      type = "TIMESTAMP",
      required = TRUE,
      default = "CURRENT_TIMESTAMP",
      description = "When the translation was first generated"
    ),

    updated_at = list(
      type = "TIMESTAMP",
      required = FALSE,
      description = "When the translation was last updated (if manually overridden)"
    ),

    # ---- API METADATA ----
    api_model = list(
      type = "VARCHAR",
      required = FALSE,
      description = "OpenAI model used for translation",
      example = "gpt-5-nano",
      note = "Track which model generated the translation"
    ),

    api_cost = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Estimated API cost for this translation (in USD)",
      example = 0.0001,
      note = "Optional cost tracking for budget management"
    ),

    # ---- REVIEW STATUS ----
    is_approved = list(
      type = "BOOLEAN",
      required = TRUE,
      default = FALSE,
      description = "Has a human reviewer approved this translation?",
      note = "All AI translations require human review before full trust"
    ),

    approved_by = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Username of person who approved the translation",
      example = "che@mambatek.com"
    ),

    approved_at = list(
      type = "TIMESTAMP",
      required = FALSE,
      description = "When the translation was approved"
    ),

    # ---- USAGE STATISTICS ----
    usage_count = list(
      type = "INTEGER",
      required = FALSE,
      default = 0,
      description = "Number of times this translation has been retrieved from cache",
      note = "Helps identify high-frequency variables needing review"
    ),

    last_used_at = list(
      type = "TIMESTAMP",
      required = FALSE,
      description = "When this cached translation was last accessed"
    )
  ),

  # ============================================================================
  # INDEXES
  # ============================================================================

  indexes = list(
    idx_locale = list(
      columns = "locale",
      description = "Speed up queries filtering by locale"
    ),

    idx_approval_status = list(
      columns = c("is_approved", "created_at"),
      description = "Find unapproved translations needing review"
    ),

    idx_usage = list(
      columns = c("usage_count", "last_used_at"),
      description = "Identify frequently used translations"
    )
  ),

  # ============================================================================
  # CONSTRAINTS
  # ============================================================================

  constraints = list(
    unique_predictor_locale = list(
      type = "UNIQUE",
      columns = c("predictor", "locale"),
      description = "Each predictor-locale pair can only have one cached translation"
    )
  ),

  # ============================================================================
  # CREATE TABLE SQL (DuckDB)
  # ============================================================================

  create_sql_duckdb = "
    CREATE TABLE IF NOT EXISTS df_ai_translation_cache (
      predictor VARCHAR PRIMARY KEY,
      locale VARCHAR NOT NULL,
      display_name VARCHAR NOT NULL,
      display_name_en VARCHAR,
      display_name_zh VARCHAR,
      display_category VARCHAR,
      translation_method VARCHAR DEFAULT 'ai_generated',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP,
      api_model VARCHAR,
      api_cost DOUBLE,
      is_approved BOOLEAN DEFAULT FALSE,
      approved_by VARCHAR,
      approved_at TIMESTAMP,
      usage_count INTEGER DEFAULT 0,
      last_used_at TIMESTAMP,
      UNIQUE(predictor, locale)
    );

    -- Create indexes
    CREATE INDEX IF NOT EXISTS idx_locale ON df_ai_translation_cache(locale);
    CREATE INDEX IF NOT EXISTS idx_approval_status ON df_ai_translation_cache(is_approved, created_at);
    CREATE INDEX IF NOT EXISTS idx_usage ON df_ai_translation_cache(usage_count DESC, last_used_at DESC);
  ",

  # ============================================================================
  # CREATE TABLE SQL (PostgreSQL)
  # ============================================================================

  create_sql_postgres = "
    CREATE TABLE IF NOT EXISTS df_ai_translation_cache (
      predictor VARCHAR PRIMARY KEY,
      locale VARCHAR NOT NULL,
      display_name VARCHAR NOT NULL,
      display_name_en VARCHAR,
      display_name_zh VARCHAR,
      display_category VARCHAR,
      translation_method VARCHAR DEFAULT 'ai_generated',
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP,
      api_model VARCHAR,
      api_cost DOUBLE PRECISION,
      is_approved BOOLEAN DEFAULT FALSE,
      approved_by VARCHAR,
      approved_at TIMESTAMP,
      usage_count INTEGER DEFAULT 0,
      last_used_at TIMESTAMP,
      UNIQUE(predictor, locale)
    );

    -- Create indexes
    CREATE INDEX IF NOT EXISTS idx_locale ON df_ai_translation_cache(locale);
    CREATE INDEX IF NOT EXISTS idx_approval_status ON df_ai_translation_cache(is_approved, created_at);
    CREATE INDEX IF NOT EXISTS idx_usage ON df_ai_translation_cache(usage_count DESC, last_used_at DESC);
  ",

  # ============================================================================
  # VALIDATION QUERIES
  # ============================================================================

  validation_queries = list(

    # Find translations needing review
    unapproved_translations = "
      SELECT predictor, locale, display_name, created_at,
             DATEDIFF('day', created_at, CURRENT_TIMESTAMP) as days_pending
      FROM df_ai_translation_cache
      WHERE is_approved = FALSE
      ORDER BY usage_count DESC, created_at ASC
      LIMIT 50
    ",

    # Find high-usage unapproved translations (priority review)
    high_priority_review = "
      SELECT predictor, locale, display_name, usage_count, created_at
      FROM df_ai_translation_cache
      WHERE is_approved = FALSE
        AND usage_count > 100
      ORDER BY usage_count DESC
    ",

    # Cache hit rate analysis
    cache_statistics = "
      SELECT
        COUNT(*) as total_cached,
        COUNT(CASE WHEN is_approved THEN 1 END) as approved_count,
        SUM(usage_count) as total_retrievals,
        AVG(usage_count) as avg_usage,
        COUNT(DISTINCT locale) as locales_count
      FROM df_ai_translation_cache
    "
  ),

  # ============================================================================
  # MAINTENANCE OPERATIONS
  # ============================================================================

  maintenance_operations = list(

    # Approve a translation
    approve_translation = function(con, predictor, locale, reviewer_email) {
      DBI::dbExecute(
        con,
        "UPDATE df_ai_translation_cache
         SET is_approved = TRUE,
             approved_by = ?,
             approved_at = CURRENT_TIMESTAMP
         WHERE predictor = ? AND locale = ?",
        params = list(reviewer_email, predictor, locale)
      )
    },

    # Override translation manually
    override_translation = function(con, predictor, locale, new_display_name, reviewer_email) {
      DBI::dbExecute(
        con,
        "UPDATE df_ai_translation_cache
         SET display_name = ?,
             translation_method = 'manual_override',
             is_approved = TRUE,
             approved_by = ?,
             approved_at = CURRENT_TIMESTAMP,
             updated_at = CURRENT_TIMESTAMP
         WHERE predictor = ? AND locale = ?",
        params = list(new_display_name, reviewer_email, predictor, locale)
      )
    },

    # Increment usage count
    increment_usage = function(con, predictor, locale) {
      DBI::dbExecute(
        con,
        "UPDATE df_ai_translation_cache
         SET usage_count = usage_count + 1,
             last_used_at = CURRENT_TIMESTAMP
         WHERE predictor = ? AND locale = ?",
        params = list(predictor, locale)
      )
    }
  ),

  # ============================================================================
  # COMPLIANCE NOTES
  # ============================================================================

  compliance_notes = list(
    mp054_compliance = paste0(
      "This table complies with MP054 (No Fake Data) because:\n",
      "1. It stores TRANSLATIONS of existing technical names, not fabricated data\n",
      "2. It converts technical terminology to user-friendly language\n",
      "3. All translations are marked as 'ai_generated' for transparency\n",
      "4. Human review is required (is_approved flag)\n",
      "5. Translation method is tracked for audit trail"
    ),

    dm_r046_compliance = paste0(
      "This table supports DM_R046 by:\n",
      "1. Providing cached translations for display name generation\n",
      "2. Avoiding redundant API calls (cost optimization)\n",
      "3. Ensuring consistency across application\n",
      "4. Enabling human review workflow\n",
      "5. Tracking translation quality and usage"
    )
  )
)

# =============================================================================
# HELPER FUNCTION: Initialize Cache Table
# =============================================================================

#' Initialize AI Translation Cache Table
#'
#' Creates the df_ai_translation_cache table if it doesn't exist.
#'
#' @param con DBI connection to database.
#' @param db_type Character. Database type ("duckdb" or "postgres").
#'
#' @return TRUE if successful, error otherwise.
#' @export
fn_initialize_ai_translation_cache <- function(con, db_type = "duckdb") {

  if (db_type == "duckdb") {
    sql <- SCHEMA_005_ai_translation_cache$create_sql_duckdb
  } else if (db_type == "postgres") {
    sql <- SCHEMA_005_ai_translation_cache$create_sql_postgres
  } else {
    stop("Unsupported database type: ", db_type)
  }

  DBI::dbExecute(con, sql)
  message("AI translation cache table initialized successfully.")
  return(TRUE)
}
