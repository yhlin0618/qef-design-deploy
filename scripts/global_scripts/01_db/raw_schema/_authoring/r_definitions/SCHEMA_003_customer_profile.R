# =============================================================================
# SCHEMA_003: Customer Profile Table Definition
# =============================================================================
# Purpose: Define the standard schema for customer profile data
# Used by: WISER, MAMBA applications for customer analysis
# Critical: This table stores basic customer identification data
# =============================================================================

# Schema Definition
SCHEMA_customer_profile <- list(

  table_name = "df_profile_by_customer",

  description = "Stores basic customer profile and identification data",

  columns = list(

    # =========================================================================
    # IDENTIFIER COLUMNS (Required)
    # =========================================================================

    customer_id = list(
      type = "VARCHAR",
      required = TRUE,
      description = "Unique customer identifier",
      example = "CUST_001234",
      unique = TRUE
    ),

    buyer_name = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Customer's buyer name (may be anonymized)",
      example = "john_doe_123",
      sensitive = TRUE
    ),

    email = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Customer email address (may be hashed)",
      example = "hash_abc123def456",
      sensitive = TRUE,
      format = "email or hash"
    ),

    platform_id = list(
      type = "VARCHAR",
      required = TRUE,
      description = "Platform identifier (e.g., 'ebay', 'amazon')",
      example = "ebay"
    ),

    display_name = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Display name for customer (public-facing)",
      example = "J***e"
    )
  ),

  # =========================================================================
  # TABLE CONSTRAINTS
  # =========================================================================

  constraints = list(
    primary_key = "customer_id",

    indexes = c("platform_id", "email"),

    unique_constraints = list(
      customer_unique = "customer_id must be unique",
      platform_customer = c("platform_id", "buyer_name") # unique per platform
    )
  ),

  # =========================================================================
  # DATA QUALITY RULES
  # =========================================================================

  quality_rules = list(
    customer_id_format = "customer_id should follow consistent format",
    email_validation = "email should be valid format or properly hashed",
    platform_values = "platform_id should be from allowed list",
    pii_handling = "PII fields should be properly anonymized or hashed"
  ),

  # =========================================================================
  # USAGE EXAMPLES
  # =========================================================================

  examples = list(

    create_table = '
    CREATE TABLE df_profile_by_customer (
      customer_id VARCHAR PRIMARY KEY,
      buyer_name VARCHAR,
      email VARCHAR,
      platform_id VARCHAR NOT NULL,
      display_name VARCHAR
    )',

    query_platform_customers = "
    SELECT platform_id, COUNT(*) as customer_count
    FROM df_profile_by_customer
    GROUP BY platform_id
    ORDER BY customer_count DESC
    ",

    join_with_dna = "
    SELECT
      cp.customer_id,
      cp.platform_id,
      cp.display_name,
      dna.total_spent,
      dna.f_value,
      dna.r_value,
      dna.m_value
    FROM df_profile_by_customer cp
    LEFT JOIN df_dna_by_customer dna
      ON cp.customer_id = dna.customer_id
    "
  )
)

# =============================================================================
# SCHEMA_003B: Customer DNA Analysis Table Definition
# =============================================================================

SCHEMA_customer_dna <- list(

  table_name = "df_dna_by_customer",

  description = "Stores customer behavioral DNA metrics (RFM and advanced metrics)",

  columns = list(

    # =========================================================================
    # IDENTIFIER COLUMNS
    # =========================================================================

    customer_id = list(
      type = "VARCHAR",
      required = TRUE,
      description = "Customer identifier (links to df_profile_by_customer)",
      example = "CUST_001234"
    ),

    platform_id = list(
      type = "VARCHAR",
      required = TRUE,
      description = "Platform identifier",
      example = "ebay"
    ),

    # =========================================================================
    # RFM CORE METRICS
    # =========================================================================

    r_value = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Recency value - days since last purchase",
      example = 15
    ),

    f_value = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Frequency value - number of purchases",
      example = 5
    ),

    m_value = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Monetary value - average purchase amount",
      example = 250.50
    ),

    r_ecdf = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Recency empirical CDF percentile (0-1)",
      example = 0.85
    ),

    f_ecdf = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Frequency empirical CDF percentile (0-1)",
      example = 0.72
    ),

    m_ecdf = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Monetary empirical CDF percentile (0-1)",
      example = 0.68
    ),

    r_label = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Recency category label",
      example = "recent"
    ),

    f_label = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Frequency category label",
      example = "frequent"
    ),

    m_label = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Monetary category label",
      example = "high_value"
    ),

    # =========================================================================
    # PURCHASE BEHAVIOR METRICS
    # =========================================================================

    ipt = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Inter-purchase time (average days between purchases)",
      example = 30.5
    ),

    ipt_mean = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Mean inter-purchase time",
      example = 28.3
    ),

    total_spent = list(
      type = "DOUBLE",
      required = TRUE,
      description = "Total amount spent by customer",
      example = 1252.50
    ),

    times = list(
      type = "INTEGER",
      required = TRUE,
      description = "Number of transactions",
      example = 5
    ),

    # =========================================================================
    # LOCATION DATA
    # =========================================================================

    zipcode = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Customer zipcode",
      example = "10001"
    ),

    state = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Customer state/region",
      example = "NY"
    ),

    lat = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Latitude coordinate",
      example = 40.7128
    ),

    lng = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Longitude coordinate",
      example = -74.0060
    ),

    # =========================================================================
    # ADVANCED METRICS
    # =========================================================================

    clv = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Customer Lifetime Value prediction",
      example = 5000.00
    ),

    cai = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Customer Activity Index",
      example = 0.75
    ),

    cai_ecdf = list(
      type = "DOUBLE",
      required = FALSE,
      description = "CAI empirical CDF percentile",
      example = 0.80
    ),

    cai_label = list(
      type = "VARCHAR",
      required = FALSE,
      description = "CAI category label",
      example = "highly_active"
    ),

    pcv = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Predicted Customer Value",
      example = 3500.00
    ),

    cri = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Customer Risk Index",
      example = 0.25
    ),

    cri_ecdf = list(
      type = "DOUBLE",
      required = FALSE,
      description = "CRI empirical CDF percentile",
      example = 0.30
    ),

    # =========================================================================
    # STATISTICAL METRICS
    # =========================================================================

    sigma_hnorm_mle = list(
      type = "DOUBLE",
      required = FALSE,
      description = "MLE estimate of sigma (halfnorm distribution)",
      example = 15.2
    ),

    sigma_hnorm_bcmle = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Bias-corrected MLE of sigma",
      example = 14.8
    ),

    mle = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Maximum likelihood estimate",
      example = 0.65
    ),

    wmle = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Weighted maximum likelihood estimate",
      example = 0.68
    ),

    # =========================================================================
    # TIME-BASED METRICS
    # =========================================================================

    time_first = list(
      type = "DATE",
      required = FALSE,
      description = "Date of first purchase",
      example = "2024-01-15"
    ),

    time_first_to_now = list(
      type = "INTEGER",
      required = FALSE,
      description = "Days since first purchase",
      example = 350
    ),

    min_time = list(
      type = "DATE",
      required = FALSE,
      description = "Earliest transaction date",
      example = "2024-01-15"
    ),

    payment_time = list(
      type = "TIMESTAMP",
      required = FALSE,
      description = "Last payment timestamp",
      example = "2025-01-10 14:30:00"
    ),

    difftime = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Time difference metric",
      example = 45.5
    ),

    # =========================================================================
    # ENGAGEMENT METRICS
    # =========================================================================

    nes_ratio = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Net Engagement Score ratio",
      example = 0.82
    ),

    nes_status = list(
      type = "VARCHAR",
      required = FALSE,
      description = "Net Engagement Status",
      example = "engaged"
    ),

    nrec = list(
      type = "INTEGER",
      required = FALSE,
      description = "Number of recommendations",
      example = 12
    ),

    nrec_prob = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Recommendation probability",
      example = 0.75
    ),

    # =========================================================================
    # BEHAVIORAL INDICES
    # =========================================================================

    e0t = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Expected time to next event",
      example = 25.5
    ),

    ge = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Growth engagement index",
      example = 1.15
    ),

    ie = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Interaction engagement index",
      example = 0.88
    ),

    be = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Behavioral engagement index",
      example = 0.92
    ),

    be2 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Behavioral engagement index v2",
      example = 0.94
    ),

    # =========================================================================
    # AGGREGATION COLUMNS
    # =========================================================================

    ni = list(
      type = "INTEGER",
      required = FALSE,
      description = "Number of items purchased",
      example = 15
    ),

    ni_2 = list(
      type = "INTEGER",
      required = FALSE,
      description = "Number of items (alternate calculation)",
      example = 14
    ),

    nt = list(
      type = "INTEGER",
      required = FALSE,
      description = "Number of transactions",
      example = 5
    ),

    date = list(
      type = "DATE",
      required = FALSE,
      description = "Analysis date",
      example = "2025-01-15"
    ),

    sum_spent_by_date = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Total spent as of analysis date",
      example = 1500.00
    ),

    count_transactions_by_date = list(
      type = "INTEGER",
      required = FALSE,
      description = "Transaction count as of analysis date",
      example = 8
    ),

    min_time_by_date = list(
      type = "DATE",
      required = FALSE,
      description = "Earliest transaction by analysis date",
      example = "2024-01-15"
    ),

    total_sum = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Total sum (may differ from total_spent based on calculation)",
      example = 1480.00
    )
  ),

  # =========================================================================
  # TABLE CONSTRAINTS
  # =========================================================================

  constraints = list(
    primary_key = c("customer_id", "platform_id"),

    indexes = c("customer_id", "platform_id", "clv", "cai"),

    foreign_keys = list(
      customer = "References df_profile_by_customer(customer_id)"
    )
  ),

  # =========================================================================
  # USAGE EXAMPLES
  # =========================================================================

  examples = list(

    segment_customers = "
    -- Segment customers by RFM
    SELECT
      customer_id,
      CASE
        WHEN r_ecdf > 0.75 AND f_ecdf > 0.75 AND m_ecdf > 0.75 THEN 'Champions'
        WHEN r_ecdf > 0.75 AND f_ecdf > 0.50 AND m_ecdf > 0.50 THEN 'Loyal'
        WHEN r_ecdf < 0.25 THEN 'At Risk'
        ELSE 'Regular'
      END as segment,
      clv
    FROM df_dna_by_customer
    ",

    high_value_customers = "
    -- Find high-value customers
    SELECT
      customer_id,
      total_spent,
      times as purchase_count,
      clv,
      cai_label
    FROM df_dna_by_customer
    WHERE clv > 1000
      AND cai > 0.7
    ORDER BY clv DESC
    "
  )
)

# =============================================================================
# VALIDATION FUNCTIONS
# =============================================================================

validate_customer_tables <- function(con) {

  results <- list()

  # Validate customer profile table
  results$profile <- validate_table_schema(
    con,
    "df_profile_by_customer",
    required_cols = c("customer_id", "platform_id")
  )

  # Validate DNA table
  results$dna <- validate_table_schema(
    con,
    "df_dna_by_customer",
    required_cols = c("customer_id", "platform_id", "total_spent", "times")
  )

  # Check referential integrity
  if (results$profile$valid && results$dna$valid) {
    orphan_check <- dbGetQuery(con, "
      SELECT COUNT(*) as orphan_count
      FROM df_dna_by_customer d
      WHERE NOT EXISTS (
        SELECT 1 FROM df_profile_by_customer p
        WHERE p.customer_id = d.customer_id
      )
    ")

    results$referential_integrity <- list(
      valid = orphan_check$orphan_count == 0,
      orphan_records = orphan_check$orphan_count
    )
  }

  return(results)
}

validate_table_schema <- function(con, table_name, required_cols) {

  if (!dbExistsTable(con, table_name)) {
    return(list(
      valid = FALSE,
      error = paste("Table", table_name, "does not exist")
    ))
  }

  actual_cols <- dbListFields(con, table_name)
  missing_cols <- setdiff(required_cols, actual_cols)

  if (length(missing_cols) > 0) {
    return(list(
      valid = FALSE,
      error = paste("Missing columns:", paste(missing_cols, collapse = ", ")),
      missing_columns = missing_cols
    ))
  }

  return(list(valid = TRUE, message = "Validation passed"))
}