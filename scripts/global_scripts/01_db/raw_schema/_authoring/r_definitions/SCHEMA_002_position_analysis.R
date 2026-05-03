# =============================================================================
# SCHEMA_002: Position Analysis Table Definition
# =============================================================================
# Purpose: Define the standard schema for product position analysis
# Used by: WISER, MAMBA applications for market positioning analysis
# Critical: This table stores comment property ratings for competitive analysis
# =============================================================================

# Schema Definition
SCHEMA_position_analysis <- list(

  table_name = "df_position",

  description = "Stores product positioning data based on customer comment property ratings",

  columns = list(

    # =========================================================================
    # IDENTIFIER COLUMNS (Required)
    # =========================================================================

    product_line_id = list(
      type = "VARCHAR",
      required = TRUE,
      description = "Product line identifier",
      example = "alf"
    ),

    product_id = list(
      type = "VARCHAR",
      required = TRUE,
      description = "Unique product identifier",
      example = "276123204433"
    ),

    brand = list(
      type = "VARCHAR",
      required = TRUE,
      description = "Product brand name",
      example = "MAMBA"
    ),

    # =========================================================================
    # PERFORMANCE METRICS (Optional - can be NULL)
    # =========================================================================

    rating = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Overall product rating (0-5 scale typically)",
      example = 4.5
    ),

    sales = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Sales volume or count",
      example = 150
    ),

    # =========================================================================
    # COMMENT PROPERTY RATINGS - CHINESE (Optional)
    # =========================================================================
    # These represent customer sentiment scores extracted from reviews

    配送快速 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Fast delivery rating",
      example = 0.85
    ),

    賣家溝通良好 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Good seller communication rating",
      example = 0.90
    ),

    推薦他人 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Would recommend to others rating",
      example = 0.88
    ),

    產品符合描述 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Product matches description rating",
      example = 0.92
    ),

    品質優良 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Excellent quality rating",
      example = 0.87
    ),

    包裝完善 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Good packaging rating",
      example = 0.89
    ),

    運作良好 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Works well rating",
      example = 0.91
    ),

    價格實惠 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Affordable price rating",
      example = 0.75
    ),

    完美匹配 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Perfect fit rating",
      example = 0.88
    ),

    物超所值 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Great value for money rating",
      example = 0.80
    ),

    客服品質佳 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Good customer service rating",
      example = 0.86
    ),

    易於安裝 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Easy to install rating",
      example = 0.84
    ),

    配件完整 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Complete accessories rating",
      example = 0.90
    ),

    做工精良 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Well-crafted rating",
      example = 0.88
    ),

    耐用性佳 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Good durability rating",
      example = 0.85
    ),

    售後保障佳 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Good after-sales warranty rating",
      example = 0.82
    ),

    賣家信譽 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Seller reputation rating",
      example = 0.89
    ),

    配送可靠 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Reliable delivery rating",
      example = 0.87
    ),

    價格昂貴 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Expensive price rating (negative sentiment)",
      example = 0.25
    ),

    優質替代品 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Quality alternative product rating",
      example = 0.78
    ),

    車輛升級改裝 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Vehicle upgrade/modification rating",
      example = 0.83
    ),

    性能卓越 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Excellent performance rating",
      example = 0.89
    ),

    外觀精美 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Beautiful appearance rating",
      example = 0.81
    ),

    符合需求 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Meets requirements rating",
      example = 0.90
    ),

    品牌信譽 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Brand reputation rating",
      example = 0.86
    ),

    卓越工藝 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Excellent craftsmanship rating",
      example = 0.88
    ),

    價格合理 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Reasonable price rating",
      example = 0.77
    ),

    零件維修更換 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Parts repair/replacement rating",
      example = 0.79
    ),

    優惠折扣 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Discount offers rating",
      example = 0.72
    ),

    材質品質佳 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Good material quality rating",
      example = 0.86
    ),

    渦輪重建更新 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Turbo rebuild/update rating",
      example = 0.82
    ),

    配送準確 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Accurate delivery rating",
      example = 0.88
    ),

    故障排除 = list(
      type = "DOUBLE",
      required = FALSE,
      description = "Troubleshooting rating",
      example = 0.75
    )
  ),

  # =========================================================================
  # TABLE CONSTRAINTS
  # =========================================================================

  constraints = list(
    primary_key = c("product_line_id", "product_id"),

    indexes = c("brand", "product_line_id"),

    comment_properties = c(
      "配送快速", "賣家溝通良好", "推薦他人", "產品符合描述", "品質優良",
      "包裝完善", "運作良好", "價格實惠", "完美匹配", "物超所值",
      "客服品質佳", "易於安裝", "配件完整", "做工精良", "耐用性佳",
      "售後保障佳", "賣家信譽", "配送可靠", "價格昂貴", "優質替代品",
      "車輛升級改裝", "性能卓越", "外觀精美", "符合需求", "品牌信譽",
      "卓越工藝", "價格合理", "零件維修更換", "優惠折扣", "材質品質佳",
      "渦輪重建更新", "配送準確", "故障排除"
    )
  ),

  # =========================================================================
  # DATA QUALITY RULES
  # =========================================================================

  quality_rules = list(
    rating_range = "rating should be between 0 and 5",
    property_range = "All comment property scores should be between 0 and 1",
    null_handling = "NULL values are acceptable for all comment properties",
    at_least_one = "At least one comment property should have a non-NULL value"
  ),

  # =========================================================================
  # USAGE EXAMPLES
  # =========================================================================

  examples = list(

    create_table = '
    CREATE TABLE df_position (
      product_line_id VARCHAR,
      product_id VARCHAR,
      brand VARCHAR,
      rating DOUBLE,
      sales DOUBLE,
      配送快速 DOUBLE,
      賣家溝通良好 DOUBLE,
      推薦他人 DOUBLE,
      產品符合描述 DOUBLE,
      品質優良 DOUBLE,
      包裝完善 DOUBLE,
      運作良好 DOUBLE,
      價格實惠 DOUBLE,
      完美匹配 DOUBLE,
      物超所值 DOUBLE,
      客服品質佳 DOUBLE,
      易於安裝 DOUBLE,
      配件完整 DOUBLE,
      做工精良 DOUBLE,
      耐用性佳 DOUBLE,
      售後保障佳 DOUBLE,
      賣家信譽 DOUBLE,
      配送可靠 DOUBLE,
      價格昂貴 DOUBLE,
      優質替代品 DOUBLE,
      車輛升級改裝 DOUBLE,
      性能卓越 DOUBLE,
      外觀精美 DOUBLE,
      符合需求 DOUBLE,
      品牌信譽 DOUBLE,
      卓越工藝 DOUBLE,
      價格合理 DOUBLE,
      零件維修更換 DOUBLE,
      優惠折扣 DOUBLE,
      材質品質佳 DOUBLE,
      渦輪重建更新 DOUBLE,
      配送準確 DOUBLE,
      故障排除 DOUBLE
    )',

    query_top_rated = "
    SELECT product_id, brand, rating, 品質優良, 性能卓越, 物超所值
    FROM df_position
    WHERE rating >= 4.0
      AND 品質優良 > 0.8
    ORDER BY rating DESC, sales DESC
    ",

    pivot_for_analysis = "
    -- Pivot comment properties for radar chart analysis
    SELECT
      product_id,
      brand,
      'quality' as dimension,
      (品質優良 + 做工精良 + 材質品質佳 + 卓越工藝) / 4 as score
    FROM df_position
    UNION ALL
    SELECT
      product_id,
      brand,
      'service' as dimension,
      (客服品質佳 + 賣家溝通良好 + 售後保障佳 + 賣家信譽) / 4 as score
    FROM df_position
    UNION ALL
    SELECT
      product_id,
      brand,
      'value' as dimension,
      (價格實惠 + 物超所值 + 價格合理 - 價格昂貴) / 3 as score
    FROM df_position
    "
  )
)

# =============================================================================
# VALIDATION FUNCTION
# =============================================================================

validate_position_table <- function(con, table_name = "df_position") {

  # Check if table exists
  if (!dbExistsTable(con, table_name)) {
    return(list(
      valid = FALSE,
      error = paste("Table", table_name, "does not exist")
    ))
  }

  # Get actual columns
  actual_cols <- dbListFields(con, table_name)

  # Required core columns
  required_cols <- c("product_line_id", "product_id", "brand")

  # Check for missing required columns
  missing_cols <- setdiff(required_cols, actual_cols)

  if (length(missing_cols) > 0) {
    return(list(
      valid = FALSE,
      error = paste("Missing required columns:", paste(missing_cols, collapse = ", ")),
      missing_columns = missing_cols
    ))
  }

  # Check for at least some comment properties
  comment_props <- SCHEMA_position_analysis$constraints$comment_properties
  available_props <- intersect(comment_props, actual_cols)

  if (length(available_props) == 0) {
    return(list(
      valid = FALSE,
      error = "No comment property columns found",
      expected_properties = comment_props
    ))
  }

  return(list(
    valid = TRUE,
    message = "Schema validation passed",
    available_properties = available_props
  ))
}

# =============================================================================
# POSITION ANALYSIS HELPER FUNCTIONS
# =============================================================================

calculate_position_dimensions <- function(df_position) {
  # Group comment properties into strategic dimensions

  dimensions <- list(
    quality = c("品質優良", "做工精良", "材質品質佳", "卓越工藝", "耐用性佳"),
    service = c("客服品質佳", "賣家溝通良好", "售後保障佳", "賣家信譽", "配送快速", "配送可靠", "配送準確"),
    value = c("價格實惠", "物超所值", "價格合理", "優惠折扣"),
    performance = c("性能卓越", "運作良好", "完美匹配", "符合需求"),
    convenience = c("易於安裝", "配件完整", "故障排除")
  )

  # Calculate dimension scores
  result <- df_position %>%
    mutate(
      quality_score = rowMeans(select(., all_of(dimensions$quality)), na.rm = TRUE),
      service_score = rowMeans(select(., all_of(dimensions$service)), na.rm = TRUE),
      value_score = rowMeans(select(., all_of(dimensions$value)), na.rm = TRUE),
      performance_score = rowMeans(select(., all_of(dimensions$performance)), na.rm = TRUE),
      convenience_score = rowMeans(select(., all_of(dimensions$convenience)), na.rm = TRUE)
    ) %>%
    select(product_line_id, product_id, brand,
           quality_score, service_score, value_score,
           performance_score, convenience_score)

  return(result)
}