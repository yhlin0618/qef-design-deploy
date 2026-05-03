# Universal Import Functions

This directory contains platform-agnostic import functions that can be used by any platform ETL pipeline.

## Function Overview

### fn_import_product_profiles.R
**Purpose**: Universal function for importing product profiles from Google Sheets  
**Platforms**: Can be used by Amazon, eBay, Official Website, Shopify, Cyberbiz, etc.  
**Phase**: ETL Phase 0 (Import)

**Key Features**:
- Platform-agnostic implementation
- Supports multiple product lines
- Creates standardized raw data tables
- Minimal import metadata only
- No staging or transformation logic

**Usage Examples**:

```r
# Amazon ETL using universal function
source(file.path(GLOBAL_DIR, "05_etl_utils", "all", "import", "fn_import_product_profiles.R"))
amz_result <- import_product_profiles(
  db_connection = raw_data,
  product_line_df = df_product_line,
  google_sheet_id = "amazon_sheet_id",
  sheet_name_prefix = "amz_product_profile"
)

# eBay ETL using same function
eby_result <- import_product_profiles(
  db_connection = raw_data,
  product_line_df = df_product_line,
  google_sheet_id = "ebay_sheet_id",
  sheet_name_prefix = "eby_product_profile"
)

# Official Website ETL using same function
ofw_result <- import_product_profiles(
  db_connection = raw_data,
  product_line_df = df_product_line,
  google_sheet_id = "website_sheet_id",
  sheet_name_prefix = "ofw_product_profile"
)
```

## Design Principles

### Universal Patterns
1. **Platform Independence**: Functions work with any data source format
2. **Consistent Interface**: Same function signature across platforms
3. **Minimal Processing**: Pure import without transformation
4. **Metadata Standards**: Consistent import metadata structure

### Integration with Platform ETL
```
Platform ETL Pipeline:
{platform}_ETL{series}_{phase}{abbrev}_{sequence}.R
    ↓
Sources: all/import/fn_import_*.R (universal)
    ↓  
Outputs: raw_{data_type}_{product_line_id} tables
```

### Output Standardization
All import functions create tables with consistent naming:
- `raw_product_profile_{product_line_id}`
- Include standard metadata: `etl_import_timestamp`, `etl_import_source`, etc.
- Platform identifier added: `etl_platform_id`

## Future Expansion

Additional universal import functions can be added here:
- `fn_import_competitor_data.R` - Universal competitor data import
- `fn_import_reviews.R` - Universal review data import
- `fn_import_sales_data.R` - Universal sales data import
- `fn_import_inventory.R` - Universal inventory data import

## Best Practices

### When to Use Universal Functions
- Function logic is truly platform-independent
- Same data source type (e.g., Google Sheets, CSV files)
- Consistent data structure across platforms
- No platform-specific business logic needed

### When to Create Platform-Specific Functions
- Platform-specific APIs or data formats
- Complex platform-specific transformations
- Different authentication mechanisms
- Platform-unique data structures

This organization ensures maximum code reusability while maintaining clear separation between universal and platform-specific functionality.