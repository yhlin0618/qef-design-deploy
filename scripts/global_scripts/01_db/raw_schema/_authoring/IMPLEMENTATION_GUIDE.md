# ETL Output Standardization Implementation Guide

## Overview

This guide provides step-by-step instructions for implementing the ETL Output Standardization Principle (MP102) across all platform ETL scripts. Following this guide ensures consistent, interoperable data outputs that enable reliable cross-platform analytics.

## Quick Reference

- **Principle**: MP102 - ETL Output Standardization
- **Rule**: DM_R027 - ETL Schema Validation
- **Registry**: `01_db/raw_schema/_authoring/schema_registry.yaml`
- **Core Schemas**: `01_db/raw_schema/_authoring/core_schemas.yaml`
- **Extensions**: `01_db/raw_schema/_authoring/platform_extensions/`

> **Path note (2026-04-27)**: This guide and its sibling yaml files relocated
> from `00_principles/docs/.../etl_schemas/` to `01_db/raw_schema/_authoring/`
> per amended MP102 + new MP156 (spectra change `glue-layer-prerawdata-bridge`,
> issue #489). All paths in this document have been updated to the new
> canonical location.

## Implementation Steps

### Step 1: Assess Current ETL Scripts

First, identify all ETL scripts for your platform:

```r
# Find all ETL scripts for a platform
platform_id <- "cbz"  # or "eby", "amz", etc.
etl_scripts <- list.files(
  path = "scripts/update_scripts",
  pattern = sprintf("^%s_ETL.*\\.R$", platform_id),
  full.names = TRUE,
  recursive = TRUE
)
```

### Step 2: Review Core Schema Requirements

Check the core fields required for each table type:

```yaml
# From core_schemas.yaml
sales:
  - order_id        # Required
  - customer_id     # Required
  - order_date      # Required
  - product_id      # Required
  - quantity        # Required
  - unit_price      # Required
  - total_amount    # Required
  - platform_id   # Required (3-letter code)
  - import_timestamp # Required
  - import_source   # Required
```

### Step 3: Map Source Fields to Core Schema

Document the mapping between your data source and core fields:

```r
# Example mapping for Cyberbiz
field_mapping <- list(
  # Core field -> Source field(s)
  order_id = "id",
  customer_id = "member_id",
  order_date = "created_at",
  product_id = "sku",
  quantity = "qty",
  unit_price = "price",
  total_amount = "total"
)
```

### Step 4: Implement Field Transformation

Update your ETL script to include field mapping:

```r
# In your ETL01_0IM.R script
transform_to_core_schema <- function(raw_data, platform_id) {
  # Map to core fields
  core_data <- raw_data %>%
    mutate(
      # Core fields (required)
      order_id = as.character(id),
      customer_id = as.character(member_id),
      order_date = as.character(created_at),
      product_id = as.character(sku),
      quantity = as.integer(qty),
      unit_price = as.numeric(price),
      total_amount = as.numeric(total),
      
      # Metadata fields (required)
      platform_id = platform_id,
      import_timestamp = Sys.time(),
      import_source = "API"
    )
  
  # Preserve platform-specific fields with prefix
  platform_fields <- names(raw_data)[!names(raw_data) %in% names(core_data)]
  
  for (field in platform_fields) {
    prefixed_name <- paste0(platform_id, "_", field)
    core_data[[prefixed_name]] <- raw_data[[field]]
  }
  
  return(core_data)
}
```

### Step 5: Add Schema Validation

Include validation before writing data:

```r
# Load validation function
source("scripts/global_scripts/05_etl_utils/fn_validate_etl_schema.R")

# Validate before writing
validate_etl_schema(
  data = df_sales,
  platform_id = "cbz",
  table_type = "sales",
  allow_extensions = TRUE
)

# Write only if validation passes
dbWriteTable(con, "df_cbz_sales___raw", df_sales, overwrite = TRUE)
```

### Step 6: Document Platform Extensions

Create or update your platform's extension file:

```yaml
# platform_extensions/cbz_extensions.yaml
sales:
  extends: "core_sales_v1"
  fields:
    cbz_shop_id:
      type: VARCHAR
      description: "Cyberbiz shop identifier"
    cbz_payment_method:
      type: VARCHAR
      description: "Payment method used"
```

### Step 7: Update Schema Registry

Register your ETL outputs in the schema registry:

```yaml
# schema_registry.yaml
platforms:
  cbz:
    outputs:
      - table_name: "df_cbz_sales___raw"
        table_type: "sales"
        conforms_to: "core_sales_v1"
        has_extensions: true
        extension_prefix: "cbz_"
```

## Migration Checklist

Use this checklist for migrating existing ETL scripts:

- [ ] Identify all ETL scripts for the platform
- [ ] Review current output table structure
- [ ] Map existing fields to core schema
- [ ] Add missing core fields
- [ ] Prefix platform-specific fields
- [ ] Add validation function calls
- [ ] Update schema registry
- [ ] Document platform extensions
- [ ] Test with sample data
- [ ] Verify downstream processes still work

## Common Patterns

### Pattern 1: API Data Import

```r
# Standard pattern for API data
api_import_pattern <- function(api_endpoint, platform_id) {
  # 1. Fetch raw data
  raw_data <- fetch_from_api(api_endpoint)
  
  # 2. Transform to core schema
  core_data <- transform_to_core_schema(raw_data, platform_id)
  
  # 3. Validate
  validate_etl_schema(core_data, platform_id, "sales")
  
  # 4. Write to database
  table_name <- sprintf("df_%s_sales_raw", platform_id)
  dbWriteTable(con, table_name, core_data, overwrite = TRUE)
}
```

### Pattern 2: CSV/Excel Import

```r
# Standard pattern for file imports
file_import_pattern <- function(file_path, platform_id) {
  # 1. Read file
  raw_data <- read_csv(file_path)
  
  # 2. Add path metadata
  raw_data$path <- file_path
  
  # 3. Transform to core schema
  core_data <- transform_to_core_schema(raw_data, platform_id)
  
  # 4. Validate and write
  validate_etl_schema(core_data, platform_id, "sales")
  dbWriteTable(con, table_name, core_data, append = TRUE)
}
```

### Pattern 3: Handling Missing Core Fields

```r
# When source doesn't have all core fields
handle_missing_fields <- function(raw_data, platform_id) {
  # Use defaults or derive values
  if (!"customer_id" %in% names(raw_data)) {
    # Derive from email or username
    raw_data$customer_id <- coalesce(
      raw_data$buyer_email,
      raw_data$buyer_username,
      paste0("UNKNOWN_", raw_data$order_id)
    )
  }
  
  if (!"unit_price" %in% names(raw_data)) {
    # Calculate from total and quantity
    raw_data$unit_price <- raw_data$total_amount / raw_data$quantity
  }
  
  return(raw_data)
}
```

## Validation Examples

### Strict Validation (Development)

```r
# Stop on any schema violation
tryCatch({
  validate_etl_schema_strict(
    data = df_sales,
    platform_id = "cbz",
    table_type = "sales"
  )
  dbWriteTable(con, table_name, df_sales, overwrite = TRUE)
  message("Data written successfully")
}, error = function(e) {
  stop(sprintf("Schema validation failed: %s", e$message))
})
```

### Permissive Validation (Migration)

```r
# Warn but continue for gradual migration
validation_result <- validate_etl_schema_permissive(
  data = df_sales,
  platform_id = "cbz",
  table_type = "sales"
)

if (!validation_result) {
  warning("Schema validation failed - review warnings above")
}

# Write data anyway during migration period
dbWriteTable(con, table_name, df_sales, overwrite = TRUE)
```

## Testing Your Implementation

### Test 1: Core Fields Present

```r
test_core_fields <- function(con, platform_id) {
  table_name <- sprintf("df_%s_sales_raw", platform_id)
  
  # Get actual fields
  actual_fields <- dbListFields(con, table_name)
  
  # Required core fields
  required_fields <- c(
    "order_id", "customer_id", "order_date",
    "product_id", "quantity", "unit_price",
    "total_amount", "platform_id",
    "import_timestamp", "import_source"
  )
  
  missing <- setdiff(required_fields, actual_fields)
  
  if (length(missing) > 0) {
    stop(sprintf("Missing core fields: %s", 
                 paste(missing, collapse = ", ")))
  }
  
  message("All core fields present")
  return(TRUE)
}
```

### Test 2: Platform Extensions Prefixed

```r
test_extension_prefix <- function(con, platform_id) {
  table_name <- sprintf("df_%s_sales_raw", platform_id)
  
  # Get all fields
  all_fields <- dbListFields(con, table_name)
  
  # Core fields (shouldn't have prefix)
  core_fields <- c(
    "order_id", "customer_id", "order_date",
    "product_id", "quantity", "unit_price",
    "total_amount", "platform_id",
    "import_timestamp", "import_source"
  )
  
  # Extension fields
  extension_fields <- setdiff(all_fields, core_fields)
  
  # Check prefix
  invalid <- extension_fields[
    !grepl(sprintf("^%s_", platform_id), extension_fields)
  ]
  
  if (length(invalid) > 0) {
    warning(sprintf("Fields missing platform prefix: %s",
                    paste(invalid, collapse = ", ")))
    return(FALSE)
  }
  
  message("All extension fields properly prefixed")
  return(TRUE)
}
```

### Test 3: Cross-Platform Compatibility

```r
test_cross_platform <- function(con) {
  platforms <- c("cbz", "eby", "amz")
  
  # Try to union all sales tables using only core fields
  query <- paste(
    map_chr(platforms, function(p) {
      sprintf(
        "SELECT order_id, customer_id, product_id, 
                quantity, total_amount, platform_id
         FROM df_%s_sales_raw", p
      )
    }),
    collapse = " UNION ALL "
  )
  
  result <- tryCatch(
    dbGetQuery(con, query),
    error = function(e) {
      stop(sprintf("Cross-platform query failed: %s", e$message))
    }
  )
  
  message(sprintf("Cross-platform query successful: %d total records",
                  nrow(result)))
  return(TRUE)
}
```

## Troubleshooting

### Issue: Missing Core Fields

**Problem**: Source data doesn't have all required core fields.

**Solution**: 
1. Check if field exists with different name (mapping issue)
2. Derive from other fields if possible
3. Use reasonable defaults (with documentation)
4. Mark as NULL if truly unavailable (update schema to allow)

### Issue: Type Mismatches

**Problem**: Data types don't match schema specification.

**Solution**:
```r
# Explicit type conversion
df_sales <- df_sales %>%
  mutate(
    order_id = as.character(order_id),
    quantity = as.integer(quantity),
    unit_price = as.numeric(unit_price),
    import_timestamp = as.POSIXct(import_timestamp)
  )
```

### Issue: Platform Fields Not Prefixed

**Problem**: Existing code expects unprefixed field names.

**Solution**:
1. Add compatibility layer during migration
2. Update downstream code gradually
3. Use views to provide both prefixed and unprefixed versions

```r
# Compatibility view
create_compatibility_view <- function(con, platform_id) {
  query <- sprintf("
    CREATE OR REPLACE VIEW v_%s_sales_compat AS
    SELECT 
      *,
      -- Alias prefixed fields to original names
      %s_payment_method AS payment_method,
      %s_shipping_cost AS shipping_cost
    FROM df_%s_sales_raw
  ", platform_id, platform_id, platform_id, platform_id)
  
  dbExecute(con, query)
}
```

## Best Practices

1. **Always validate before writing** - Catch issues early
2. **Document field mappings** - In script comments
3. **Use consistent naming** - Follow the patterns
4. **Preserve all data** - Don't drop fields, prefix them
5. **Test with sample data** - Before full migration
6. **Version your schemas** - Track changes over time
7. **Monitor validation logs** - Identify recurring issues

## Support Resources

- **Principle Documentation**: MP102_etl_output_standardization.qmd
- **Validation Rule**: DM_R027_etl_schema_validation.qmd
- **Schema Registry**: `01_db/raw_schema/_authoring/schema_registry.yaml`
- **Core Schemas**: `01_db/raw_schema/_authoring/core_schemas.yaml`
- **Platform Extensions**: `01_db/raw_schema/_authoring/platform_extensions/`

## Next Steps

1. Start with one platform as pilot
2. Migrate ETL scripts following this guide
3. Test thoroughly with sample data
4. Update downstream processes if needed
5. Roll out to other platforms
6. Monitor and refine based on experience

Remember: The goal is consistency and interoperability while preserving platform-specific richness. When in doubt, preserve data with proper prefixing rather than dropping it.