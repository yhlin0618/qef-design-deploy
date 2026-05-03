# ETL Utility Functions

This directory contains ETL-specific utility functions organized by phase and functionality.

## Directory Structure

### Platform-Based Organization

Functions are organized by platform using standardized platform IDs:

| Platform | Platform ID | Directory | Product ID Coding |
|----------|-------------|-----------|-------------------|
| All Platforms | `all` | `all/` | - (universal functions) |
| Amazon | `amz` | `amz/` | asin |
| eBay | `eby` | `eby/` | ebay_product_number |
| Official Website | `ofw` | `ofw/` | sku |
| Shopify | `sho` | `sho/` | - |
| Cyberbiz | `cbz` | `cbz/` | sku |

### Platform-Specific Functions

#### All Platforms (all/)
**Universal functions that work across all platforms:**
- `import/fn_import_product_profiles.R` - Universal product profiles import from Google Sheets

#### Amazon (amz/)
- `fn_import_competitor_products.R` - Import competitor product data  
- `fn_import_comment_properties.R` - Import comment property ratings
- `fn_import_comment_property_ratings.R` - Import comment property ratings data
- `core_import_df_amz_competitor_sales.R` - Import competitor sales data
- `import_amazon_sales.R` - Main Amazon sales import
- Legacy processing functions for Amazon sales data

#### Official Website (ofw/)
- Legacy processing functions for official website sales data

#### Common Utilities (common/)
Cross-platform utility functions that can be used by any platform-specific ETL:

**Data Processing Functions:**
- `DNA_Function_data.table.R` - Customer DNA analysis using data.table
- `DNA_Function_dplyr.R` - Customer DNA analysis using dplyr
- `convert_all_columns_to_utf8.R` - UTF-8 encoding conversion
- `convert_list_columns.R` - List column type conversion
- `fn_remove_illegal_utf8.R` - Clean illegal UTF-8 characters

**Time & Date Processing:**
- `Day_time_partition.R` - Day/time partitioning utilities
- `Time_Recode.R` - Time recoding functions
- `Holidays_Recode.R` - Holiday detection and recoding
- `convert_to_local_time.R` - Local time conversion

**Geographic & Location:**
- `StateNameTranformation.R` - State name standardization
- `closest_position.R` - Geographic proximity calculations
- `209g_process_zip.R` - ZIP code processing

**Data Management:**
- `load_all_RDS.R` - Batch RDS file loading
- `load_selected_rds.R` - Selective RDS loading
- `save_list_elements.R` - Save list elements to files
- `process_by_customer_time.R` - Customer-time based processing
- `save_customer_time_data.R` - Customer time data persistence

**Text Processing:**
- `convert_chinese_chars.R` - Chinese character conversion
- `convert_chinese_chars_and_names.R` - Chinese character and name conversion
- `revert_chinese_chars.R` - Revert Chinese character conversion
- `revert_chinese_chars_and_names.R` - Revert Chinese character and name conversion
- `convert_units_to_smallest.R` - Unit conversion utilities

**Example Usage:**
```r
# Source common utilities
source(file.path(GLOBAL_DIR, "05_etl_utils", "common", "convert_all_columns_to_utf8.R"))
source(file.path(GLOBAL_DIR, "05_etl_utils", "common", "DNA_Function_dplyr.R"))

# Use in any platform ETL
clean_data <- convert_all_columns_to_utf8(raw_data)
dna_results <- calculate_customer_dna(sales_data)
```

## ETL Phase Organization

Functions are organized according to the ETL00 framework:

| Phase | Code | Functions | Purpose |
|-------|------|-----------|---------|
| 0 | Import (IM) | fn_import_*.R | Pure data import from external sources |
| 1 | Staging (ST) | fn_stage_*.R | File preprocessing and validation |
| 2 | Transform (TR) | fn_transform_*.R | Schema mapping and type conversion |
| 3 | Cleanse (CL) | fn_cleanse_*.R | Data quality and duplicate handling |
| 4 | Process (PR) | fn_process_*.R | Business logic and aggregation |
| 5 | Application (AP) | fn_prepare_*.R | UI-ready optimization |

## Usage

```r
# Source universal ETL utility functions
source(file.path(GLOBAL_DIR, "05_etl_utils", "all", "import", "fn_import_product_profiles.R"))
source(file.path(GLOBAL_DIR, "05_etl_utils", "amz", "fn_import_competitor_products.R"))

# Use universal functions in any platform ETL
# Amazon example using universal function
amz_result <- import_product_profiles(
  db_connection = raw_data,
  product_line_df = df_product_line,
  google_sheet_id = "amazon_sheet_id"
)

# eBay example using same universal function
eby_result <- import_product_profiles(
  db_connection = raw_data,
  product_line_df = df_product_line,
  google_sheet_id = "ebay_sheet_id"
)

# Cross-platform naming convention
# {platform_id}_ETL{series}_{phase}{abbrev}_{sequence}.R
# Examples:
# amz_ETL03_0IM_00.R  - Amazon product Profiles Import
# ofw_ETL01_0IM_00.R  - Official Website Sales Import  
# eby_ETL02_1ST_00.R  - eBay Data Staging
```

## Migration Notes

This directory was renamed from `05_data_processing` to better reflect its ETL-specific purpose and align with the ETL framework architecture defined in ETL00.

---

## Legacy Documentation (Amazon Sales Data Processing)

### Overview

The Amazon sales data processing pipeline consists of two main steps:

1. **Import**: Reading Excel files from Amazon reports and importing them into the raw data database.
2. **Process**: Transforming the raw data into a standardized format and storing it in the processed data database.

### Usage

#### Basic Usage

```r
# Initialize the environment
source(file.path("update_scripts", "global_scripts", "00_principles", "000g_initialization_update_mode.R"))

# Connect to databases
raw_data <- dbConnect(duckdb::duckdb(), dbdir = "path/to/raw_data.duckdb")
processed_data <- dbConnect(duckdb::duckdb(), dbdir = "path/to/processed_data.duckdb")

# Import Amazon sales data from Excel files
import_amazon_sales_dta(
  folder_path = "path/to/amazon_data", 
  connection = raw_data
)

# Process the imported data
process_amazon_sales(
  raw_data = raw_data,
  Data = processed_data
)

# Disconnect from databases
dbDisconnect_all()
```

### Table Schema

The Amazon sales data processing creates a table with the following structure:

- `customer_id`: Extracted from buyer email (before @)
- `time`: Timestamp of the purchase
- `sku`: Stock keeping unit
- `lineproduct_price`: Price of the product
- `asin`: Amazon Standard Identification Number
- `product_line_id`: Product line identifier
- Additional fields from the Amazon report
