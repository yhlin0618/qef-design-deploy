# Common ETL Utilities

This directory contains cross-platform utility functions that can be used by any platform-specific ETL pipeline. These functions provide standardized data processing capabilities across all platforms.

## Function Categories

### Data Processing & Transformation
- `convert_all_columns_to_utf8.R` - UTF-8 encoding standardization
- `convert_list_columns.R` - Convert list columns to appropriate types
- `fn_remove_illegal_utf8.R` - Remove illegal UTF-8 characters
- `convert_units_to_smallest.R` - Unit conversion utilities

### Customer Analytics
- `DNA_Function_data.table.R` - Customer DNA analysis using data.table
- `DNA_Function_dplyr.R` - Customer DNA analysis using dplyr
- `process_by_customer_time.R` - Customer-time based data processing
- `save_customer_time_data.R` - Customer time data persistence

### Time & Date Processing
- `Day_time_partition.R` - Day/time partitioning for analysis
- `Time_Recode.R` - Time recoding and standardization
- `Holidays_Recode.R` - Holiday detection and recoding
- `convert_to_local_time.R` - Local time zone conversion

### Geographic & Location
- `StateNameTranformation.R` - State name standardization
- `closest_position.R` - Geographic proximity calculations
- `209g_process_zip.R` - ZIP code processing and validation

### Data Management
- `load_all_RDS.R` - Batch loading of RDS files
- `load_selected_rds.R` - Selective RDS file loading
- `save_list_elements.R` - Save list elements to individual files
- `214g_process_and_save_all_products_states_and_times.R` - Product state/time processing

### Text Processing
- `convert_chinese_chars.R` - Chinese character encoding conversion
- `convert_chinese_chars_and_names.R` - Chinese character and name conversion
- `revert_chinese_chars.R` - Revert Chinese character conversion
- `revert_chinese_chars_and_names.R` - Revert Chinese character and name conversion

## Usage Guidelines

### ETL Phase Integration

These common functions are designed to be used across all ETL phases:

**Phase 1 (Staging):**
```r
# Source common staging utilities
source(file.path(GLOBAL_DIR, "05_etl_utils", "common", "convert_all_columns_to_utf8.R"))
source(file.path(GLOBAL_DIR, "05_etl_utils", "common", "fn_remove_illegal_utf8.R"))

# Apply in staging phase
staged_data <- convert_all_columns_to_utf8(raw_data)
clean_data <- remove_illegal_utf8(staged_data)
```

**Phase 2 (Transform):**
```r
# Source common transformation utilities
source(file.path(GLOBAL_DIR, "05_etl_utils", "common", "convert_list_columns.R"))
source(file.path(GLOBAL_DIR, "05_etl_utils", "common", "Time_Recode.R"))

# Apply transformations
transformed_data <- convert_list_columns(staged_data)
time_standardized <- standardize_time_columns(transformed_data)
```

**Phase 4 (Process):**
```r
# Source common processing utilities
source(file.path(GLOBAL_DIR, "05_etl_utils", "common", "DNA_Function_dplyr.R"))
source(file.path(GLOBAL_DIR, "05_etl_utils", "common", "process_by_customer_time.R"))

# Apply business logic processing
customer_dna <- calculate_customer_dna(cleansed_data)
customer_metrics <- process_by_customer_time(customer_dna)
```

### Cross-Platform Consistency

Common functions ensure consistent data processing across all platforms:

```r
# Amazon ETL using common functions
source(file.path(GLOBAL_DIR, "05_etl_utils", "common", "StateNameTranformation.R"))
amz_standardized <- standardize_state_names(amz_data)

# eBay ETL using same common functions  
eby_standardized <- standardize_state_names(eby_data)

# Official Website ETL using same common functions
ofw_standardized <- standardize_state_names(ofw_data)
```

## Development Guidelines

### Adding New Common Functions

When creating new common functions:

1. **Naming Convention**: Use descriptive names that indicate function purpose
2. **Documentation**: Include roxygen2 documentation with examples
3. **Testing**: Ensure functions work across different data formats
4. **Dependencies**: Minimize external package dependencies
5. **Error Handling**: Include robust error handling and validation

### Function Template

```r
#' Descriptive Function Title
#'
#' Detailed description of what the function does and when to use it.
#'
#' @param data A data frame or data table to process
#' @param param2 Additional parameter description
#' @return Processed data with same structure as input
#' @examples
#' \dontrun{
#' result <- common_function(data, param2 = "value")
#' }
#' @export
common_function <- function(data, param2 = NULL) {
  
  # Validation
  if (is.null(data) || nrow(data) == 0) {
    stop("Input data cannot be NULL or empty")
  }
  
  # Processing logic
  result <- data
  
  # Return processed data
  return(result)
}
```

## Platform Integration

Common functions are designed to integrate seamlessly with platform-specific ETL pipelines:

- **Amazon (amz/)**: Uses common functions for data standardization
- **eBay (eby/)**: Leverages common text processing functions
- **Official Website (ofw/)**: Uses common geographic functions
- **Shopify (sho/)**: Benefits from common time processing functions
- **Cyberbiz (cbz/)**: Uses common data management functions

This ensures consistent data quality and processing standards across all platforms while reducing code duplication.