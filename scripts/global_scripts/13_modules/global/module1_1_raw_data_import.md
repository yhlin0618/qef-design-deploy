# Module 1.1: Raw Data Import Module

## Purpose

The Raw Data Import Module is responsible for the reliable transformation of original source files from the `rawdata_[companyname]/` directory into the `raw_data` database while maintaining data integrity and complete traceability.

## Key Principles

1. **Data Sanctity**: Original source files are never modified
2. **Reproducibility**: All import operations must be scripted and reproducible
3. **Traceability**: Complete logging of all files, operations, and timestamps
4. **Validation**: Systematic verification of import success and data quality
5. **Modularity**: Operates as a standalone module with well-defined inputs and outputs

## Module Inputs

- Original source files in `rawdata_[companyname]/` directory
- Configuration parameters specifying file formats and expected structures
- Optional: Data dictionaries describing expected fields and data types

## Module Outputs

- Imported data tables in the `raw_data` database
- File catalog with checksums, timestamps, and metadata
- Validation report documenting any issues found
- Import log with detailed operation records

## Process Flow

```
                       ┌──────────────────────────────────────────────────┐
                       │                                                  │
                       │                                                  │
External Files → rawdata_[companyname]/ → Verification → raw_data Database → Validation 
               (preserve original)       (checksums)     (import as-is)     (quality checks)
                       │                                                  │
                       │                Raw Data Import Module            │
                       └──────────────────────────────────────────────────┘
                                               │
                                               ↓
                                     Import Log & File Catalog
                                       (complete traceability)
```

## Module Components

### 1. File Verification
- Calculate checksums of all source files
- Verify file integrity
- Record file sizes, timestamps, and sources
- Maintain a catalog of all imported files

### 2. File Parsing
- Parse files according to their format (CSV, Excel, etc.)
- Handle character encodings and format-specific issues
- Preserve original data exactly as provided

### 3. Database Import
- Create appropriate tables in the `raw_data` database
- Import data without any transformations
- Maintain original field names and data types
- Handle large files with proper memory management

### 4. Validation
- Count records before and after import
- Verify all fields were imported correctly
- Generate summary statistics for key fields
- Document any issues or anomalies found

### 5. Logging and Documentation
- Maintain detailed logs of all operations
- Document any decisions made during import
- Create an audit trail for data provenance
- Store metadata about all imported files

## Implementation Guidelines

### Database Connection
```r
# Connect to raw_data database using standardized connection function
source("update_scripts/global_scripts/02_db_utils/100g_dbConnect_from_list.R")
raw_db <- dbConnect_from_list("raw_data", read_only = FALSE, verbose = TRUE)
```

### File Handling
```r
# Example for CSV files - preserve all original data exactly
import_csv_to_raw <- function(file_path, table_name, raw_db) {
  # Calculate and log checksum
  checksum <- calculate_file_checksum(file_path)
  log_file_metadata(file_path, checksum)
  
  # Read file as-is without any transformations
  data <- read.csv(file_path, 
                  stringsAsFactors = FALSE, 
                  check.names = FALSE,  # Preserve original column names
                  na.strings = NULL)    # Don't convert any values to NA
  
  # Import to database preserving all original data
  dbWriteTable(raw_db, table_name, data, overwrite = TRUE)
  
  # Validate import
  record_count <- nrow(data)
  db_count <- dbGetQuery(raw_db, paste0("SELECT COUNT(*) FROM ", table_name))
  
  # Log results
  log_import_result(file_path, table_name, record_count, db_count)
  
  return(list(success = record_count == db_count[1,1], 
              count = record_count,
              issues = record_count != db_count[1,1]))
}
```

### Logging
```r
# Example logging function for file metadata
log_file_metadata <- function(file_path, checksum) {
  file_info <- file.info(file_path)
  
  file_catalog <- data.frame(
    file_path = file_path,
    file_name = basename(file_path),
    size_bytes = file_info$size,
    modified_time = file_info$mtime,
    import_time = Sys.time(),
    checksum = checksum
  )
  
  # Append to file catalog
  catalog_path <- "logs/file_catalog.csv"
  if(file.exists(catalog_path)) {
    existing_catalog <- read.csv(catalog_path, stringsAsFactors = FALSE)
    file_catalog <- rbind(existing_catalog, file_catalog)
  }
  
  write.csv(file_catalog, catalog_path, row.names = FALSE)
}
```

## Best Practices

1. **Always preserve raw data**: Never modify, clean, or transform data during import
2. **Handle file encoding carefully**: Detect and properly handle different character encodings
3. **Maintain detailed logs**: Record all operations for auditability
4. **Use standardized connection methods**: Always use dbConnect_from_list
5. **Create validation reports**: Document any issues or anomalies discovered
6. **Close connections properly**: Use dbDisconnect_all when finished

## Error Handling

1. If a file cannot be read, log the error and continue with other files
2. If a database connection fails, retry with exponential backoff
3. If validation shows discrepancies, generate a detailed report but do not modify data
4. For critical errors, halt the process and alert the operator

## Example Usage

```r
# Source required functions
source("update_scripts/global_scripts/module1_1_raw_data_import.R")

# Define the source directory and target database
source_dir <- "rawdata_company123/"
import_log_file <- "logs/import_log.txt"

# Run the import process
import_result <- import_raw_data(
  source_dir = source_dir,
  log_file = import_log_file,
  verbose = TRUE
)

# Generate validation report
generate_validation_report(
  import_result = import_result,
  output_file = "reports/raw_data_validation.html"
)
```

## Integration with Other Modules

- Provides imported raw data to Module 1.2 (Data Cleansing)
- Works with Module 0 (Common Functions) for logging and configuration
- Reports validation results to Module 8 (Quality Assurance)

## Maintainers

- Data Engineering Team

## Documentation References

- [Data Integrity Principles](../../00_principles/data_integrity_principles.md)
- [Function Reference](../../00_principles/function_reference.md)
- [Project Principles](../../00_principles/project_principles.md)