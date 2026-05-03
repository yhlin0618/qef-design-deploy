# Database Utilities Module (02_db_utils)

This module provides essential database connection and utility functions for the MAMBA system.

## Module Structure

```
02_db_utils/
├── General Functions (Priority - Main Folder)
│   ├── fn_dbCopyTable.R             # Universal table copying (any DBI connection)
│   ├── fn_nrow2.R                   # Safe row counting (any R object)
│   ├── fn_dbDisconnect_all.R        # General disconnect (any DBI connection)
│   ├── fn_data_cleaning.R           # Database-agnostic data cleaning
│   ├── fn_list_column_handling.R    # General list column processing
│   ├── fn_coalesce_suffix_cols.R    # General column operations
│   ├── fn_dbPrintInfo.R             # General DB info (any DBI connection)
│   └── fn_print_query.R             # General SQL formatting
├── duckdb/ (DuckDB-specific only)
│   ├── fn_dbConnectDuckdb.R         # DuckDB connection wrapper
│   ├── fn_duckdb_connection.R       # Advanced DuckDB connection patterns
│   ├── fn_duckdb_copy_optimized.R   # High-performance DuckDB table copying
│   ├── fn_duckdb_disconnect_with_export.R # DuckDB disconnect with export/backup
│   ├── fn_get_default_db_paths.R    # DuckDB-specific path management
│   ├── fn_set_db_paths.R            # DuckDB-specific path configuration
│   ├── fn_dbAttachDuckdb.R          # Attach databases
│   ├── fn_dbDetachDuckdb.R          # Detach databases
│   ├── fn_dbDetach_all.R            # Detach all attached databases
│   ├── fn_dbOverwrite.R             # DuckDB file overwrite
│   ├── fn_duckdb_import_export.R    # Import/export utilities
│   ├── fn_duckdb_list_columns.R     # LIST type handling
│   ├── fn_duckdb_mamba_patterns.R   # MAMBA-specific patterns
│   ├── fn_duckdb_nested_types.R     # STRUCT/MAP type handling
│   ├── fn_duckdb_query_patterns.R   # Query optimization patterns
│   ├── fn_duckdb_type_conversion.R  # Type conversion utilities
│   └── fn_export_duckdb_dataframes.R # Export dataframes
└── tbl2/ (universal table access)
    └── fn_tbl2.R                    # Core tbl2 function
```

## Architecture: Generality Over Specificity (MP098)

This module implements **MP098: Generality Over Specificity Principle**, which prefers general, reusable functions over specific implementations:

- **General Functions** (top level): Work with any DBI connection or R object
- **Specific Functions** (subdirectories): Optimized for particular database types
- **Universal Access** (tbl2): Consistent interface across different data sources

### General vs Specific Function Examples

```r
# ✅ GOOD: General function (works with any DBI connection)
dbCopyTable(postgres_con, duckdb_con, "customers")  # PostgreSQL → DuckDB
dbCopyTable(sqlite_con, mysql_con, "products")     # SQLite → MySQL

# ❌ BAD: Would need separate functions for each combination
# dbCopyPostgresToDuckDB(), dbCopyDuckDBToPostgres(), etc.
```

## Primary Functions

### General Database Operations (Priority)

```r
# Copy tables between ANY database types (general DBI implementation)
dbCopyTable(source_con, target_con, "table_name", overwrite = TRUE)

# Safe row counting for ANY R object or database table
count <- nrow2(data_frame)      # Data frame
count <- nrow2(tbl_reference)   # Database table (uses COUNT query)
count <- nrow2(NULL)            # Returns 0 safely

# Disconnect all connections (general DBI implementation)
dbDisconnect_all()

# Print info for any database
dbPrintInfo(any_con, verbose = TRUE)

# Format any SQL query
print_query("SELECT * FROM table", "My Query")
```

### DuckDB-Specific Operations

```r
# Located in duckdb/ subdirectory
source("duckdb/fn_dbConnectDuckdb.R")
con <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)

# High-performance DuckDB table copying (uses ATTACH/DETACH)
source("duckdb/fn_duckdb_copy_optimized.R")
duckdb_copy_table_optimized(source_con, target_con, "large_table")

# DuckDB disconnect with export and backup
source("duckdb/fn_duckdb_disconnect_with_export.R")
duckdb_disconnect_with_export(create_backups = TRUE)

# DuckDB path management
source("duckdb/fn_get_default_db_paths.R")
source("duckdb/fn_set_db_paths.R")
```

### Universal Data Access - tbl2

The `tbl2()` function provides a consistent interface for accessing data from various sources:

```r
# Database connection
data <- tbl2(con, "sales")

# List with data.frames
data_list <- list(customers = customers_df)
data <- tbl2(data_list, "customers")

# Function-based access
data_fn <- function() read.csv("data.csv")
data <- tbl2(data_fn, "data")
```

## Data Processing Utilities

### Data Cleaning
```r
# Clean and standardize data
data_clean <- data_cleaning(data, 
  remove_duplicates = TRUE,
  standardize_names = TRUE)
```

### List Column Handling
```r
# Handle complex nested data structures
data_flat <- handle_list_columns(data, 
  flatten = TRUE,
  preserve_structure = FALSE)
```

### Column Coalescing
```r
# Coalesce columns with suffixes (useful after joins)
data_merged <- coalesce_suffix_cols(data, 
  suffixes = c(".x", ".y"),
  keep_first = TRUE)
```

## Debugging Tools

### Database Information
```r
# Print database tables and structure
dbPrintInfo(con, verbose = TRUE)
```

### Query Debugging
```r
# Print formatted SQL queries
print_query(query_string, format = TRUE)
```

## General Table Operations

### Universal Table Copying with dbCopyTable

The `dbCopyTable` function works with ANY DBI-compliant connection:

```r
# Copy between different database types
dbCopyTable(duckdb_con, postgres_con, "sales_data")      # DuckDB → PostgreSQL
dbCopyTable(sqlite_con, mysql_con, "products", overwrite = TRUE)  # SQLite → MySQL

# Optimized DuckDB → DuckDB (uses ATTACH + CTAS, avoids R memory)
dbCopyTable(source_duckdb, target_duckdb, "large_table")  # Direct database copy

# Copy with rename
dbCopyTable(source_con, target_con, "old_name", "new_name")

# Create temporary table
dbCopyTable(source_con, target_con, "data", temporary = TRUE)
```

### Safe Row Count with nrow2

The `nrow2` function provides safe row counting for ANY R object:

```r
# Returns 0 instead of errors for NULL or invalid inputs
count <- nrow2(NULL)  # Returns 0
count <- nrow2(invalid_object)  # Returns 0 instead of error

# With database tbl reference - uses optimized COUNT query
customers_ref <- tbl2(con, "customers") %>%
  filter(status == "active")
count <- nrow2(customers_ref)  # Efficient COUNT(*) query, no data transfer

# Safe usage in conditional statements
if (nrow2(my_data) > 0) {
  # Process data safely
}
```

## DuckDB-Specific Functions

Functions that are truly specific to DuckDB (cannot be generalized) are organized in the `duckdb/` subdirectory:

### When to Use Specific Functions
- **Performance**: DuckDB-specific optimizations (bulk operations, ATTACH)
- **Features**: Database-specific functionality not in DBI standard
- **File Operations**: Direct file manipulation (database overwrite)

### General vs Specific Decision Matrix
| Operation | General Function | Specific Function | Reason |
|-----------|------------------|-------------------|---------|
| Copy table | `dbCopyTable()` | `duckdb/fn_duckdb_copy_optimized.R` | DuckDB ATTACH optimization available |
| Count rows | `nrow2()` | - | Works with any R object |
| Disconnect | `dbDisconnect_all()` | `duckdb/fn_duckdb_disconnect_with_export.R` | DuckDB export/backup features |
| File overwrite | - | `duckdb/fn_dbOverwrite.R` | DuckDB file-specific operation |
| Bulk import | `dbWriteTable()` | `duckdb/fn_duckdb_import_export.R` | DuckDB optimizations available |

### Connection Management
- `fn_duckdb_connection.R` - Advanced connection patterns
- `fn_dbAttachDuckdb.R` - Attach external databases
- `fn_dbDetachDuckdb.R` - Detach databases
- `fn_dbDetach_all.R` - Detach all attached databases

### Data Import/Export
- `fn_duckdb_import_export.R` - Import/export patterns
- `fn_export_duckdb_dataframes.R` - Export dataframes

### Type Handling
- `fn_duckdb_type_conversion.R` - Type conversion utilities
- `fn_duckdb_nested_types.R` - Nested data structures
- `fn_duckdb_list_columns.R` - List column handling

### Query Patterns
- `fn_duckdb_query_patterns.R` - Query optimization
- `fn_duckdb_mamba_patterns.R` - MAMBA-specific patterns

## MP098 Reorganization (2025-08-28)

This module was reorganized according to **MP098: Generality Over Specificity Principle**:

### Major Changes Made

#### 1. Function Separation (Mixed → Separate)
- **`fn_dbCopyTable.R`** - REFACTORED: Removed DuckDB-specific optimization, now purely general DBI
- **`fn_dbDisconnect_all.R`** - REFACTORED: Removed DuckDB export logic, now purely general DBI
- **`fn_duckdb_copy_optimized.R`** - NEW: DuckDB-specific high-performance copying with ATTACH
- **`fn_duckdb_disconnect_with_export.R`** - NEW: DuckDB-specific disconnect with export/backup

#### 2. General Functions (Confirmed in Main Folder - Priority)
- `fn_dbCopyTable.R` - Pure DBI implementation, works with any database
- `fn_nrow2.R` - Safe counting for any R object
- `fn_dbDisconnect_all.R` - Pure DBI implementation, general disconnect
- `fn_data_cleaning.R` - Database-agnostic data cleaning
- `fn_list_column_handling.R` - General R list column processing
- `fn_coalesce_suffix_cols.R` - General column operations using dplyr
- `fn_dbPrintInfo.R` - Works with any DBI connection
- `fn_print_query.R` - General SQL formatting utility

#### 3. DuckDB-Specific Functions (All confirmed in duckdb/)
- `fn_duckdb_copy_optimized.R` - **NEW**: ATTACH-based high-performance copying
- `fn_duckdb_disconnect_with_export.R` - **NEW**: Export/backup functionality
- All other DuckDB functions - CONFIRMED: Already correctly placed

### MP098 Implementation Benefits

1. **Clean Separation**: No more mixed general/specific functions
2. **Priority to General**: General functions at top level for maximum reuse
3. **Performance Options**: Choose general (compatible) or specific (optimized)
4. **Clear API**: Easy to understand which functions work universally vs specifically
5. **Maintainability**: Changes to general functions benefit all database types

### Archived Functions
The following functions remain archived in `archive_20250828/`:
- `fn_dbConnect_from_list.R` - Deprecated, complex sync control
- `fn_dbCopyorReadTemp.R` - DuckDB-specific, unused
- `fn_dbDeletedb.R` - Dangerous, unused

## Testing

Comprehensive testing for tbl2 is available in the `tbl2/` subdirectory:
- `tbl2_test.R` - Main test suite
- `test_simple.R` - Simple connection tests
- `test_syntax_detection.R` - SQL syntax detection
- `test_attached_database.R` - DuckDB attached databases
- `test_tbl2_attached.R` - tbl2 with attached databases

## Usage Examples

### Standard Database Connection Pattern
```r
# Initialize using autoinit
autoinit()

# Connect to databases
raw_data <- dbConnectDuckdb(db_path_list$raw_data, read_only = FALSE)
staged_data <- dbConnectDuckdb(db_path_list$staged_data, read_only = FALSE)

# Use tbl2 for data access
sales_data <- tbl2(raw_data, "df_eby_sales_raw") %>%
  filter(!is.na(sale_date)) %>%
  collect()

# Copy processed data using general function
dbCopyTable(raw_data, staged_data, "df_eby_sales_raw", "df_eby_sales_processed", overwrite = TRUE)

# Clean up connections
dbDisconnect_all()
```

### Data Processing Pipeline
```r
# Load and clean data
data <- tbl2(con, "raw_sales") %>%
  collect()

# Safe row count check
if (nrow2(data) == 0) {
  stop("No data to process")
}

# Clean data
data_clean <- data_cleaning(data, 
  remove_duplicates = TRUE,
  standardize_names = TRUE)

# Handle list columns
if (has_list_columns(data_clean)) {
  data_clean <- handle_list_columns(data_clean)
}

# Write back to database
dbWriteTable(con, "clean_sales", data_clean, overwrite = TRUE)

# Copy to archive database (works with any database type)
archive_con <- dbConnect(RSQLite::SQLite(), "archive.db")
dbCopyTable(con, archive_con, "clean_sales", "sales_archive_2025")
dbDisconnect(archive_con)
```

## Principles and Rules

This module implements:
- **MP098**: Generality Over Specificity Principle
- **R100**: Database Access via tbl() Rule
- **R101**: Unified tbl-like Data Access Pattern
- **R93**: Function Placement Rule
- **R99**: Test App Building Principles
- **R116**: Enhanced Data Access with tbl2 and safe utilities