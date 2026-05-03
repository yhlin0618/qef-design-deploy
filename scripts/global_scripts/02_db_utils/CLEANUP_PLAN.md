# 02_db_utils Cleanup Plan

## Analysis Date: 2025-08-28

## Usage Analysis Results

### Actively Used Functions (KEEP in 02_db_utils)
1. **fn_dbConnectDuckdb.R** - Primary connection function used in all update scripts
2. **fn_data_cleaning.R** - Data cleaning utilities (used in components)
3. **fn_list_column_handling.R** - Handle list columns in data processing
4. **fn_coalesce_suffix_cols.R** - Coalesce columns with suffixes (used in joins)
5. **fn_dbDisconnect_all.R** - Clean disconnect of all connections (used in autoinit)
6. **fn_dbPrintInfo.R** - Print database info for debugging
7. **fn_get_default_db_paths.R** - Get default database paths
8. **fn_set_db_paths.R** - Set database paths  
9. **fn_print_query.R** - Print SQL queries for debugging

### DuckDB-Specific Functions (MOVE to duckdb/)
1. **fn_dbAttachDuckdb.R** - DuckDB-specific attach functionality
2. **fn_dbDetachDuckdb.R** - DuckDB-specific detach functionality
3. **fn_dbDetach_all.R** - Detach all DuckDB databases
4. **fn_export_duckdb_dataframes.R** - Export DuckDB dataframes

### Redundant/Deprecated Functions (REMOVE)
1. **fn_dbConnect_from_list.R** - Complex, deprecated (shows warning), replaced by fn_dbConnectDuckdb
2. **fn_dbCopyTable.R** - Not used anywhere except in archived files
3. **fn_dbCopyorReadTemp.R** - Not used anywhere except in archived files
4. **fn_dbDeletedb.R** - Dangerous function, not used anywhere
5. **fn_dbOverwrite.R** - Not actively used, potentially dangerous
6. **fn_nrow2.R** - Duplicate exists in 04_utils/base/fn_nrow2.R (should use that one)

## Rationale for Each Decision

### Functions to Keep
- **fn_dbConnectDuckdb.R**: Essential - used by all ETL scripts for database connections
- **fn_data_cleaning.R**: Provides data cleaning utilities used in Shiny components
- **fn_list_column_handling.R**: Critical for handling complex data structures
- **fn_coalesce_suffix_cols.R**: Used in join operations to handle duplicate columns
- **fn_dbDisconnect_all.R**: Essential for cleanup in autodeinit()
- **fn_dbPrintInfo.R**: Useful debugging tool
- **fn_get/set_default_db_paths.R**: Core path management
- **fn_print_query.R**: SQL debugging utility

### Functions to Move to duckdb/
- **fn_dbAttachDuckdb.R**: DuckDB-specific functionality
- **fn_dbDetachDuckdb.R**: DuckDB-specific functionality  
- **fn_dbDetach_all.R**: DuckDB-specific detach operations
- **fn_export_duckdb_dataframes.R**: DuckDB-specific export functionality

### Functions to Remove
- **fn_dbConnect_from_list.R**: 
  - Already deprecated with warning message
  - Overly complex with sync control, permissions, etc.
  - Replaced by simpler fn_dbConnectDuckdb
  
- **fn_dbCopyTable.R**:
  - No usage in update scripts
  - Only referenced in archived files
  
- **fn_dbCopyorReadTemp.R**:
  - No usage in active code
  - Only in archived files
  
- **fn_dbDeletedb.R**:
  - Dangerous function (deletes database files)
  - No usage anywhere
  - Risk outweighs benefit
  
- **fn_dbOverwrite.R**:
  - Not used in update scripts
  - Potentially dangerous
  - Can use standard DBI functions instead
  
- **fn_nrow2.R**:
  - Duplicate - already exists in 04_utils/base/fn_nrow2.R
  - Should use the one in 04_utils/base/

## Migration Steps

### Phase 1: Move DuckDB-specific functions
1. Move fn_dbAttachDuckdb.R → duckdb/fn_dbAttachDuckdb.R
2. Move fn_dbDetachDuckdb.R → duckdb/fn_dbDetachDuckdb.R
3. Move fn_dbDetach_all.R → duckdb/fn_dbDetach_all.R
4. Move fn_export_duckdb_dataframes.R → duckdb/fn_export_duckdb_dataframes.R

### Phase 2: Remove deprecated/unused functions
1. Archive fn_dbConnect_from_list.R (already deprecated)
2. Archive fn_dbCopyTable.R (unused)
3. Archive fn_dbCopyorReadTemp.R (unused)
4. Archive fn_dbDeletedb.R (dangerous, unused)
5. Archive fn_dbOverwrite.R (unused)
6. Remove fn_nrow2.R (duplicate)

### Phase 3: Update documentation
1. Update README.md to reflect new structure
2. Update SUMMARY.md with cleanup results
3. Document rationale in principle files if needed

## Impact Assessment

### Low Risk Changes
- Moving DuckDB-specific functions: These are loaded recursively anyway
- Removing unused functions: No impact on active code

### Medium Risk Changes  
- Removing fn_dbConnect_from_list.R: Already shows deprecation warning
- Removing fn_nrow2.R: Code should use 04_utils/base/fn_nrow2.R

### Testing Requirements
1. Run all update scripts to ensure connections work
2. Test autoinit() and autodeinit() functions
3. Verify Shiny components still work

## Final Structure

```
02_db_utils/
├── README.md
├── SUMMARY.md
├── CLEANUP_PLAN.md (this file)
├── duckdb/
│   ├── fn_duckdb_connection.R (existing)
│   ├── fn_duckdb_import_export.R (existing)
│   ├── fn_duckdb_list_columns.R (existing)
│   ├── fn_duckdb_mamba_patterns.R (existing)
│   ├── fn_duckdb_nested_types.R (existing)
│   ├── fn_duckdb_query_patterns.R (existing)
│   ├── fn_duckdb_type_conversion.R (existing)
│   ├── fn_dbAttachDuckdb.R (moved)
│   ├── fn_dbDetachDuckdb.R (moved)
│   ├── fn_dbDetach_all.R (moved)
│   └── fn_export_duckdb_dataframes.R (moved)
├── fn_dbConnectDuckdb.R (keep - primary function)
├── fn_data_cleaning.R (keep)
├── fn_list_column_handling.R (keep)
├── fn_coalesce_suffix_cols.R (keep)
├── fn_dbDisconnect_all.R (keep)
├── fn_dbPrintInfo.R (keep)
├── fn_get_default_db_paths.R (keep)
├── fn_set_db_paths.R (keep)
├── fn_print_query.R (keep)
└── tbl2/
    ├── fn_tbl2.R
    └── tests...
```

## Estimated Cleanup Benefits
- Reduce function count from 19 to 9 in main directory
- Remove 3000+ lines of redundant/deprecated code
- Clearer separation of DuckDB-specific vs general utilities
- Reduced confusion from duplicate functions
- Safer codebase (removed dangerous unused functions)