# SQL Utilities

This directory contains utility functions for SQL operations, following the "one function per file" pattern.

## Functions

- `100g_sanitize_identifier.R` - Replaces hyphens with underscores in database identifiers
- `101g_quote_identifier.R` - Properly quotes database identifiers as needed
- `102g_process_column_def.R` - Processes column definition strings
- `103g_generate_column_definitions.R` - Generates column definitions for SQL CREATE TABLE statements
- `104g_create_summary_table.R` - Creates or replaces a database table with proper column definitions
- `105g_read_csvxlsx.R` - Reads CSV or Excel files based on file extension

## Dependency Order

These functions have dependencies on each other. The initialization script should load them in the correct order:

1. `100g_sanitize_identifier.R` - No dependencies
2. `101g_quote_identifier.R` - Depends on `sanitize_identifier`
3. `102g_process_column_def.R` - Depends on `quote_identifier`
4. `103g_generate_column_definitions.R` - Depends on `sanitize_identifier`
5. `104g_create_summary_table.R` - Depends on `process_column_def` and `quote_identifier`
6. `105g_read_csvxlsx.R` - No dependencies on other SQL functions