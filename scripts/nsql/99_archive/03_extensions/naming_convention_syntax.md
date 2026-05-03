# Naming Convention Syntax

This document defines the standardized naming conventions used throughout the Precision Marketing system to differentiate between functions, objects, scripts, and other code elements.

## Prefix-Based Type Identification

The naming system uses consistent prefixes to immediately identify the type and purpose of files and code elements:

| Prefix | Type | Description | Examples |
|--------|------|-------------|----------|
| `fn_` | Function | Reusable code unit that performs a specific task | `fn_create_table.R`, `fn_transform_data.R` |
| `sc_` | Script | Runnable procedure that executes a sequence of operations | `sc_initialization_update_mode.R`, `sc_import_data.R` |
| `df_` | Data Frame | Tabular data structure in memory | `df_customer_data`, `df_sales` |
| `tbl_` | Database Table | Persistent tabular data in a database | `tbl_customers`, `tbl_transactions` |
| `test_` | Test File | Code that tests other components | `test_create_table.R`, `test_data_import.R` |
| `MP` | Meta-Principle | Fundamental architectural concept | `MP068_language_as_index.md` |
| `R` | Rule | Specific implementation guideline | `R092_universal_dbi_approach.md` |
| `D` | Derivation | Document defining derived components | `D00_create_app_data_frames.md` |

## File Naming Conventions

### Code Files

```
{prefix}_{descriptive_name}.{extension}
```

Examples:
- `fn_generate_create_table_query.R` - Function for generating table creation SQL
- `sc_initialization_update_mode.R` - Script for initializing the update mode
- `test_database_connection.R` - Tests for database connection functionality

### Documentation Files

```
{prefix}_{descriptive_name}.md
```

Examples:
- `fn_generate_create_table_query.md` - Documentation for table creation function
- `sc_initialization.md` - Documentation for initialization scripts
- `df_customer_profile.md` - Documentation for customer profile data structure

## Variable Naming Conventions

### Functions

Functions use the `snake_case` naming convention with a verb-first approach:

```
{verb}_{object}[_{modifier}]
```

Examples:
- `create_table()`
- `transform_data()`
- `validate_input()`
- `generate_report_summary()`

### Objects

Objects use `snake_case` with type-based prefixes for clarity:

```
{type_prefix}_{descriptive_name}
```

Examples:
- `df_customer_data` - Data frame containing customer information
- `tbl_sales` - Database table containing sales records
- `lst_parameters` - List containing parameter values
- `vec_customer_ids` - Vector of customer IDs
- `con_app_data` - Database connection to app_data

## Function Parameter Naming

Function parameters follow consistent patterns:

```
{descriptive_name}[_{type_indicator}]
```

Examples:
- `table_name` - Name of a table
- `column_defs_lst` - List of column definitions
- `data_df` - Data frame parameter
- `transform_fn` - Function parameter (for callbacks)

## Script Section Naming

Within scripts, section names use all caps with underscores:

```
# SECTION_NAME
```

Examples:
```r
# INITIALIZATION
# ...initialization code...

# DATA_PROCESSING
# ...processing code...

# CLEANUP
# ...cleanup code...
```

## Implementation in Code

Example of consistent naming in an R script:

```r
#' @file fn_transform_customer_data.R
#' @author Claude
#' @date 2025-04-16

#' Transform customer data by applying standardization rules
#'
#' @param data_df data.frame Customer data to transform
#' @param rules_lst list Rules to apply during transformation
#' @return data.frame Transformed customer data
fn_transform_customer_data <- function(data_df, rules_lst) {
  # VALIDATION
  if (!is.data.frame(data_df)) {
    stop("data_df must be a data frame")
  }
  
  # TRANSFORMATION
  result_df <- data_df
  
  for (rule_fn in rules_lst) {
    result_df <- rule_fn(result_df)
  }
  
  # RETURN
  return(result_df)
}
```

## Implementation in NSQL

Example of referencing functions and objects in NSQL:

```
TRANSFORM df_customer_data USING fn_transform_customer_data
=== Transformation Parameters ===
rules_lst = c(
  fn_standardize_names,
  fn_validate_emails,
  fn_categorize_customers
)
output = df_processed_customers
```

## Benefits

This naming convention provides several benefits:

1. **Immediate type identification** - The purpose of each file and variable is clear at a glance
2. **Self-documenting code** - Names explicitly indicate types and purposes
3. **Improved searchability** - Easy to find all functions, scripts, or specific object types
4. **Reduced naming conflicts** - Different types of entities can have similar descriptive names
5. **IDE support** - Auto-completion becomes more useful with prefixed names

## Related Principles

- **MP001: Primitive Terms and Definitions** - Establishes core terminology
- **MP068: Language as Index Meta-Principle** - Uses naming as an indexing mechanism
- **R001: File Naming Convention** - Defines broader file naming rules
- **R019: Object Naming Convention** - Defines broader object naming rules