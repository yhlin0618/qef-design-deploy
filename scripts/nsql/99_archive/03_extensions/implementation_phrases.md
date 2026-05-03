# Implementation Phrases for NSQL

This document defines standard implementation phrases used in NSQL directives. These phrases provide a consistent way to translate high-level directives into executable code patterns.

## Implementation Phrase Structure

Each implementation phrase follows the pattern:

```
DIRECTIVE IN CONTEXT = "implementation_code"
```

Where:
- `DIRECTIVE` is the high-level action (e.g., INITIALIZE, IMPLEMENT, CREATE)
- `CONTEXT` defines where or how the directive applies
- `implementation_code` is the actual R code that implements the directive

## Core Implementation Phrases

### Initialization and Deinitialization Phrases

```
INITIALIZE IN UPDATE_MODE = "source(file.path('update_scripts', 'global_scripts', '00_principles', 'sc_initialization_update_mode.R'))"

INITIALIZE IN APP_MODE = "source(file.path('update_scripts', 'global_scripts', '00_principles', 'sc_initialization_app_mode.R'))"

INITIALIZE DATABASE ONLY = "source(file.path('update_scripts', 'global_scripts', '00_principles', 'sc_init_db_only.R'))"

INITIALIZE_SYNTAX = "# Initialize required libraries and dependencies
tryCatch({
  # Load required libraries
  suppressPackageStartupMessages({
    $libraries
  })
  
  # Set environment variables and parameters
  $environment_setup
  
  message('Initialization completed successfully')
}, error = function(e) {
  message('Error during initialization: ', e$message)
  return(FALSE)
})"

DEINITIALIZE_SYNTAX = "# Clean up resources and connections
tryCatch({
  # Close open connections
  $close_connections
  
  # Reset environment variables
  $reset_environment
  
  message('Deinitialization completed successfully')
}, error = function(e) {
  message('Error during deinitialization: ', e$message)
  return(FALSE)
})"
```

### Database Connection Phrases

```
CONNECT TO APP_DATA = "app_data <- dbConnect_from_list('app_data')"

CONNECT TO RAW_DATA = "raw_data <- dbConnect_from_list('raw_data')"

DISCONNECT ALL = "dbDisconnect_all()"
```

### Implementation Phrases

```
IMPLEMENT TABLE CREATION = "
# Generate and execute CREATE TABLE query
create_table_query <- generate_create_table_query(
  con = $connection,
  or_replace = TRUE,
  target_table = '$table_name',
  source_table = NULL,
  column_defs = $column_definitions,
  primary_key = $primary_key,
  indexes = $indexes
)
dbExecute($connection, create_table_query)
"
```

### Data Transformation Phrases

```
TRANSFORM DATA = "
# Transform data using specified function
transformed_data <- $transform_function(
  $source_data,
  $transform_parameters
)
"
```

## Using Implementation Phrases

Implementation phrases can be used in NSQL directives to specify how a high-level concept should be implemented in code:

```
IMPLEMENT D00_01_00 IN update_scripts
=== Implementation Details ===
INITIALIZE IN UPDATE_MODE
CONNECT TO APP_DATA

IMPLEMENT TABLE CREATION
  $connection = app_data
  $table_name = df_customer_profile
  $column_definitions = list(
    list(name = "customer_id", type = "INTEGER"),
    list(name = "buyer_name", type = "VARCHAR"),
    list(name = "email", type = "VARCHAR"),
    list(name = "platform_id", type = "INTEGER", not_null = TRUE),
    list(name = "display_name", type = "VARCHAR",
         generated_as = "buyer_name || ' (' || email || ')'")
  )
  $primary_key = c("customer_id", "platform_id")
  $indexes = list(
    list(columns = "platform_id")
  )
```

## Extending Implementation Phrases

New implementation phrases can be added to the system by:

1. Updating this document with the new phrase definition
2. Adding the phrase to the implementation_phrase_registry.xlsx file
3. Updating the implementation_extension.R file to support the new phrase pattern