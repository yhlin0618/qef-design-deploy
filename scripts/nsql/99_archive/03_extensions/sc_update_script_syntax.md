# Update Script Syntax

This document defines the standardized structure for update scripts (`sc_` prefix files) in the Precision Marketing system.

## Update Script Purpose

Update scripts are responsible for making changes to the system state, particularly database structures and content. They follow a consistent four-part structure to ensure reliable execution, testability, and proper resource management as specified in R113 (Four-Part Update Script Structure Rule).

## Update Script Naming Convention

Update scripts follow the naming pattern defined in the Type-Prefix Naming Meta-Principle (MP070):

```
sc_{purpose}[_{context}].R
```

Examples:
- `sc_initialization_update_mode.R`
- `sc_create_customer_table.R`
- `sc_import_sales_data.R`
- `sc_generate_reports.R`

For implementation scripts derived from derivation documents, the pattern is:

```
D{document_id}_{section_id}[_P{platform_id}].R
```

Examples:
- `D00_01_00.R` - General implementation of section 01 in document D00
- `D01_04_P07.R` - Platform 07-specific implementation of section 04 in document D01

## Standard Four-Part Structure

All update scripts must follow this four-part structure:

```r
#' @file sc_example.R
#' @author [Author name]
#' @date [Creation date]
#' @title [Script title]
#' @description [Script description]
#' @requires [Required packages and functions]
#' @principle [Relevant principles]

# 1. INITIALIZE
# Initialization code...

# 2. MAIN
# Main processing code...

# 3. TEST
# Test/verification code...

# 4. DEINITIALIZE
# Cleanup code...
```

### 1. INITIALIZE Section

The INITIALIZE section sets up the environment and resources needed for execution:

```r
# 1. INITIALIZE
# Source initialization script for the appropriate mode
source(file.path("update_scripts", "global_scripts", "00_principles", "sc_initialization_update_mode.R"))

# Or using the INITIALIZE_SYNTAX directive
INITIALIZE_SYNTAX
  $libraries = library(dplyr)
  library(tidyr)
  library(DBI)
  library(duckdb)
  
  $environment_setup = options(stringsAsFactors = FALSE)
  options(scipen = 999)
  VERBOSE <- TRUE
  
# Connect to required databases
if (!exists("app_data") || !inherits(app_data, "DBIConnection")) {
  app_data <- dbConnect_from_list("app_data")
  connection_created <- TRUE
  message("Connected to app_data database")
} else {
  connection_created <- FALSE
}
```

Key components:
- Package loading
- Environment configuration
- Database connections
- Variable initialization
- Error handling setup

### 2. MAIN Section

The MAIN section contains the core functionality of the script:

```r
# 2. MAIN
tryCatch({
  # Step 1: Read necessary data
  existing_data <- dbGetQuery(app_data, "SELECT * FROM existing_table")
  
  # Step 2: Perform transformations
  transformed_data <- existing_data %>%
    filter(!is.na(key_column)) %>%
    mutate(
      new_column = calculate_value(value_column),
      status = case_when(
        condition1 ~ "Status1",
        condition2 ~ "Status2",
        TRUE ~ "Other"
      )
    )
  
  # Step 3: Write results
  create_table_query <- generate_create_table_query(
    con = app_data,
    or_replace = TRUE,
    target_table = "new_table",
    source_table = NULL,
    column_defs = list(
      list(name = "id", type = "INTEGER", not_null = TRUE),
      list(name = "name", type = "VARCHAR"),
      list(name = "value", type = "DOUBLE"),
      list(name = "status", type = "VARCHAR")
    ),
    primary_key = "id"
  )
  
  # Execute the query
  dbExecute(app_data, create_table_query)
  
  # Insert data
  dbWriteTable(app_data, "new_table", transformed_data, append = TRUE)
  
  message("Main processing completed successfully")
}, error = function(e) {
  message("Error in MAIN section: ", e$message)
  error_occurred <- TRUE
})
```

Key components:
- Data retrieval
- Data transformation
- Database operations
- Error handling

### 3. TEST Section

The TEST section verifies that the changes were applied correctly:

```r
# 3. TEST
if (!exists("error_occurred") || !error_occurred) {
  tryCatch({
    # Verification query
    verification_query <- "SELECT COUNT(*) as record_count FROM new_table"
    result <- dbGetQuery(app_data, verification_query)
    
    # Verify results
    if (result$record_count > 0) {
      message("Verification successful: ", result$record_count, " records created")
      test_passed <- TRUE
    } else {
      message("Verification failed: No records found in new_table")
      test_passed <- FALSE
    }
    
    # Additional tests
    # Test data integrity, column types, key constraints, etc.
    
  }, error = function(e) {
    message("Error in TEST section: ", e$message)
    test_passed <- FALSE
  })
} else {
  message("Skipping tests due to error in MAIN section")
  test_passed <- FALSE
}
```

Key components:
- Result verification
- Data integrity tests
- Constraint validation
- Performance checks
- Status reporting

### 4. DEINITIALIZE Section

The DEINITIALIZE section cleans up resources and provides a final status. There are two recommended approaches:

#### Approach 1: Standard Deinitialization Script (Preferred)

Use the standard deinitialization script for common cleanup tasks:

```r
# 4. DEINITIALIZE
tryCatch({
  # Clean up script-specific temporary objects (if any)
  if (exists("temp_data")) {
    rm(temp_data)
  }
  
  # Set final status before standard deinitialization
  if (exists("test_passed") && test_passed) {
    message("Script executed successfully with all tests passed")
    final_status <- TRUE
  } else {
    message("Script execution incomplete or tests failed")
    final_status <- FALSE
  }
  
  # Use standard deinitialization script
  source(file.path("update_scripts", "global_scripts", "00_principles", "sc_deinitialization_update_mode.R"))
  
}, error = function(e) {
  message("Error in DEINITIALIZE section: ", e$message)
  final_status <- FALSE
}, finally = {
  # This will always execute
  message("Script execution completed at ", Sys.time())
})

# Return final status
if (exists("final_status")) {
  final_status
} else {
  FALSE
}
```

#### Approach 2: Direct Cleanup

Handle all cleanup tasks directly in the script:

```r
# 4. DEINITIALIZE
tryCatch({
  # Close connections opened in this script
  if (exists("connection_created") && connection_created && 
      exists("app_data") && inherits(app_data, "DBIConnection")) {
    dbDisconnect(app_data)
    message("Database connection closed")
  }
  
  # Clean up temporary objects
  if (exists("CLEANUP_ENVIRONMENT") && CLEANUP_ENVIRONMENT) {
    rm(list = setdiff(ls(), c("test_passed", "output_data")))
  }
  
  # Report final status
  if (exists("test_passed") && test_passed) {
    message("Script executed successfully with all tests passed")
    final_status <- TRUE
  } else {
    message("Script execution incomplete or tests failed")
    final_status <- FALSE
  }
  
}, error = function(e) {
  message("Error in DEINITIALIZE section: ", e$message)
  final_status <- FALSE
}, finally = {
  # This will always execute
  message("Script execution completed at ", Sys.time())
})

# Return final status
if (exists("final_status")) {
  final_status
} else {
  FALSE
}
```

The first approach is preferred as it promotes consistency and maintainability by centralizing common deinitialization logic.

Key components:
- Resource cleanup (connections, files)
- Environment cleanup
- Status reporting
- Return value (success/failure)

## NSQL Implementation Example

The four-part structure can be expressed in NSQL:

```
IMPLEMENT D00_01_00 IN update_scripts
=== Implementation Details ===
# 1. INITIALIZE
INITIALIZE IN UPDATE_MODE
CONNECT TO APP_DATA

# 2. MAIN
IMPLEMENT TABLE CREATION
  $connection = app_data
  $table_name = df_customer_profile
  $column_definitions = list(...)
  $primary_key = c("customer_id", "platform_id")
  $indexes = list(list(columns = "platform_id"))

# 3. TEST
VALIDATE TABLE df_customer_profile
  $connection = app_data
  $expected_column_count = 5
  $expected_primary_key = c("customer_id", "platform_id")

# 4. DEINITIALIZE
# First set final status
if (exists("test_passed") && test_passed) {
  final_status <- TRUE
} else {
  final_status <- FALSE
}

# Then use standard deinitialization script
DEINITIALIZE IN UPDATE_MODE
```

## Benefits of the Four-Part Structure

1. **Consistency**: All scripts follow the same pattern, making them easier to understand and maintain
2. **Isolation**: Clear separation between setup, execution, testing, and cleanup
3. **Error Management**: Each section can have its own error handling
4. **Testability**: Dedicated testing section ensures changes are verified
5. **Resource Management**: Proper initialization and cleanup prevents resource leaks

## Related Principles

- **R113: Update Script Structure Rule** - Mandates the four-part structure for all update scripts
- **MP031: Initialization First** - Establishes proper setup before operations
- **MP033: Deinitialization Final** - Ensures proper cleanup of resources
- **MP042: Runnable First** - Scripts should be runnable without additional steps
- **MP070: Type-Prefix Naming** - Consistent naming with type prefixes
- **P076: Error Handling Patterns** - Standardized approaches to error handling