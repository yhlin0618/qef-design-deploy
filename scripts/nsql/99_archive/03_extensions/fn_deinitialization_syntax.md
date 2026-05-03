# Deinitialization Syntax in NSQL

This document defines the syntax for deinitialization directives in NSQL, which are used to properly clean up resources and report script status.

## DEINITIALIZE Directive

The `DEINITIALIZE` directive provides a standardized way to clean up resources and finalize script execution.

### Basic Syntax

```
DEINITIALIZE [IN context] [WITH options]
=== Deinitialization Details ===
[deinitialization_parameters]
```

Where:
- `[context]` specifies the execution context (e.g., UPDATE_MODE, APP_MODE)
- `[options]` are optional parameters to control the deinitialization behavior
- `[deinitialization_parameters]` define specific configuration for the deinitialization process

### Contexts

The directive supports several operational contexts:

1. **UPDATE_MODE**: For scripts operating in update/modification mode
   ```
   DEINITIALIZE IN UPDATE_MODE
   ```

2. **APP_MODE**: For scripts operating within a Shiny application
   ```
   DEINITIALIZE IN APP_MODE
   ```

3. **CUSTOM**: For custom deinitialization process
   ```
   DEINITIALIZE WITH custom_params=TRUE
   === Deinitialization Details ===
   $cleanup_list = c("temp_var1", "temp_var2")
   $preserve_vars = c("output_data")
   ```

### DEINITIALIZE_SYNTAX

For more complex customization, the `DEINITIALIZE_SYNTAX` directive allows detailed parameter specification:

```
DEINITIALIZE_SYNTAX
  $close_connections = if(exists("connection_created") && connection_created) dbDisconnect(app_data)
  $reset_environment = rm(list = setdiff(ls(), c("test_passed", "output_data")))
  $report_status = TRUE
```

### Parameters

The deinitialization parameters section supports the following parameters:

```
$close_connections = [connection closure expression]
$reset_environment = [environment cleanup expression]
$preserve_vars = [vector of variables to preserve]
$cleanup_list = [vector of variables to remove]
$report_status = [TRUE|FALSE]
```

Where:
- `$close_connections` - Expression for closing open connections
- `$reset_environment` - Expression for cleaning up environment variables
- `$preserve_vars` - List of variables to preserve during environment cleanup
- `$cleanup_list` - Specific variables to remove
- `$report_status` - Whether to report final execution status

## Examples

### Basic Standard Deinitialization in UPDATE_MODE

```
DEINITIALIZE IN UPDATE_MODE
```

### Custom Deinitialization with Specific Resource Cleanup

```
DEINITIALIZE_SYNTAX
  $close_connections = if(exists("db_connection") && inherits(db_connection, "DBIConnection")) dbDisconnect(db_connection)
  $reset_environment = rm(list = setdiff(ls(), c("output_data", "final_status")))
  $report_status = TRUE
```

### Deinitialization with Specific Variable Cleanup

```
DEINITIALIZE WITH preserve_data=TRUE
=== Deinitialization Details ===
$cleanup_list = c("temp_df", "intermediate_result", "parsed_data")
$preserve_vars = c("customer_data", "final_result")
```

## R Code Generation

The directive translates to R code using the appropriate deinitialization function:

```r
# Standard deinitialization in UPDATE_MODE
# Set final status if not already set
if (!exists('final_status')) {
  if (exists('test_passed') && test_passed) {
    final_status <- TRUE
  } else {
    final_status <- FALSE
  }
}

# Use standard deinitialization script
source(file.path('update_scripts', 'global_scripts', '00_principles', 'sc_deinitialization_update_mode.R'))

# Custom deinitialization with syntax parameters
tryCatch({
  # Close connections
  if(exists("db_connection") && inherits(db_connection, "DBIConnection")) {
    dbDisconnect(db_connection)
  }
  
  # Cleanup environment
  rm(list = setdiff(ls(), c("output_data", "final_status")))
  
  # Report status
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
  message("Script execution completed at ", Sys.time())
})
```

## Implementation Details

The `DEINITIALIZE` directive is implemented through two primary mechanisms:

1. **Standard Deinitialization**: Uses predefined deinitialization scripts for standard contexts
   - `sc_deinitialization_update_mode.R` for UPDATE_MODE context
   - `sc_deinitialization_app_mode.R` for APP_MODE context

2. **Custom Deinitialization**: Uses the `DEINITIALIZE_SYNTAX` directive to create custom deinitialization with specified parameters.

Both implementations adhere to the Deinitialization Final Principle (MP033), ensuring all resources are properly closed and the environment is cleaned up.

## Grammar (EBNF)

```ebnf
deinitialize_directive ::= 'DEINITIALIZE' [context] [options] [delimiter deinitialization_parameters]

context ::= 'IN' identifier

options ::= 'WITH' option (',' option)*

option ::= identifier '=' value

delimiter ::= '=== Deinitialization Details ==='

deinitialization_parameters ::= (parameter_assignment)*

parameter_assignment ::= '$' parameter_name '=' parameter_value

deinitialize_syntax_directive ::= 'DEINITIALIZE_SYNTAX' [parameters]

parameters ::= (parameter_assignment)+
```

## Best Practices

1. **Use Standard Scripts When Possible**: For most cases, the standard deinitialization scripts provide sufficient functionality.

2. **Always Set Final Status**: Ensure the script returns a final status indicating success or failure.

3. **Clean Up Temporary Resources**: Always clean up temporary files, connections, and objects.

4. **Handle Errors Properly**: Wrap deinitialization in tryCatch to handle errors gracefully.

5. **Preserve Important Results**: Use the $preserve_vars parameter to keep important results.

## Related Principles and Rules

- **R113: Update Script Structure Rule** - Mandates the four-part structure with DEINITIALIZE section
- **MP033: Deinitialization Final** - Ensures proper cleanup of resources
- **MP031: Initialization First** - Pairs with Deinitialization Final for proper resource lifecycle
- **MP071: Capitalization Convention** - DEINITIALIZE as a capitalized NSQL directive
- **MP072: Cognitive Distinction Principle** - Visual distinction for NSQL directives