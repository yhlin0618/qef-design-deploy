# IMPLEMENT Extension for NSQL

This directory contains resources for implementing the IMPLEMENT directive in NSQL (Natural SQL Language).

## Purpose

The IMPLEMENT directive extends NSQL to allow explicit implementation instructions for various entities defined in derivation documents. It provides a standardized way to translate high-level data definitions into executable code.

## Files

- `implementation_phrases.md` - Documents the standard implementation phrases used in NSQL directives
- `implementation_phrase_registry.xlsx` - Registry of all implementation phrases with their associated code patterns
- `syntax.md` - Defines the syntax rules for the IMPLEMENT directive

## Using the IMPLEMENT Directive

The IMPLEMENT directive uses a three-part format:

1. Directive header: `IMPLEMENT [entity_id] IN [location] WITH [options]`
2. Delimiter: `=== Implementation Details ===`
3. Implementation body: The actual implementation details using NSQL implementation phrases or direct R code

Example:

```
IMPLEMENT D00_01_00 IN update_scripts WITH automated_execution=TRUE
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

## Implementation Process

1. Define the implementation using the IMPLEMENT directive in a derivation document
2. Use the implementation_extension.R parser to extract and translate the implementation
3. Generate the implementation script in the specified location
4. Execute the implementation if automated execution is specified

## Extending the System

To add new implementation phrases:

1. Update the implementation_phrases.md document
2. Add the phrase to the implementation_phrase_registry.xlsx file
3. Update the implementation_extension.R file to support the new phrase pattern