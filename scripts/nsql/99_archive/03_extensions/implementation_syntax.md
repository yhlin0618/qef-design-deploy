# IMPLEMENT Directive Syntax

This document defines the syntax for the IMPLEMENT directive in NSQL.

## Basic Syntax

The IMPLEMENT directive follows a three-part format:

```
IMPLEMENT [entity_id] IN [location] WITH [options]
=== Implementation Details ===
[implementation_content]
```

### Directive Header

- `IMPLEMENT` - The directive keyword
- `[entity_id]` - Identifier of the entity being implemented (e.g., D00_01_00)
- `IN [location]` - Where the implementation should be placed (e.g., update_scripts)
- `WITH [options]` - Optional parameters for implementation (e.g., automated_execution=TRUE)

### Delimiter

The delimiter `=== Implementation Details ===` separates the directive header from the implementation content.

### Implementation Content

The implementation content can be:

1. Direct R code
2. NSQL implementation phrases
3. A combination of both

## Implementation Phrases

Implementation phrases are shorthand for common code patterns. They follow this syntax:

```
PHRASE_NAME
  $parameter1 = value1
  $parameter2 = value2
```

Example:

```
IMPLEMENT TABLE CREATION
  $connection = app_data
  $table_name = df_customer_profile
  $column_definitions = list(...)
  $primary_key = c("customer_id", "platform_id")
```

## Complete Example

```
IMPLEMENT D00_01_00 IN update_scripts WITH automated_execution=TRUE
=== Implementation Details ===
INITIALIZE IN UPDATE_MODE
CONNECT TO APP_DATA

# Generate CREATE TABLE query for customer profile
create_customer_profile_query <- generate_create_table_query(
  con = app_data,
  or_replace = TRUE,
  target_table = "df_customer_profile",
  source_table = NULL,
  column_defs = list(
    list(name = "customer_id", type = "INTEGER"),
    list(name = "buyer_name", type = "VARCHAR"),
    list(name = "email", type = "VARCHAR"),
    list(name = "platform_id", type = "INTEGER", not_null = TRUE),
    list(name = "display_name", type = "VARCHAR",
         generated_as = "buyer_name || ' (' || email || ')'")
  ),
  primary_key = c("customer_id", "platform_id"),
  indexes = list(
    list(columns = "platform_id")
  )
)

# Execute the query
dbExecute(app_data, create_customer_profile_query)
```

## Grammar (EBNF)

```ebnf
implement_directive ::= directive_header delimiter implementation_content

directive_header ::= 'IMPLEMENT' entity_id location [options]

entity_id ::= identifier

location ::= 'IN' path

options ::= 'WITH' option (',' option)*

option ::= identifier '=' value

delimiter ::= '=== Implementation Details ==='

implementation_content ::= (r_code | implementation_phrase)+

implementation_phrase ::= phrase_name [phrase_parameters]

phrase_name ::= identifier

phrase_parameters ::= ('$' parameter_name '=' parameter_value)+

r_code ::= any_valid_r_code
```