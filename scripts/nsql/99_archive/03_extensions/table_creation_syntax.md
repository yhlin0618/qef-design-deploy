# Table Creation Syntax in NSQL

This document defines the syntax for table creation directives in NSQL.

## CREATE Directive

The CREATE directive is used to define tables within a database connection.

### Basic Syntax

```
CREATE {table_name} AT {connection}[.{schema}]
=== CREATE TABLE {table_name} ===
{sql_definition}
```

Where:
- `{table_name}` is the name of the table to be created
- `{connection}` is the database connection name
- `{schema}` (optional) is the schema name within the connection
- `{sql_definition}` is a valid SQL CREATE TABLE statement

### Examples

```
CREATE df_customer_profile AT app_data
=== CREATE TABLE df_customer_profile ===
CREATE OR REPLACE TABLE df_customer_profile (
  customer_id INTEGER,
  buyer_name VARCHAR,
  email VARCHAR,
  platform_id INTEGER NOT NULL,
  display_name VARCHAR GENERATED ALWAYS AS (buyer_name || ' (' || email || ')') VIRTUAL,
  PRIMARY KEY (customer_id, platform_id)
);

CREATE INDEX IF NOT EXISTS idx_df_customer_profile_platform_id_df_customer_profile ON df_customer_profile(platform_id);
```

## CREATE TEMPORARY Directive

For temporary tables that exist only for the duration of a session.

### Syntax

```
CREATE TEMPORARY {table_name} AT {connection}[.{schema}]
=== CREATE TEMPORARY TABLE {table_name} ===
{sql_definition}
```

### Example

```
CREATE TEMPORARY df_temp_results AT app_data
=== CREATE TEMPORARY TABLE df_temp_results ===
CREATE TEMPORARY TABLE df_temp_results (
  id INTEGER PRIMARY KEY,
  result_value DOUBLE,
  calculation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## R Code Generation

The table creation directives translate to R code using the `generate_create_table_query` function:

```r
generate_create_table_query(
  con = connection,
  or_replace = TRUE,
  target_table = "table_name",
  source_table = NULL,
  column_defs = list(...),
  primary_key = ...,
  indexes = list(...)
)
```

## Grammar (EBNF)

```ebnf
create_directive ::= 'CREATE' [temporary_modifier] table_name 'AT' connection_reference delimiter sql_definition

temporary_modifier ::= 'TEMPORARY'

table_name ::= identifier

connection_reference ::= identifier ['.' identifier]

delimiter ::= '===' ('CREATE' | 'CREATE TEMPORARY') 'TABLE' table_name '==='

sql_definition ::= sql_create_table_statement [sql_create_index_statement]*

sql_create_table_statement ::= 'CREATE' ['OR REPLACE'] ['TEMPORARY'] 'TABLE' [if_not_exists] table_name '(' column_definition_list ')' ';'

sql_create_index_statement ::= 'CREATE' ['UNIQUE'] 'INDEX' [if_not_exists] index_name 'ON' table_name '(' column_list ')' ';'

if_not_exists ::= 'IF NOT EXISTS'

column_definition_list ::= column_definition (',' column_definition)*

column_definition ::= column_name data_type [column_constraint]*

column_constraint ::= 'NOT NULL' | 'PRIMARY KEY' | 'UNIQUE' | default_clause | check_clause | generated_clause

generated_clause ::= 'GENERATED ALWAYS AS' '(' expression ')' ['STORED' | 'VIRTUAL']
```