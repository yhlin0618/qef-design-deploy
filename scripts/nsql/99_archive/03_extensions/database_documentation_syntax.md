# Database Documentation Syntax in NSQL

This document defines the syntax for database documentation directives in NSQL.

## DOCUMENT Directive

The DOCUMENT directive is used to generate documentation for database objects.

### Document Table Syntax

```
DOCUMENT TABLE {table_name} FROM {connection} TO {output_path}
=== Documentation Parameters ===
{parameters}
```

Where:
- `{table_name}` is the name of the table to document
- `{connection}` is the database connection name
- `{output_path}` is the path where documentation will be saved
- `{parameters}` are optional configuration parameters

### Document Database Syntax

```
DOCUMENT DATABASE {connection} TO {output_path}
=== Documentation Parameters ===
{parameters}
```

Where:
- `{connection}` is the database connection name
- `{output_path}` is the path where documentation will be saved
- `{parameters}` are optional configuration parameters

### Example

```
DOCUMENT DATABASE app_data TO "docs/database"
=== Documentation Parameters ===
format = "markdown"
include_indexes = TRUE
include_constraints = TRUE
include_foreign_keys = TRUE
```

## GENERATE Directive for ER Diagrams

The GENERATE directive is used to create visual representations of database structure.

### Syntax

```
GENERATE ER DIAGRAM FROM {connection} TO {output_path}
=== Diagram Parameters ===
{parameters}
```

Where:
- `{connection}` is the database connection name
- `{output_path}` is the path where the diagram will be saved
- `{parameters}` are optional configuration parameters

### Example

```
GENERATE ER DIAGRAM FROM app_data TO "docs/diagrams"
=== Diagram Parameters ===
format = "dot"
include_all_tables = TRUE
highlight_primary_keys = TRUE
show_data_types = FALSE
```

## R Code Generation

The documentation directives translate to R code that interacts with the database and generates documentation:

```r
# For DOCUMENT TABLE
table_info <- dbGetQuery(connection, paste0("PRAGMA table_info('", table_name, "');"))
table_doc <- data.frame(...)
write.csv(table_doc, file.path(output_dir, paste0(table_name, "_documentation.csv")))

# For DOCUMENT DATABASE
tables <- dbListTables(connection)
table_docs <- lapply(tables, function(table) {...})
full_doc <- do.call(rbind, table_docs)
write.csv(full_doc, file.path(output_dir, "database_documentation.csv"))

# For GENERATE ER DIAGRAM
tables <- dbListTables(connection)
relations <- lapply(tables, function(table) {...})
writeLines(dot_representation, file.path(output_dir, "er_diagram.dot"))
```

## Grammar (EBNF)

```ebnf
document_directive ::= 'DOCUMENT' document_target 'FROM' connection_reference 'TO' output_path delimiter document_parameters

document_target ::= 'TABLE' table_name | 'DATABASE' | 'SCHEMA'

generate_directive ::= 'GENERATE' 'ER' 'DIAGRAM' 'FROM' connection_reference 'TO' output_path delimiter diagram_parameters

connection_reference ::= identifier

output_path ::= string_literal

delimiter ::= '===' ('Documentation' | 'Diagram') 'Parameters' '==='

document_parameters ::= (parameter_assignment)*

diagram_parameters ::= (parameter_assignment)*

parameter_assignment ::= identifier '=' (string_literal | boolean | number)

boolean ::= 'TRUE' | 'FALSE'
```