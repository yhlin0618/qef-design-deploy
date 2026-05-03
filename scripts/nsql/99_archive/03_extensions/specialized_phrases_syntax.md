# Specialized Phrases Syntax in NSQL

This document defines the syntax for specialized data processing directives in NSQL.

## TRANSFORM Directive

The TRANSFORM directive is used to apply transformations to data sources.

### Basic Syntax

```
TRANSFORM {data_source} TO {output}
=== Transformation Rules ===
{transformation_rules}
```

Where:
- `{data_source}` is the data to be transformed (table, dataframe, or file)
- `{output}` is the destination for the transformed data
- `{transformation_rules}` specify the transformations to apply

### Example

```
TRANSFORM app_data.df_customer_data TO "data/processed/customers_clean.rds"
=== Transformation Rules ===
COLUMNS = c("first_name", "last_name", "email")
FUNCTION = tolower
FILTER = !is.na(email) & status == "active"
SAVE_RESULT = TRUE
```

## ANALYZE RFM Directive

The ANALYZE RFM directive is used to compute Recency, Frequency, Monetary metrics.

### Syntax

```
ANALYZE RFM FROM {transaction_data} TO {output}
=== RFM Parameters ===
{rfm_parameters}
```

Where:
- `{transaction_data}` is the source of transaction data
- `{output}` is the destination for RFM analysis results
- `{rfm_parameters}` specify the RFM analysis configuration

### Example

```
ANALYZE RFM FROM app_data.df_transactions TO "data/metrics/customer_rfm.rds"
=== RFM Parameters ===
CUSTOMER_ID = customer_id
DATE = transaction_date
AMOUNT = total_amount
SAVE_RESULT = TRUE
SEGMENTS = TRUE
```

## EXPORT DATA Directive

The EXPORT DATA directive is used to save data in various formats.

### Syntax

```
EXPORT {data_source} TO {output_path} AS {format}
=== Export Options ===
{export_options}
```

Where:
- `{data_source}` is the data to be exported
- `{output_path}` is the directory where the export will be saved
- `{format}` is the export file format (csv, excel, json, etc.)
- `{export_options}` specify format-specific export options

### Example

```
EXPORT app_data.df_customer_segments TO "reports/segments" AS csv
=== Export Options ===
FILENAME = customer_segments
ROW_NAMES = FALSE
QUOTE = TRUE
NA = ""
```

## VALIDATE SCHEMA Directive

The VALIDATE SCHEMA directive is used to check data against expected types.

### Syntax

```
VALIDATE SCHEMA OF {data_source} AGAINST {schema_definition}
=== Validation Options ===
{validation_options}
```

Where:
- `{data_source}` is the data to validate
- `{schema_definition}` is the expected schema
- `{validation_options}` specify validation behavior

### Example

```
VALIDATE SCHEMA OF customer_data AGAINST expected_schema
=== Validation Options ===
REPORT_ALL_ERRORS = TRUE
STRICT = FALSE
COERCE = FALSE
```

## R Code Generation

The specialized directives translate to R code:

```r
# For TRANSFORM
transformed_data <- source_data %>%
  dplyr::mutate_at(columns, transform_function) %>%
  dplyr::filter(filter_condition)

if (save_result) {
  saveRDS(transformed_data, file.path(output_dir, paste0(output_name, ".rds")))
}

# For ANALYZE RFM
rfm_data <- transaction_data %>%
  dplyr::group_by(customer_id_col) %>%
  dplyr::summarize(
    recency = as.numeric(difftime(Sys.Date(), max(date_col), units = "days")),
    frequency = n(),
    monetary = mean(amount_col)
  ) %>%
  dplyr::mutate(
    r_score = ntile(recency, 5),
    f_score = ntile(frequency, 5),
    m_score = ntile(monetary, 5),
    rfm_score = paste0(r_score, f_score, m_score)
  )

# For EXPORT DATA
switch(format,
  csv = write.csv(data, file.path(output_dir, paste0(output_name, ".csv")), row.names = FALSE),
  excel = writexl::write_xlsx(data, file.path(output_dir, paste0(output_name, ".xlsx"))),
  json = jsonlite::write_json(data, file.path(output_dir, paste0(output_name, ".json")))
)

# For VALIDATE SCHEMA
actual_schema <- sapply(data, class)
mismatches <- which(actual_schema != expected_schema)
```

## Grammar (EBNF)

```ebnf
specialized_directive ::= transform_directive | analyze_rfm_directive | export_directive | validate_directive

transform_directive ::= 'TRANSFORM' data_source 'TO' output delimiter transform_rules

analyze_rfm_directive ::= 'ANALYZE' 'RFM' 'FROM' data_source 'TO' output delimiter rfm_parameters

export_directive ::= 'EXPORT' data_source 'TO' output_path 'AS' format delimiter export_options

validate_directive ::= 'VALIDATE' 'SCHEMA' 'OF' data_source 'AGAINST' schema_definition delimiter validation_options

data_source ::= (table_reference | variable_name | file_path)

table_reference ::= [connection_name '.'] table_name

output ::= (table_reference | variable_name | file_path)

output_path ::= string_literal

format ::= 'csv' | 'excel' | 'json' | 'parquet' | 'feather' | identifier

schema_definition ::= identifier

delimiter ::= '===' ('Transformation' 'Rules' | 'RFM' 'Parameters' | 'Export' 'Options' | 'Validation' 'Options') '==='

transform_rules ::= (transform_property_assignment)+

rfm_parameters ::= (rfm_property_assignment)+

export_options ::= (export_property_assignment)*

validation_options ::= (validation_property_assignment)*

property_assignment ::= property_name '=' property_value

property_value ::= identifier | string_literal | boolean | number | expression
```