# NSQL Extensions

This directory contains extensions to the Natural SQL Language (NSQL) as defined in MP24. These extensions provide domain-specific functionality and syntax for specialized use cases.

## Available Extensions

- **time_series_extension.R**: Extensions for time series analysis
- **marketing_extension.R**: Extensions for marketing analytics
- **ecommerce_extension.R**: Extensions for e-commerce analysis
- **ml_extension.R**: Extensions for machine learning operations
- **database_documentation_extension.R**: Extensions for database documentation (implements MP43)

## Extension Interface

All extensions implement the following interface:

```r
register_extension <- function(registry) {
  # Register extension components with the NSQL registry
  # Returns the updated registry
}
```

Extensions can add:
- New functions
- New operators
- New statement patterns
- New translation rules

## Extension Components

### Functions

Extensions can define new functions with:
- Function name
- Parameter definitions
- Translation rules for each target language
- Documentation

### Patterns

Extensions can define new statement patterns with:
- Pattern syntax
- Translation to core NSQL
- Disambiguation rules

## Extension Registry

Extensions are registered with the central NSQL registry, which maintains:
- Function definitions
- Operator definitions
- Pattern definitions
- Translation rules

## Usage Example

```r
library(nsql)
library(nsql.extensions.timeseries)

# Register time series extension
nsql_registry <- register_extension(nsql_registry, "timeseries")

# Use time series functions in NSQL
nsql_statement <- "
  transform SalesData to TimeSeriesAnalysis as
    time_series_decompose(sales, period=12) as decomposition,
    seasonal_pattern(sales, frequency='monthly') as seasonality,
    trend_direction(sales, window=6) as trend
    grouped by product_category
"

# Parse and translate with extension support
ast <- parse_nsql(nsql_statement, registry = nsql_registry)
sql <- translate_nsql(ast, target = "sql", registry = nsql_registry)
```

## Database Documentation Extension

The Database Documentation Extension implements MP43 (Database Documentation Principle) by providing SNSQL commands for generating database documentation:

```r
# Register database documentation extension
nsql_registry <- register_extension(nsql_registry, "database_documentation")

# Use database documentation commands
nsql_statement <- "summarize database 'app_data.duckdb' to 'docs/database/app_structure.md' with sample_rows=10"
nsql_statement <- "document all databases in 'docs/database'"

# Parse and execute
result <- execute_nsql(nsql_statement, registry = nsql_registry)
```

This extension provides specialized commands for creating comprehensive documentation of database structures and contents, following the project's established documentation standards.