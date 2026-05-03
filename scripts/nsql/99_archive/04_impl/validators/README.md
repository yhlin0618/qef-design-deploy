# NSQL Validators

This directory contains validator implementations for the Natural SQL Language (NSQL) as defined in MP24. These validators check NSQL statements for correctness, clarity, and adherence to best practices.

## Available Validators

- **syntax_validator.R**: Validates basic syntax correctness
- **ambiguity_validator.R**: Identifies potential ambiguities in statements
- **style_validator.R**: Checks adherence to NSQL style guidelines
- **performance_validator.R**: Identifies potential performance issues

## Validator Interface

All validators implement the following interface:

```r
validate_nsql <- function(nsql_statement, options = list()) {
  # Validate the NSQL statement
  # Returns a list with validation results
}
```

The results include:
- Overall validity status
- List of issues with severity levels
- Suggestions for fixing issues
- Performance recommendations

## Validation Levels

Validation can be performed at different levels:

- **Syntax**: Basic syntax correctness
- **Semantics**: Logical correctness and consistency
- **Style**: Adherence to style guidelines
- **Performance**: Potential performance issues
- **Ambiguity**: Potential ambiguities

## Interactive Disambiguation

As defined in NSQL_R02 (Interactive Update Rule), validators support interactive disambiguation:

```r
disambiguate_nsql <- function(nsql_statement, options = list()) {
  # Identify ambiguities in the statement
  # Returns a list of possible interpretations
}
```

## Usage Example

```r
library(nsql)

# Validate an NSQL statement
results <- validate_nsql("transform Sales to Summary as sum(revenue) as total grouped by region")

# Check if valid
if (results$valid) {
  # Process the valid statement
} else {
  # Handle validation issues
  for (issue in results$issues) {
    cat(sprintf("%s: %s\n", issue$severity, issue$message))
  }
}

# Check for ambiguities
ambiguities <- disambiguate_nsql("show sales by region for last quarter")
if (length(ambiguities) > 1) {
  # Present disambiguation options to the user
}
```