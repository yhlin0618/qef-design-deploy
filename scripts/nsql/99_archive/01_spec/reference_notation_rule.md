# NSQL Reference Notation Rule

## Overview

This document defines the standardized notation patterns for referring to datasets, fields, and their characteristics in Natural SQL language (NSQL).

## Notation Patterns

### Dataset Reference

When referring to a dataset or table in documentation, code comments, or natural language discussions:

```
DatasetName
```

Example: `CustomerProfiles`, `Orders`, `TransactionHistory`

### Field Reference

When referring to a specific field within a dataset:

```
DatasetName.field_name
```

Example: `CustomerProfiles.email`, `Orders.order_date`

### Key Notation

When specifying the primary key or join key of a dataset:

```
DatasetName (key = field_name)
```

Examples:
- `CustomerProfiles (key = customer_id)`
- `OrderItems (key = order_item_id)`
- `Transactions (key = transaction_id)`

For composite keys:

```
DatasetName (key = [field1, field2])
```

Example: `OrderDetails (key = [order_id, product_id])`

### Completeness Notation

When describing the completeness relationship between datasets:

```
DatasetA (complete) vs DatasetB (partial, key = field_name)
```

This indicates that DatasetB contains only a subset of the records in DatasetA, joined on the specified key.

Example: 
- `CustomerProfiles (complete, key = customer_id) vs CustomerDNA (partial, key = customer_id)`

## Usage Guidelines

### In Documentation

When documenting a data model or explaining relationships:

```
The system manages CustomerProfiles (key = customer_id) containing all customers,
while CustomerDNA (key = customer_id) contains analytics for a subset of customers.
```

### In Function Documentation

In R function documentation:

```r
#' Process Customer Data
#'
#' @param df_customer_profile CustomerProfiles (key = customer_id)
#' @param df_dna_by_customer CustomerDNA (key = customer_id)
```

### In Natural Language Queries

```
show all customers from CustomerProfiles (key = customer_id) 
with corresponding data from CustomerDNA (key = customer_id)
```

## Implementation

This notation should be used consistently across:

1. Documentation files
2. Code comments
3. Function documentation
4. Variable names (where appropriate)
5. NSQL queries and examples

## Benefits

This standardized notation provides several benefits:

1. **Clarity**: Makes key fields immediately identifiable
2. **Consistency**: Establishes a uniform pattern for documentation
3. **Completeness**: Communicates both structure and relationships
4. **Integration**: Seamlessly works with existing NSQL syntax

## Related Rules

- MP083: Natural SQL Language
- NSQL_R01: NSQL Dictionary Rule (was R21)
- NSQL_R02: NSQL Interactive Update Rule (was R22)