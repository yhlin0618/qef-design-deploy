---
id: "NSQL-REC-20250404-01"
title: "Hierarchical Table References with Dot Notation"
type: "enhancement"
date_created: "2025-04-04"
date_modified: "2025-04-04"
author: "Claude"
relates_to:
  - "MP24": "Natural SQL Language"
---

# Hierarchical Table References with Dot Notation

## Summary

This record documents the addition of hierarchical table reference syntax using dot notation to NSQL. This enhancement allows NSQL to express references to tables in nested schemas or databases using the format `A.B.C` where:
- `A.B` means table B in schema A
- `A.B.C` means table C in schema B, which is in database A

## Motivation

The addition of dot notation for table hierarchies was motivated by:

1. The need to reference tables in complex database environments with multiple schemas
2. The importance of distinguishing between tables with the same name in different schemas
3. Alignment with SQL conventions where dot notation is commonly used for schema qualifiers
4. Support for multi-database environments where the same logical entity might exist in different databases

## Grammar Changes

The NSQL grammar was updated with the following changes:

1. **Enhanced Identifier Definition**:
   - Split Identifier into SimpleName and QualifiedName
   - Added support for dot-separated hierarchical identifiers

Original grammar:
```
Identifier ::= Letter {Letter | Digit | '_'}
```

Updated grammar:
```
Identifier ::= SimpleName | QualifiedName
SimpleName ::= Letter {Letter | Digit | '_'}
QualifiedName ::= SimpleName {'.' SimpleName}
```

## Example Usage

### Basic Schema-Qualified Table

```
transform Sales.Transactions to MonthlySummary as
  sum(revenue) as monthly_revenue
  grouped by month
```

### Multi-Level Hierarchy

```
transform Analytics.Sales.Transactions to RegionalSummary as
  sum(revenue) as regional_revenue
  grouped by region
```

### Cross-Schema Join

```
transform CRM.Customers joined with Sales.Orders on CRM.Customers.id = Sales.Orders.customer_id to CustomerOrders as
  CRM.Customers.name as customer_name,
  count(Sales.Orders.id) as order_count
  grouped by CRM.Customers.id, CRM.Customers.name
```

### With Arrow Syntax

```
Analytics.Sales.Transactions ->
  group(region) ->
  aggregate(
    regional_revenue = sum(revenue),
    order_count = count(order_id)
  )
```

## Translation Impact

The translation of hierarchical references to target languages follows these principles:

1. **SQL Translation**: Maintains the dot notation as standard SQL schema qualifiers
2. **dplyr Translation**: Uses the appropriate package and database connection methods
3. **pandas Translation**: Uses appropriate multi-level dataframe methods

## Implementation Details

The implementation involves:

1. **Parser Updates**: Enhanced to recognize and validate dot-separated identifiers
2. **AST Representation**: Extended to store hierarchy levels separately
3. **Translator Modifications**: Updated to handle hierarchical references in target languages
4. **Validation Rules**: Added validation for consistent hierarchy usage

## Benefits

1. **Enhanced Expressivity**: Enables clear reference to hierarchical database structures
2. **SQL Alignment**: More closely aligns with standard SQL qualification syntax
3. **Multi-Database Support**: Provides clean syntax for cross-database operations
4. **Disambiguation**: Reduces ambiguity in environments with similarly named tables

## Conclusion

The addition of dot notation for hierarchical table references enhances NSQL's ability to express operations in complex database environments. This syntax extension maintains NSQL's readability while adding the expressivity needed for multi-schema and multi-database environments. The implementation preserves backward compatibility while adding this important capability for enterprise database environments.