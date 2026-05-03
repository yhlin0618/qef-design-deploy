# dplyr: Joining Operations

## Description

Joining operations combine data from two tables based on matching keys. These operations are essential for relating data from different sources in relational database-like operations.

## Code Example

```r
# Inner join (returns only matching rows)
customers %>%
  inner_join(transactions, by = "customer_id")

# Left join (keeps all rows from left table)
customers %>%
  left_join(transactions, by = "customer_id")

# Join by different column names
customers %>%
  left_join(transactions, by = c("id" = "customer_id"))

# Join by multiple columns
customers %>%
  inner_join(transactions, by = c("customer_id", "segment"))

# Anti-join (find records that don't match)
customers %>%
  anti_join(transactions, by = "customer_id")
```

## Parameters

| Parameter | Description | Example Value |
|-----------|-------------|---------------|
| x, y | Two tables to join | customers, transactions |
| inner_join() | Keeps only matching rows | inner_join(y, by = "key") |
| left_join() | Keeps all rows in x | left_join(y, by = "key") |
| right_join() | Keeps all rows in y | right_join(y, by = "key") |
| full_join() | Keeps all rows in both | full_join(y, by = "key") |
| anti_join() | Rows in x without matches in y | anti_join(y, by = "key") |
| semi_join() | Rows in x with matches in y | semi_join(y, by = "key") |
| by | Join key specification | "customer_id" or c("x_id" = "y_id") |

## Notes

- Always explicitly specify join keys with the `by` parameter
- Be cautious of duplicate join keys - they will create cartesian products
- For different column names, use `by = c("table1_col" = "table2_col")`
- Consider how missing values (NA) affect your joins
- When joining tables from different sources, validate key relationships first (R90)
- For large tables, optimize by selecting only needed columns before joining
- `suffix` parameter can be used to disambiguate duplicate column names

## Related Usage Patterns

- [Data Pipeline](./data_pipeline.md): Complete transformation workflows
- Integration with database tables: [Integration](./integration.md)