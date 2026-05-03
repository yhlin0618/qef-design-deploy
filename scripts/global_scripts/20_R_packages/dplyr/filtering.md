# dplyr: Filtering Operations

## Description

Filtering operations extract rows from a data frame based on logical conditions. These are essential for subsetting data to specific criteria of interest.

## Code Example

```r
# Basic filtering with single condition
customer_data %>%
  filter(segment == "high_value")

# Multiple conditions (AND logic)
customer_data %>%
  filter(
    segment == "high_value",
    transaction_date >= start_date,
    transaction_date <= end_date
  )

# OR logic with |
customer_data %>%
  filter(segment == "high_value" | segment == "medium_value")

# Multiple OR conditions using %in%
customer_data %>%
  filter(segment %in% c("high_value", "medium_value", "returning"))
```

## Parameters

| Parameter | Description | Example Value |
|-----------|-------------|---------------|
| data | Input data frame/tibble | customer_data |
| filter() | Row filtering function | filter(condition) |
| condition | Logical predicate | segment == "high_value" |
| multiple conditions | Combined with AND by default | condition1, condition2 |
| OR conditions | Use pipe operator | condition1 \| condition2 |

## Notes

- Multiple conditions in a single filter() are combined with AND logic
- Use the OR operator (`|`) for OR logic between conditions
- Use `%in%` for checking membership in a vector of values
- `filter()` automatically handles NA values (returning FALSE)
- For complex filtering logic, consider R57 Filtering Logic Patterns
- Always write conditions that evaluate to TRUE/FALSE
- Use `is.na()` to explicitly filter for/exclude missing values

## Related Usage Patterns

- [Data Pipeline](./data_pipeline.md): Complete transformation workflows
- Integration with universal_data_accessor: [Integration](./integration.md)