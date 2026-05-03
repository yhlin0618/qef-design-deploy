# dplyr: Data Pipeline Pattern

## Description

The data pipeline pattern is the core usage pattern in dplyr, allowing multiple operations to be chained together with the pipe operator (`%>%`). This creates readable, maintainable data transformation workflows.

## Code Example

```r
# Standard data pipeline pattern
data %>%
  filter(condition) %>%
  select(needed_columns) %>%
  mutate(new_column = transformation) %>%
  group_by(grouping_variable) %>%
  summarize(
    metric_1 = calculation_1,
    metric_2 = calculation_2
  )
```

## Parameters

| Parameter | Description | Example Value |
|-----------|-------------|---------------|
| data | Input data frame/tibble | customer_data |
| filter() | Row filtering operation | segment == "high_value" |
| select() | Column selection | id, name, value |
| mutate() | Create/transform variables | total = price * quantity |
| group_by() | Group by variables | segment, region |
| summarize() | Aggregate calculations | avg_value = mean(value) |

## Notes

- Operations are executed in the order they appear in the pipeline
- Each step takes the result of the previous step as input
- The pipe operator `%>%` can be read as "then"
- For complex pipelines, consider creating intermediate variables for clarity
- Always follow vectorized operations principles (MP30)
- In R 4.1+, you can also use the native pipe `|>` as an alternative

## Related Usage Patterns

- [Filtering](./filtering.md): Detailed filter operations
- [Grouping & Summarizing](./grouping_summarizing.md): Group-based aggregations