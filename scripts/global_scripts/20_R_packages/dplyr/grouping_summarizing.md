# dplyr: Grouping & Summarizing

## Description

Grouping and summarizing operations allow you to aggregate data by groups and calculate summary statistics for each group, which is essential for data analysis and reporting.

## Code Example

```r
# Basic grouping and summarizing
customer_data %>%
  group_by(segment) %>%
  summarize(
    count = n(),
    avg_value = mean(value, na.rm = TRUE),
    total_value = sum(value, na.rm = TRUE),
    min_value = min(value, na.rm = TRUE),
    max_value = max(value, na.rm = TRUE)
  )

# Multiple grouping variables
customer_data %>%
  group_by(segment, region) %>%
  summarize(
    count = n(),
    avg_value = mean(value, na.rm = TRUE)
  )

# Group, calculate, and keep original data with mutate
customer_data %>%
  group_by(segment) %>%
  mutate(
    segment_avg = mean(value, na.rm = TRUE),
    value_vs_avg = value / segment_avg
  ) %>%
  ungroup()  # Important: ungroup when group operations complete
```

## Parameters

| Parameter | Description | Example Value |
|-----------|-------------|---------------|
| data | Input data frame/tibble | customer_data |
| group_by() | Grouping function | group_by(segment) |
| grouping variables | Columns to group by | segment, region |
| summarize() | Aggregation function | summarize(metric = calculation) |
| aggregations | Summary calculations | count = n(), avg = mean(x) |
| ungroup() | Remove grouping | ungroup() |

## Notes

- Always follow group_by() with either summarize() or mutate()
- Use na.rm = TRUE in aggregation functions to handle missing values
- n() counts rows in each group
- summarize() reduces the dataset to one row per group
- mutate() with group_by() calculates group-level values but keeps all rows
- Always ungroup() when you're done with grouped operations
- Be aware of the impact of NA values on your calculations

## Related Usage Patterns

- [Data Pipeline](./data_pipeline.md): Complete transformation workflows
- Custom aggregate calculations: See R47 Aggregate Variable Naming