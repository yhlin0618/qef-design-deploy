# dplyr: Integration Examples

## Integration with Project Components

These examples demonstrate how dplyr integrates with other components of the precision marketing application.

## Example 1: Integration with universal_data_accessor

```r
# Analyze customer segments with database connection
analyze_customer_segments <- function(connection) {
  # Get customer data
  customers <- universal_data_accessor(connection, "customer_profile")
  
  # Analyze using dplyr
  customers %>%
    group_by(segment) %>%
    summarize(
      count = n(),
      avg_value = mean(customer_value, na.rm = TRUE),
      total_value = sum(customer_value, na.rm = TRUE),
      retention_rate = mean(is_returning, na.rm = TRUE)
    )
}
```

## Example 2: Integration with Shiny Reactive Context

```r
# Server-side filtering implementation
output$filtered_table <- renderTable({
  # Reactive dependency on input
  req(input$segment_selection)
  req(input$date_range)
  
  # Get data
  customer_data <- universal_data_accessor(db_connection, "customer_profile")
  
  # Apply dplyr operations based on UI inputs
  filtered_data <- customer_data %>%
    filter(
      segment %in% input$segment_selection,
      transaction_date >= input$date_range[1],
      transaction_date <= input$date_range[2]
    ) %>%
    select(customer_id, name, email, value, last_transaction) %>%
    arrange(desc(value))
    
  # Return for display
  filtered_data
})
```

## Example 3: Integration with Report Generation

```r
# Create a summarized report for export
generate_segment_report <- function(connection, date_range) {
  # Get raw data
  customers <- universal_data_accessor(connection, "customer_profile")
  transactions <- universal_data_accessor(connection, "transactions")
  
  # Join and analyze with dplyr
  report_data <- customers %>%
    left_join(transactions, by = "customer_id") %>%
    filter(
      transaction_date >= date_range[1],
      transaction_date <= date_range[2]
    ) %>%
    group_by(segment, region) %>%
    summarize(
      customers = n_distinct(customer_id),
      transactions = n(),
      total_revenue = sum(amount, na.rm = TRUE),
      avg_transaction = mean(amount, na.rm = TRUE)
    ) %>%
    arrange(desc(total_revenue))
    
  return(report_data)
}
```

## Troubleshooting

Common issues and their solutions:

1. **Slow Performance**
   - For large datasets, consider using data.table (R50)
   - Select only needed columns before joining tables
   - Apply filters early in the pipeline to reduce data size

2. **Missing Values**
   - Always use na.rm = TRUE in summary functions
   - Check for NAs in join keys before joining tables
   - Use drop_na() or complete() to handle missing values

3. **Grouped Data Issues**
   - Remember to ungroup() after grouped operations
   - Be aware that group_by() affects subsequent operations

## Best Practices

1. Follow the MP30 Vectorization Principle for all operations
2. Use R51 Lowercase Variable Naming consistently
3. Explicitly specify join columns with the `by` parameter
4. Apply filters early in data pipelines to reduce data volume
5. Use R48 Switch Over If-Else and R49 Apply Over Loops
6. Consider R50 Data Table Vectorization for performance-critical operations