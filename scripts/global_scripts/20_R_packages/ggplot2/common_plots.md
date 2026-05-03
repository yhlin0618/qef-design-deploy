# ggplot2: Common Plot Types

## Description

This guide provides examples of standard plot types commonly used in our precision marketing application, following our visualization standards.

## Bar Charts

```r
# Basic bar chart
ggplot(segment_summary, aes(x = segment, y = count)) +
  geom_col(fill = "#0073C2") +
  labs(title = "Customer Count by Segment",
       x = "Segment", 
       y = "Count") +
  theme_minimal()

# Grouped bar chart
ggplot(segment_region, aes(x = segment, y = count, fill = region)) +
  geom_col(position = "dodge") +
  scale_fill_brewer(palette = "Set2") +
  labs(title = "Customer Count by Segment and Region",
       x = "Segment", 
       y = "Count",
       fill = "Region") +
  theme_minimal()
```

## Line Charts

```r
# Basic time series
ggplot(time_data, aes(x = date, y = value)) +
  geom_line(color = "#0073C2", size = 1) +
  geom_point(color = "#0073C2") +
  scale_y_continuous(labels = scales::dollar) +
  labs(title = "Value Over Time",
       x = "Date", 
       y = "Value ($)") +
  theme_minimal()

# Multiple time series
ggplot(time_data, aes(x = date, y = value, color = segment, group = segment)) +
  geom_line(size = 1) +
  geom_point() +
  scale_y_continuous(labels = scales::dollar) +
  scale_color_brewer(palette = "Set1") +
  labs(title = "Value by Segment Over Time",
       x = "Date", 
       y = "Value ($)",
       color = "Segment") +
  theme_minimal()
```

## Scatter Plots

```r
# Basic scatter plot
ggplot(customer_data, aes(x = recency, y = frequency)) +
  geom_point(alpha = 0.7, color = "#0073C2") +
  labs(title = "Customer Recency vs Frequency",
       x = "Days Since Last Purchase", 
       y = "Purchase Frequency") +
  theme_minimal()

# Scatter plot with smoothing and groups
ggplot(customer_data, aes(x = recency, y = frequency, color = segment)) +
  geom_point(alpha = 0.7) +
  geom_smooth(method = "loess", se = FALSE) +
  scale_color_brewer(palette = "Set1") +
  labs(title = "Customer Recency vs Frequency by Segment",
       x = "Days Since Last Purchase", 
       y = "Purchase Frequency",
       color = "Segment") +
  theme_minimal()
```

## Box Plots

```r
# Basic box plot
ggplot(customer_data, aes(x = segment, y = value)) +
  geom_boxplot(fill = "#0073C2", alpha = 0.7) +
  scale_y_continuous(labels = scales::dollar) +
  labs(title = "Value Distribution by Segment",
       x = "Segment", 
       y = "Value ($)") +
  theme_minimal()

# Box plot with points
ggplot(customer_data, aes(x = segment, y = value)) +
  geom_boxplot(fill = "#0073C2", alpha = 0.4) +
  geom_jitter(width = 0.2, alpha = 0.5) +
  scale_y_continuous(labels = scales::dollar) +
  labs(title = "Value Distribution by Segment",
       x = "Segment", 
       y = "Value ($)") +
  theme_minimal()
```

## Histograms and Density Plots

```r
# Histogram
ggplot(customer_data, aes(x = value)) +
  geom_histogram(bins = 30, fill = "#0073C2", alpha = 0.7) +
  scale_x_continuous(labels = scales::dollar) +
  labs(title = "Distribution of Customer Value",
       x = "Value ($)", 
       y = "Count") +
  theme_minimal()

# Density plot by group
ggplot(customer_data, aes(x = value, fill = segment)) +
  geom_density(alpha = 0.5) +
  scale_x_continuous(labels = scales::dollar) +
  scale_fill_brewer(palette = "Set1") +
  labs(title = "Distribution of Customer Value by Segment",
       x = "Value ($)", 
       y = "Density",
       fill = "Segment") +
  theme_minimal()
```

## Notes

- Follow P06 Data Visualization principles for all visualizations
- Use consistent color schemes across all plots
- Use theme_minimal() as the default theme
- Format axes appropriately (currency, percentages, dates)
- Use transparent points (alpha < 1) when plotting many points
- Include clear titles, axis labels, and legends
- Consider colorblind-friendly palettes (ColorBrewer "Set1", "Set2", "Dark2")

## Related Usage Patterns

- [Customization](./customization.md): Learn how to style these plots consistently
- [Multiple Plots](./multiple_plots.md): Combining plots together
- [Integration](./integration.md): Using these plots in the application