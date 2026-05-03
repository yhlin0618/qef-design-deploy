# ggplot2: Basic Plot Structure

## Description

The layered grammar of graphics approach in ggplot2 builds visualizations by combining data, aesthetics, and geometries. This document outlines the fundamental structure of ggplot2 plots.

## Code Example

```r
# Basic ggplot2 structure
ggplot(data = customer_data, aes(x = date, y = value)) +
  geom_line() +
  geom_point() +
  labs(
    title = "Customer Value Over Time",
    subtitle = "By Transaction Date",
    x = "Transaction Date",
    y = "Customer Value ($)",
    caption = "Source: CRM Database"
  ) +
  theme_minimal()
```

## Components

| Component | Description | Example Usage |
|-----------|-------------|---------------|
| ggplot() | Initialize a plot | ggplot(data, aes(x, y)) |
| aes() | Map variables to visual properties | aes(x = date, y = value, color = segment) |
| geom_*() | Add geometric elements | geom_point(), geom_line(), geom_bar() |
| scale_*() | Customize scales | scale_y_continuous(labels = scales::dollar) |
| labs() | Add labels and titles | labs(title = "Plot Title", x = "X Label") |
| theme_*() | Apply themes | theme_minimal(), theme_light() |
| theme() | Customize themes | theme(axis.text = element_text(size = 12)) |
| facet_*() | Create multi-panel plots | facet_wrap(~segment) |

## Notes

- All ggplot2 plots start with ggplot() function
- Aesthetics (aes) map data variables to visual properties
- Multiple layers (geoms) can be added with the + operator
- Follow a consistent theme for all visualizations (P06)
- Initialize with data and global aesthetics, then add layers
- Aesthetic mappings in aes() are inherited by all layers
- Fixed aesthetics go outside aes(): geom_point(color = "blue")
- Follow P06 Data Visualization principles for all plots

## Related Usage Patterns

- [Common Plot Types](./common_plots.md): Standard visualizations
- [Customization](./customization.md): Styling and theming
- [Integration with Shiny](./integration.md): Interactive visualizations