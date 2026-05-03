# Position DNA Plotly Component

## Overview

The Position DNA Plotly Component provides interactive multi-dimensional brand positioning visualization using Plotly. This component transforms position data into DNA-style line charts that allow comparison of multiple brands across various attributes.

## Architecture

This component follows the project's architectural principles:

- **MP56**: Connected Component Principle (modular UI-Server-Defaults structure)
- **MP73**: Interactive Visualization Preference (Plotly for interactive charts)
- **MP81**: Explicit Parameter Specification (clear function arguments)
- **R116**: Enhanced Data Access with tbl2 (efficient database queries)
- **R09**: UI-Server-Defaults Triple (separated concerns)
- **MP88**: Immediate Feedback (real-time filtering)
- **MP47**: Functional Programming (data transformation functions)

## Features

### Core Functionality
- **Interactive DNA Visualization**: Multi-brand comparison with line charts
- **Attribute-based Analysis**: Shows brand performance across multiple dimensions
- **Dynamic Filtering**: Real-time brand and attribute selection
- **Hover Information**: Detailed ASIN information on hover
- **Legend Controls**: Toggle brand visibility with legend interaction

### User Interface
- **Brand Selection**: Multi-select dropdown for brand filtering
- **Attribute Selection**: Multi-select dropdown for attribute filtering
- **Display Options**: Customizable line width, marker size, and initial visibility
- **Reset Functionality**: One-click filter reset

### Data Processing
- **Automatic Transformation**: Converts position data to DNA format
- **NA Handling**: Filters out excluded variables automatically
- **Performance Optimization**: Uses functional programming for data transformation

## Component Structure

```
positionDNAPlotly/
├── positionDNAPlotly.R              # Main component implementation
├── positionDNAPlotly_production_test.R  # Production test application
└── README.md                        # This documentation
```

## API Reference

### Main Component Function

```r
positionDNAPlotlyComponent(id, app_data_connection = NULL, config = NULL, translate = identity)
```

**Parameters:**
- `id`: Character string. Module ID for namespacing
- `app_data_connection`: Database connection supporting tbl2 pattern
- `config`: List or reactive. Configuration with platform/product line filters
- `translate`: Function. Translation function for UI text

**Returns:**
- List with `ui` (filter/display) and `server` components

### Data Transformation Functions

```r
transform_position_to_dna_format(data, exclude_vars = NULL)
```
Transforms position data from wide to long format suitable for DNA visualization.

```r
create_brand_groups(dna_data)
```
Creates brand-specific data groups for Plotly trace generation.

## Usage Examples

### Basic Usage
```r
# Create component instance
dna_comp <- positionDNAPlotlyComponent("dna_plot")

# In UI
dna_comp$ui$filter    # Filter controls
dna_comp$ui$display   # Plotly visualization

# In server
dna_res <- dna_comp$server(input, output, session)
```

### With Database Connection
```r
dna_comp <- positionDNAPlotlyComponent(
  id = "dna_plot",
  app_data_connection = app_conn,
  config = list(platform_id = "amz", product_line_id = "electronics")
)
```

### With Reactive Configuration
```r
comp_config <- reactive({
  list(
    filters = list(
      platform_id = input$platform,
      product_line_id = input$product_line
    )
  )
})

dna_comp <- positionDNAPlotlyComponent(
  id = "dna_plot",
  app_data_connection = app_conn,
  config = comp_config
)
```

## Data Requirements

### Expected Database Schema
The component expects a `df_position` table with the following structure:

```sql
df_position (
  asin TEXT,
  brand TEXT,
  product_line_id TEXT,
  platform_id TEXT,
  [attribute_columns] NUMERIC,
  ...
)
```

### Excluded Variables
The component automatically excludes these system columns:
- `product_line_id`
- `platform_id` 
- `rating`
- `sales`
- `revenue`

## Configuration Options

### Filter Configuration
```r
config <- list(
  filters = list(
    platform_id = "amz",      # Platform filter
    product_line_id = "electronics"  # Product line filter
  )
)
```

### Display Options (via UI)
- **Brand Selection**: Multi-select brand filtering
- **Attribute Selection**: Multi-select attribute filtering
- **Show All Initially**: Toggle to show all brands on load
- **Line Width**: Adjustable line thickness (1-5)
- **Marker Size**: Adjustable marker size (5-20)

## Integration with Production Apps

### Dashboard Integration
```r
# In main app server
dna_comp <- positionDNAPlotlyComponent("dna", app_connection, comp_config, translate)

# Dynamic filter injection
output$dynamic_filter <- renderUI({
  switch(input$sidebar_menu,
         "dna_visualization" = dna_comp$ui$filter,
         NULL)
})

# Display rendering
output$dna_display <- renderUI(dna_comp$ui$display)

# Server activation
dna_res <- dna_comp$server(input, output, session)
```

### Cross-Component Communication
```r
# Access component state
observe({
  dna_status <- dna_res$component_status()
  if (dna_status == "ready") {
    # React to component readiness
  }
})
```

## Testing

Run the production test:
```r
source("positionDNAPlotly_production_test.R")
```

The test app provides a complete demonstration of the component with:
- Platform and product line selection
- Dynamic filter injection
- Full DNA visualization functionality
- Status notifications

## Performance Considerations

1. **Database Queries**: Uses tbl2 pattern for efficient SQL generation
2. **Data Transformation**: Functional programming minimizes memory usage
3. **Reactive Updates**: Optimized reactive chain prevents unnecessary recalculation
4. **Plotly Rendering**: Efficient trace management for multiple brands

## Troubleshooting

### Common Issues

1. **No Data Displayed**: Check product_line_id is not "all"
2. **Empty Brand List**: Verify database connection and data availability
3. **Slow Rendering**: Consider reducing number of brands/attributes shown simultaneously
4. **Memory Issues**: Ensure proper session cleanup in production

### Debug Information

The component provides status tracking through `component_status()`:
- `"idle"`: Waiting for product line selection
- `"loading"`: Fetching data from database
- `"ready"`: Data loaded and ready for visualization
- `"computing"`: Processing visualization
- `"error"`: Error in data loading or processing

## Future Enhancements

Potential improvements for future versions:
1. **Animation Support**: Smooth transitions between filter states
2. **Export Options**: PNG/PDF export functionality
3. **Statistical Overlays**: Add trend lines or statistical summaries
4. **Custom Color Schemes**: Brand-specific color theming
5. **Performance Metrics**: Built-in performance monitoring