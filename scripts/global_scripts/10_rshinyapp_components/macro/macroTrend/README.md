# macroTrend Component

## Overview

The macroTrend component provides comprehensive trend analysis and visualization for sales data at the macro (aggregate) level. It implements the Universal Data Access Pattern (R91) to work with any data connection type.

## Features

- **Time-based Sales Trend Analysis**: Visualize sales trends over time with multiple view options (line, bar, area)
- **Category Distribution Analysis**: See breakdown of sales by product category
- **Comparative Analysis**: Compare performance across different time periods (year-over-year, month-over-month)
- **Flexible Filtering**: Filter by date range, category, and time granularity
- **Key Performance Metrics**: Display of total sales, average daily sales, growth rate, and trend direction
- **Data Export**: Download filtered data for further analysis

## Component Structure

The component follows the UI-Server-Defaults triple pattern and consists of:

1. **macroTrendFilterUI**: Filter interface for selecting date ranges, categories, and visualization options
2. **macroTrendUI**: Main display interface with charts, tables, and metrics
3. **macroTrendDefaults**: Default values to handle missing or invalid data
4. **macroTrendServer**: Server logic to process data and render visualizations
5. **macroTrendInitialize**: Helper function to initialize the complete component

## Usage

### Basic Usage

```r
# In UI
macroTrendFilterUI("trend_module")
macroTrendUI("trend_module")

# In Server
macroTrendServer("trend_module", app_data_connection)
```

### Using the Initialize Helper

```r
# In UI
trend_components <- macroTrendInitialize("trend_module")
trend_components$ui$filter
trend_components$ui$display

# In Server
filtered_data <- trend_components$server(input, output, session)
```

## Data Requirements

The macroTrend component requires a data connection that provides a `sales_trend_data` source with at least the following fields:

- **date**: Date of the transaction
- **sales_amount**: Numeric value representing sales amount
- **category**: Category of the product sold

Optional fields that improve functionality:

- **transaction_id**: Unique identifier for each transaction
- **customer_id**: Customer identifier for segmentation

## Implementation Details

- Follows R91 (Universal Data Access Pattern) for connecting to any data source
- Implements P76 (Error Handling Patterns) for robust operation
- Uses P77 (Performance Optimization) for efficient data processing
- Adheres to MP52 (Unidirectional Data Flow) for clean data management
- Provides MP53 (Feedback Loop) for user interaction

## Connection Types

The component works with multiple connection types through the Universal Data Access Pattern:

1. **DBI Connection**: Direct database connection
2. **List with Data Frames**: In-memory data structures
3. **List with Functions**: Function-based data access
4. **Reactive Expression**: Shiny reactive data sources

## Test Configuration

For testing purposes, use the `macroTrend_test.R` script which:

1. Generates realistic sample data
2. Creates a self-contained Shiny app
3. Demonstrates all component features

See TEST.md for more details on testing procedures.