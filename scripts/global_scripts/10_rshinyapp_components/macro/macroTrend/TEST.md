# Testing the macroTrend Component

This document outlines the testing procedures for the macroTrend component.

## Test Preparation

Before testing the component, ensure you have the following:

1. R environment with required packages installed:
   - shiny
   - bs4Dash
   - dplyr
   - ggplot2
   - DT
   - lubridate

2. Access to the component source files:
   - macroTrend.R
   - macroTrend_test.R

## Running the Test Application

The component includes a self-contained test application that demonstrates all features with generated sample data.

### Method 1: Direct Execution

1. Open R or RStudio
2. Set your working directory to the folder containing the macroTrend files
3. Run: `source("macroTrend_test.R")`

### Method 2: Within Another Application

```r
# Source the test file
source("path/to/macroTrend_test.R")

# Run the test app function
test_app()
```

## Test Scenarios

The test application provides several scenarios to validate different aspects of the component:

### 1. Basic Functionality Test

- **Purpose**: Verify that the component loads and displays properly
- **Steps**:
  1. Run the test application
  2. Confirm that both filter and display sections are visible
  3. Verify that charts and metrics display properly with sample data

### 2. Filtering Test

- **Purpose**: Verify that filtering works correctly
- **Steps**:
  1. Change the date range selection
  2. Select different categories
  3. Change time granularity (Daily, Weekly, Monthly)
  4. Confirm that visualizations update correctly
  5. Test the Reset Filters button

### 3. Visualization Options Test

- **Purpose**: Test different visualization options
- **Steps**:
  1. Test switching between Line, Bar, and Area chart types
  2. Toggle the view options (Show Trend Line, Show Categories, Show Year-over-Year)
  3. Verify that each visualization type displays correctly

### 4. Comparative Analysis Test

- **Purpose**: Test the comparative analysis functionality
- **Steps**:
  1. Switch between Year-over-Year, Month-over-Month, and Category Comparison views
  2. Verify that each comparison type displays meaningful data

### 5. Download Test

- **Purpose**: Test the data download functionality
- **Steps**:
  1. Apply various filters
  2. Click the Download button
  3. Verify that the downloaded CSV contains the filtered data with correct formatting

## Test with Different Connection Types

The test script demonstrates using a list-based connection, but you can modify it to test with different connection types:

### DBI Connection Example

```r
# Create a DBI connection
library(DBI)
library(duckdb)
conn <- dbConnect(duckdb::duckdb(), ":memory:")
dbWriteTable(conn, "sales_trend_data", sample_data)

# Create a reactive connection object
data_connection <- reactive({
  universal_accessor_wrapper(conn, type = "dbi")
})

# Use in the server
macroTrendServer("test_module", data_connection)
```

### Function-Based Connection Example

```r
# Create a function-based connection
get_sales_trend_data <- function() {
  return(sample_data)
}

# Create a reactive connection object
data_connection <- reactive({
  list(
    sales_trend_data = get_sales_trend_data
  )
})

# Use in the server
macroTrendServer("test_module", data_connection)
```

## Troubleshooting Common Issues

- **Issue**: Charts show "No data available"
  - **Solution**: Check that the data connection is providing data with the required columns

- **Issue**: Error when changing filters
  - **Solution**: Verify that date format is correct and that category values exist in the data

- **Issue**: Performance issues with large datasets
  - **Solution**: Add more efficient data filtering on the server side before passing to the module

## Test Validation Checklist

- [ ] Component loads without errors
- [ ] All UI elements display correctly
- [ ] Filtering updates the visualizations appropriately
- [ ] All chart types (Line, Bar, Area) work correctly
- [ ] Comparative analysis shows meaningful comparisons
- [ ] Download feature produces correct CSV files
- [ ] Component handles different connection types properly
- [ ] Component degrades gracefully when data is missing or invalid