# macroNESTransition Component

This component visualizes customer transitions between New/Existing/Sleeping (NES) statuses over time.

## Overview

The `macroNESTransition` component creates a stacked bar chart visualization that shows how customers move between different NES statuses from one time period to another. This helps business analysts understand customer lifecycle patterns, such as:

- How many sleeping customers are reactivated
- How many new customers become sleeping
- Retention rates for existing customers

## Usage

```r
# Load the component
source("path/to/macroNESTransition/macroNESTransition.R")

# Create the component
nes_transition_component <- macroNESTransitionComponent(
  id = "nes_transitions",
  app_data_connection = app_conn,
  config = list(platform_id = "amz")
)

# In UI
ui <- fluidPage(
  fluidRow(
    column(width = 3, nes_transition_component$ui$filter),
    column(width = 9, nes_transition_component$ui$display)
  )
)

# In server
server <- function(input, output, session) {
  nes_transition_result <- nes_transition_component$server(input, output, session)
}
```

## Component Parameters

The component accepts these parameters:

- `id` (required): The module ID used for namespacing inputs and outputs
- `app_data_connection`: Database connection object or list supporting the R116 data access pattern
- `config`: Configuration list or reactive expression with customization settings
- `translate`: Translation function for UI text elements (defaults to identity function)

## Configuration Options

The `config` parameter accepts these keys:

- `platform_id`: The three-letter platform_id to filter data by
- `filters`: A nested list containing filters, including `platform_id`

## Data Requirements

The component expects certain tables to be accessible through the `app_data_connection`:

- `sales_by_customer`: Customer sales data with NES status information
- `product_categories`: Product category information

Key columns required:

- `customer_id`: Unique customer identifier
- `nesstatus`: NES status of customers (N, E0, S1, S2, S3)
- `source`: Distribution channel
- `product_line_id`: Product category
- `state`: Geographic location
- `time_condition`: Time period markers including "now" and time scale indicators

## Features

- Allows filtering by time scale, distribution channel, product category, and geography
- Visualizes transition patterns with a stacked bar chart
- Calculates key metrics like reactivation counts and newly sleeping customers
- Shows hover details with counts and percentages
- Updates dynamically when filters change
- Provides status indicators during data loading and processing

## Design Principles

The component follows these design principles:

- MP56: Connected Component Principle (component structure)
- MP55: Computation Allocation Principle (pre-computation)
- MP73: Interactive Visualization Preference (plotly for visualizations)
- MP81: Explicit Parameter Specification (function arguments)
- R116: Enhanced Data Access with tbl2 (data access)
- R91: Universal Data Access Pattern (data access)
- R09: UI-Server-Defaults Triple (component organization)
- P006: Data Visualization (visualization standards)
