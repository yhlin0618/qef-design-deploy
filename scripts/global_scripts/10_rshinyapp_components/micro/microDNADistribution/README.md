# microDNADistribution Component

This component visualizes customer DNA distributions (M, R, F, IPT, NES values) with interactive plots using Plotly.

## ⚠️ IMPORTANT CODE MAINTENANCE NOTES (Updated 2025-04-24) ⚠️

### Platform Handling Issues

1. **UI Component Parameters**
   - radioButtons parameters MUST be named explicitly (inputId, label, choices, selected) per MP081
   - Example:
     ```r
     radioButtons(
       inputId = "platform",
       label = NULL,
       choices = c("Amazon" = "amz", "All Platforms" = "all"),
       selected = "amz"
     )
     ```
   - ⚠️ NEVER use positional parameters as this has caused bugs

2. **Platform Switch Observer**
   - Platform switch handling MUST use consistent variable names throughout the observer
   - Any variables used in notification must also be updated in the logging section
   - Use a mapping dictionary for platform names: `platform_map <- c("all" = "All Platforms", "amz" = "Amazon")`
   - Currently supported platforms: Amazon (amz) and All Platforms (all)
   - ⚠️ Previous bugs were caused by variable naming inconsistencies

3. **Related Files**
   - microDNADistribution_production_test.R
   - microDNADistribution.R

## Key Principles

This component follows several key principles:
- **MP081**: Explicit Parameter Specification (function arguments) - ⚠️ CRITICAL for platform handling
- **MP073**: Interactive Visualization Preference (plotly for visualizations)
- **MP056**: Connected Component Principle (component structure)
- **MP055**: Computation Allocation Principle (pre-computation)
- **R116**: Enhanced Data Access with tbl2 (data access)
- **R009**: UI-Server-Defaults Triple (component organization)
- **P006**: Data Visualization (visualization standards)

## Overview

The microDNADistribution component provides a comprehensive visualization suite for customer DNA metrics:
- Monetary value (M) distribution
- Recency (R) distribution
- Frequency (F) distribution
- Inter-purchase Time (IPT) distribution
- NES status (New/Established/Lost) distribution

The component follows MP073 (Interactive Visualization Preference) by using Plotly for all visualizations, providing interactive tooltips, zooming capabilities, and exportable visualizations.

## Implementation Principles

This component implements several key architectural principles:

### MP081: Explicit Parameter Specification

- All functions have fully documented parameters with types and descriptions
- Parameters follow a consistent naming pattern across functions
- Default values are provided where appropriate and documented
- Return values are clearly specified with types and descriptions
- Function examples demonstrate proper usage
- Internal helper functions are also fully documented

### MP073: Interactive Visualization Preference

- Uses Plotly for all visualizations to ensure interactive capabilities
- Implements detailed tooltips with statistical information
- Provides hover information with exact values and percentages
- Supports zooming, panning, and export capabilities
- Allows user selection of different metrics and visualization types

### MP056: Connected Component Principle

The component is designed as a self-contained connected component:
- Clear edge nodes (buttons as inputs, plot as output)
- Encapsulated internal state (current visualization, component status)
- Well-defined data flow paths
- Isolated side effects

The internal graph structure follows this pattern:
```
                ┌────────────────────┐
                │  Platform Config   │
                └──────────┬─────────┘
                           │
     ┌─────────────┐       │
     │  DNA Data   │◄──────┘
     └──────┬──────┘
            │
            ▼
 ┌─────────┐       ┌───────────────┐       ┌──────────────┐
 │ M_ecdf  │ ─────►│ M ECDF        │       │              │
 │ Button  │       │ Observer      │       │              │
 └─────────┘       └───────┬───────┘       │              │
                           │               │              │
 ┌─────────┐       ┌───────────────┐       │              │
 │ R_ecdf  │ ─────►│ R ECDF        │       │              │
 │ Button  │       │ Observer      │       │              │
 └─────────┘       └───────┬───────┘       │              │
                           │               │              │
 ┌─────────┐       ┌───────────────┐       │  Plotly      │
 │ F_ecdf  │ ─────►│ F ECDF        │ ─────►│  Visualization│
 │ Button  │       │ Observer      │       │              │
 └─────────┘       └───────┬───────┘       │              │
                           │               │              │
 ┌─────────┐       ┌───────────────┐       │              │
 │ IPT_ecdf│ ─────►│ IPT ECDF      │       │              │
 │ Button  │       │ Observer      │       │              │
 └─────────┘       └───────┬───────┘       │              │
                           │               │              │
 ┌─────────┐       ┌───────────────┐       │              │
 │ NES     │ ─────►│ NES Status    │       │              │
 │ Button  │       │ Observer      │       │              │
 └─────────┘       └───────────────┘       └──────────────┘
```

### MP055: Computation Allocation Principle

The component implements strategic computation allocation:
- **Pre-computation**: Heavy distribution calculations are pre-computed during initialization
- **On-demand computation**: Calculations are performed as needed when pre-computation is disabled
- **Computation classification**:
  - Heavy + Frequent + Semi-static: Distribution calculations (pre-computed)
  - Light + Dynamic: UI updates and statistic summaries (computed on-demand)
- Implements reactive caching for performance optimization
- Provides user control over computation strategy

### R116: Enhanced Data Access with tbl2

The component implements the Enhanced Data Access Pattern:
- Uses `tbl2` to access `df_dna_by_customer` data with enhanced flexibility
- Supports multiple data source types including databases, data frames, and files
- Implements platform filtering through dplyr's filter functionality
- Properly handles data availability and errors
- Provides clear status messages during data loading
- Maintains full pipe operator compatibility

### R009: UI-Server-Defaults Triple

The component follows the UI-Server-Defaults triple pattern:
- UI function for display elements
- Server function for logic
- Defaults section for testing and development

## Usage

### Basic Implementation

```r
# Create component
dna_component <- microDNADistributionComponent(
  id = "dna_module",
  app_data_connection = app_db_conn, # Your data connection
  config = list(platform_id = "amz") # Optional platform filtering
)

# UI
ui <- fluidPage(
  dna_component$ui$filter,
  dna_component$ui$display
)

# Server
server <- function(input, output, session) {
  dna_component$server(input, output, session)
}
```

### With Platform Filtering

```r
# Create component with reactive platform filtering
dna_component <- microDNADistributionComponent(
  id = "dna_module",
  app_data_connection = app_db_conn,
  config = reactive({ list(platform_id = input$selected_platform) }) # Reactive config
)
```

### Backward Compatibility

The original function name is still supported for backward compatibility:

```r
# Using the original function name (backward compatibility)
dna_component <- microDNADistribution(
  id = "dna_module", 
  app_data_connection = app_db_conn
)
```

### Testing

Run the test file to verify component functionality:
```r
source("update_scripts/global_scripts/10_rshinyapp_components/micro/microDNADistribution/microDNADistribution_test.R")
```

## Data Requirements

The component expects a data frame with the following columns:
- `customer_id`: Customer identifier
- `M`: Monetary value (purchase amount)
- `R`: Recency value (days since last purchase)
- `F`: Frequency (number of purchases)
- `IPT_mean`: Inter-purchase time (average days between purchases)
- `NES`: NES status classification (E0, S, L for Early/Stable/Lost)

## Available Visualizations

1. **Cumulative Distribution Functions (ECDFs)**:
   - M value (purchase amount) 
   - R value (recency)
   - F value (frequency)
   - IPT value (inter-purchase time)

2. **Histograms**:
   - Frequency distribution
   - NES status distribution

3. **Statistics Display**:
   - Count, mean, median, standard deviation
   - Min, max, Q1, Q3 values
   - Additional NES status breakdowns

## Dependencies

- shiny
- plotly
- bs4Dash (for styling)
- dplyr (for data manipulation)

## Filtering Capabilities

The component provides multiple ways to filter the data:
- By platform (through config parameter)
- By customer segment (High Value, Repeat, New)
- Custom filters can be implemented by passing pre-filtered data

## Performance Considerations

- Enable pre-computation for large datasets (`use_precomputed = TRUE`)
- Set `use_precomputed = FALSE` for smaller datasets or when memory is limited
- Component automatically manages computation status and provides user feedback
