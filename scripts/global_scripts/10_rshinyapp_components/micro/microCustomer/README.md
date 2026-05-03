# Micro Customer Module

This module provides customer-level analytics visualization following R91 (Universal Data Access Pattern) and R92 (Universal DBI Approach).

## Implementation Files

The module consists of these main files:

1. `microCustomer.R` - Main implementation with UI, server and defaults (following P15 Debug Efficiency Exception)
2. `microCustomer_test.R` - Test script for the standard implementation
3. `microCustomer_universal.R` - Implementation with Universal Data Access Pattern (if available)
4. `microCustomer_test_universal.R` - Test script for the universal implementation (if available)

## Features

- Customer profile visualization with RFM metrics
- Customer filtering and selection
- Universal data access compatible with multiple connection types
- Support for internationalization
- Responsive design with bs4Dash components

## Data Access Pattern

The module implements R91 (Universal Data Access Pattern) allowing it to work with different data connection types:

- Direct data frames
- DBI/SQL database connections
- Function-based data access
- Reactive expressions
- Mixed connection types

## Usage

### Standard Implementation

```r
# Load the module
source("update_scripts/global_scripts/10_rshinyapp_components/micro/microCustomer/microCustomer.R")

# UI Component
ui <- fluidPage(
  # Filter component
  microCustomerFilterUI("customer1"),
  
  # Display component
  microCustomerUI("customer1")
)

# Server implementation
server <- function(input, output, session) {
  # Use with direct data frames
  filtered_data <- microCustomerServer(
    "customer1", 
    dna_data_frame,
    customer_profile_data_frame
  )
}

shinyApp(ui, server)
```

### Universal Data Access Implementation

```r
# Load the universal version
source("update_scripts/global_scripts/10_rshinyapp_components/micro/microCustomer/microCustomer_universal.R")

# UI components (same as standard)
ui <- fluidPage(
  microCustomerFilterUI("customer1"),
  microCustomerUI("customer1")
)

# Server with universal data access
server <- function(input, output, session) {
  # Works with any connection type
  filtered_data <- microCustomerServer(
    "customer1", 
    app_data_connection  # Can be data frames, DBI connection, functions, etc.
  )
}

shinyApp(ui, server)
```

## Testing

Run the test scripts to verify functionality:

```r
# Standard implementation test
source("update_scripts/global_scripts/10_rshinyapp_components/micro/microCustomer/microCustomer_test.R")

# Universal implementation test
source("update_scripts/global_scripts/10_rshinyapp_components/micro/microCustomer/microCustomer_test_universal.R")
```

These tests provide a full UI for interactive testing and verification.

## Implementation Notes

The module follows these principles:

- **P15 Debug Efficiency Exception**: UI-server-defaults triple in one file for easier debugging
- **MP52 Unidirectional Data Flow**: Clear data flow from connection to UI
- **MP53 Feedback Loop**: UI elements update in response to user actions
- **MP54 UI-Server Correspondence**: Every UI element has a corresponding server implementation
- **R91 Universal Data Access Pattern**: Works with multiple connection types
- **R76 Module Data Connection Rule**: Clear interface for data connections

## Future Directions

1. 如果資料大量的話怎樣不會lag
2. 讓他可以自由選micro的指標要是什麼

## Refactoring Plan

According to P15, the module will be refactored back into separate files once the micro-level component design is finalized (estimated Q3 2025).