# Precision Marketing Application Components

This directory contains the components for the Precision Marketing application, organized by navigation hierarchy and utilizing the Union pattern.

## Navigation Hierarchy and Directory Structure

The application uses a hierarchical navigation structure with two levels:

### Top-Level Navigation (Main App Tabs)
Implemented as buttons in the main app header.

- **micro/** - Micro-level analysis tab components 
  - Contains components accessed via second-level navigation

### Second-Level Navigation (Within Micro Tab)
Implemented as buttons at the uppermost part of the display UI.

- `microCustomer/microCustomer.R` - Customer profile analysis
- `microDNADistribution/microDNADistribution.R` - DNA distribution analysis
- `microTransactions/microTransactions.R` - Transaction analysis

Each component directory typically contains:
- Main component file (e.g., `microCustomer.R`) defining the component
- Test file (e.g., `microCustomer_test.R`) for demonstrating and testing the component
- Documentation files (e.g., `README.md`, `TEST.md`) with usage instructions and examples
- Potentially multiple versions of the component (e.g., `microCustomer.R`, `microCustomer2.R`)
- Archive directory for previous versions if needed

### Special Directories

- **unions/** - Union pattern implementation
  - `Union.R` - Core implementation of the Union pattern
  - `Union.md` - Documentation of the Union pattern
  - `CompleteUnion_test.R` - Examples and tests

- **sidebars/** - Sidebar component templates and utilities

## Component Structure

All components follow the UI-Server-Defaults Triple Rule (R09) with a consistent structure:

```r
componentName <- function(id, app_data_connection = NULL, config = NULL, translate = function(x) x) {
  return(list(
    ui = list(
      filter = function(id) { ... },  # UI for filters in sidebar
      display = function(id) { ... }  # UI for main display area
    ),
    server = function(input, output, session) { 
      # Server logic
      return(reactive_data)
    },
    defaults = function() { 
      # Default values
      return(list(...))
    }
  ))
}
```

Component implementations are stored in their namesake directory with matching filename:
- `/microCustomer/microCustomer.R` 
- `/microDNADistribution/microDNADistribution.R`

## Union Pattern Usage

The Union pattern is used to combine components at different navigation levels:

1. **Tab-Level Union**: Combines top-level tabs (micro, macro, target)
2. **Micro-Level Union**: Combines components within the micro tab (microCustomer, microDNADistribution, microTransactions)
3. **Sidebar Union**: Combines sidebar filter components

## Usage Example

```r
# Creating a union of micro components
micro_union <- Union(
  "micro_union",
  customer = microCustomerComponent("customer", app_data_connection),
  dna = microDNADistributionComponent("dna", app_data_connection),
  transactions = microTransactionsComponent("transactions", app_data_connection),
  config = list(
    initial_visibility = list(
      customer = TRUE,
      dna = FALSE,
      transactions = FALSE
    )
  )
)

# Using the union in the app
ui <- dashboardPage(
  # ...
  body = dashboardBody(
    # Simple navbar for micro components
    div(
      class = "navbar-micro",
      actionButton("customer_btn", "Customer Profiles"),
      actionButton("dna_btn", "DNA Distribution"),
      actionButton("transactions_btn", "Transactions")
    ),
    
    # Micro union display
    micro_union$ui$display("micro_union")
  )
)

server <- function(input, output, session) {
  # Initialize the union
  micro_server <- micro_union$server("micro_union", app_data_connection, session)
  
  # Control component visibility with navbar buttons
  observeEvent(input$customer_btn, {
    micro_server$component_state$toggle_component("customer", TRUE)
    micro_server$component_state$toggle_component("dna", FALSE)
    micro_server$component_state$toggle_component("transactions", FALSE)
  })
  # Similar handlers for other buttons
}
```