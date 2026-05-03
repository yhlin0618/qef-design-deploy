# shiny: Modules

## Description

Shiny modules are a way to create reusable, self-contained components with namespaced IDs. They help organize complex applications into manageable pieces and enable code reuse.

## Code Example

```r
# Module UI function
filterPanelUI <- function(id) {
  # Create a namespace function for this module
  ns <- NS(id)
  
  tagList(
    selectInput(ns("segment"), "Segment:", 
                choices = c("All", "High Value", "Medium Value", "Low Value")),
    dateRangeInput(ns("dates"), "Date range:",
                  start = Sys.Date() - 30, end = Sys.Date()),
    actionButton(ns("apply"), "Apply Filters")
  )
}

# Module server function
filterPanelServer <- function(id, data) {
  moduleServer(id, function(input, output, session) {
    # Reactive expression that filters data
    filtered_data <- eventReactive(input$apply, {
      # Start with the full dataset
      result <- data
      
      # Filter by segment if not "All"
      if (input$segment != "All") {
        result <- result %>% filter(segment == input$segment)
      }
      
      # Filter by date range
      result <- result %>% filter(
        date >= input$dates[1],
        date <= input$dates[2]
      )
      
      return(result)
    })
    
    # Return the filtered data to the parent
    return(filtered_data)
  })
}

# Usage in main app
ui <- fluidPage(
  filterPanelUI("customer_filter"),
  tableOutput("results")
)

server <- function(input, output, session) {
  # Get data
  customer_data <- reactive({
    universal_data_accessor(connection, "customer_profile")
  })
  
  # Use the module
  filtered_customers <- filterPanelServer("customer_filter", customer_data)
  
  # Display results
  output$results <- renderTable({
    filtered_customers()
  })
}
```

## Module Components

| Component | Description | Example Usage |
|-----------|-------------|---------------|
| NS() | Create a namespace function | ns <- NS(id) |
| ns() | Namespacing IDs | ns("button_id") |
| moduleServer() | Define module server logic | moduleServer(id, function(input, output, session) {...}) |
| UI function | Define module UI components | moduleUI <- function(id) {...} |
| Server function | Define module server logic | moduleServer <- function(id, data) {...} |

## Notes

- Follow R06 Module Naming Convention for all modules
- Follow R42 Module Naming for function names
- Always use NS() and namespaced IDs within modules
- Module UI functions should always take an id parameter
- Module server functions should return reactive values needed by the parent
- Follow R88 Shiny Module ID Handling for ID namespacing
- Use R76 Module Data Connection pattern for data passing
- Follow MP54 UI Server Correspondence in module design
- Module names should reflect their functionality (e.g., filterPanelUI/filterPanelServer)

## Related Usage Patterns

- [UI Components](./ui_components.md): Components used within modules
- [Reactivity](./reactivity.md): Reactive programming in modules
- [App Structure](./app_structure.md): Overall application organization