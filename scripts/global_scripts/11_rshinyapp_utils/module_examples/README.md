# Shiny Module Examples

This directory contains examples of Shiny modules implemented according to project principles.

## Using Debug Efficiency Exception (P15)

The `customer_profile_triple.R` file demonstrates a proper implementation of the Debug Efficiency Exception principle (P15), which allows the UI-server-defaults triple to be maintained in a single file for debugging purposes.

### Key Features:

1. **Clear Documentation**:
   - Includes required documentation headers
   - References P15 explicitly
   - Includes justification for the exception
   - Includes a refactoring plan

2. **Organized Structure**:
   - UI function
   - Server function
   - Defaults function
   - Clear section headers and documentation

3. **Implementation Standards**:
   - Follows R09 (UI-Server-Defaults Triple) pattern
   - Maintains proper namespace usage
   - Uses reactive programming patterns
   - Provides debugging hooks

## When to Use the Debug Efficiency Exception

This exception should be used only in specific cases:

1. During active UI development where the triple components need frequent coordination
2. During debugging sessions where toggling between files creates excessive overhead
3. For modules under active development or refactoring

Remember that this is an exception to R21 (One Function One File) and should be used judiciously. The long-term goal should be to refactor into separate files once the module design stabilizes.

## File Structure

```
module_examples/
├── customer_profile_triple.R   # Example of UI-server-defaults triple in one file (P15)
└── README.md                  # Documentation
```

## Usage Example

```r
# In app.R or module implementation file
source("update_scripts/global_scripts/11_rshinyapp_utils/module_examples/customer_profile_triple.R")

# Using the module in a Shiny app
ui <- fluidPage(
  customer_profileUI("customer1")
)

server <- function(input, output, session) {
  customer_data <- reactive({
    # Get customer data
    read_customer_data()
  })
  
  customer_profile("customer1", customer_data)
}

shinyApp(ui, server)
```