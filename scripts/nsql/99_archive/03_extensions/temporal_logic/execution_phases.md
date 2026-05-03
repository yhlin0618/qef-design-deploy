# NSQL Execution Phases

NSQL defines a precise model of execution phases for reactive applications, clarifying when different types of operations should occur.

## Core Execution Phases

| Phase | Description | Appropriate Operations | Temporal Pattern |
|-------|-------------|------------------------|------------------|
| **Initialization Phase** | Complete setup before user interaction | See initialization stages below | One-time (⚡¹) |
| **Runtime Phase** | Interactive application execution during user interaction | Reactive updates, event handling, dynamic visibility | Many-time (⚡*) |
| **Termination Phase** | System cleanup after application ends | Resource cleanup, state saving, connection closing | One-time (⚡¹) |

## Initialization Phase Stages

| Stage | Description | Appropriate Operations | Temporal Pattern |
|-------|-------------|------------------------|------------------|
| **System Initialization Stage** | Foundation setup | Library loading, environment configuration, global settings | One-time (⚡¹) |
| **UI Construction Stage** | Building all UI components | Creating DOM elements, initializing component structure | One-time (⚡¹) |
| **Data Preparation Stage** | Loading initial data | Reading static files, preparing reference data | One-time (⚡¹) |
| **Session Setup Stage** | Establishing session state | Setting initial values, preparing reactive context | One-time (⚡¹) |

## Visual Timeline
```
┌─────────────────────────── Initialization Phase ────────────────────────────┐ ┌───────────────────────────┐ ┌─────────────────┐
│ ┌───────────────┐ ┌───────────────┐ ┌───────────────┐ ┌───────────────┐    │ │ Runtime                   │ │ Termination     │
│ │ System Init   │ │ UI            │ │ Data          │ │ Session       │    │ │ Phase                     │ │ Phase           │
│ │ Stage         │ │ Construction  │ │ Preparation   │ │ Setup         │    │ │                           │ │                 │
│ └───────────────┘ └───────────────┘ └───────────────┘ └───────────────┘    │ │                           │ │                 │
└─────────────────────────────────────────────────────────────────────────────┘ └───────────────────────────┘ └─────────────────┘
                                                                                 ↑                              ↑
                                                                                 │                              │
                                                                                 └─ User interaction begins     └─ User closes app
```

## Phase and Stage as Time Units

Phases and stages represent distinct units of time in the application lifecycle:

- **Phase**: A major time period with a distinct purpose and resource allocation
- **Stage**: A process that spends time and resources to accomplish a specific task
- **A phase is a sequence of stages**: Each phase consists of stages that consume time and resources in succession

This temporal structure allows for precise reasoning about when operations occur and how resources are allocated.

## Phase Characteristics

### Initialization Phase

- **When**: Before application content creation
- **What**: System-level setup that must occur before any UI is created
- **Example Operations**:
  - Loading required libraries
  - Setting up global environment
  - Configuring logging
  - Establishing database connections
- **Code Location**: Typically at the top of app.R or in separate initialization scripts

### Start Phase

- **When**: After initialization, before user interaction
- **What**: Creation of all application content and structure
- **Example Operations**:
  - UI structure creation with all possible components
  - Static data loading
  - One-time component initialization
  - Creating the complete DOM structure
- **Code Location**: Within UI definition and at the beginning of server function

### Runtime Phase

- **When**: During user interaction with the application
- **What**: Reactive updates and responses to user actions
- **Example Operations**:
  - Responding to input changes
  - Updating outputs based on reactive dependencies
  - Showing/hiding UI elements with CSS
  - Processing user events
- **Code Location**: Within reactive contexts in the server function

### Termination Phase

- **When**: After user closes the application
- **What**: Cleanup operations to ensure proper resource release
- **Example Operations**:
  - Closing database connections
  - Saving user state
  - Releasing system resources
  - Logging session information
- **Code Location**: In onStop and onSessionEnded handlers

## Implementation Guidelines

### Initialization Phase Code

```r
# Initialization Phase: System setup
library(shiny)
library(bs4Dash)
library(shinyjs)

# Initialize database connections
db_connection <- dbConnect(...)

# Set up logging
initialize_logging(...)
```

### Start Phase Code

```r
# Start Phase: One-time UI creation
ui <- fluidPage(
  # All possible components are created here
  oneTimeUnionUI(
    common = commonFiltersUI(),
    tab1 = tab1FiltersUI(),
    tab2 = tab2FiltersUI()
  )
)

# Start Phase: One-time server initialization
server <- function(input, output, session) {
  # One-time operations that run at server start
  static_data <- read.csv("data.csv")  # Load data once
  
  # Runtime Phase begins after this point
  # ...
}
```

### Runtime Phase Code

```r
# Runtime Phase: Reactive updates
server <- function(input, output, session) {
  # ... startup code ...
  
  # These execute multiple times during runtime
  observe({
    # Control visibility with CSS
    if (input$active_tab == "tab1") {
      shinyjs::show("tab1_content")
      shinyjs::hide("tab2_content")
    } else {
      shinyjs::hide("tab1_content")
      shinyjs::show("tab2_content")
    }
  })
  
  # Reactive data processing
  filtered_data <- reactive({
    filter(static_data, category == input$category)
  })
  
  # Output rendering
  output$plot <- renderPlot({
    ggplot(filtered_data(), aes(x, y)) + geom_point()
  })
}
```

### Termination Phase Code

```r
# Termination Phase: Cleanup
onStop(function() {
  # Close database connections
  dbDisconnect(db_connection)
  
  # Save application state
  save_state(...)
  
  # Log shutdown
  log_info("Application terminated")
})
```

## Execution Phase Best Practices

1. **Clean Separation**: Keep code for different phases clearly separated
2. **Phase-Appropriate Operations**: Only perform operations appropriate for each phase
3. **One-Time at Start**: Create all UI elements once during the Start Phase
4. **CSS for Visibility**: Control visibility during Runtime Phase using CSS
5. **Proper Cleanup**: Always include Termination Phase handlers for resource cleanup

## Benefits of Phase-Aware Development

1. **Performance**: Operations happen at the right time for optimal performance
2. **Reliability**: System resources are properly initialized and cleaned up
3. **Clarity**: Clear mental model of when code executes
4. **Maintainability**: Easier to understand and modify code when phases are clear

## Related NSQL Concepts

- MP39: One-Time Operations At Start
- P22: CSS Controls Over Shiny Conditionals
- ⚡¹: One-time execution adjective
- ⚡*: Many-time execution adjective