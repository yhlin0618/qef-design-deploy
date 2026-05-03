# NSQL Temporal Operators

## Execution Frequency Notation

NSQL introduces formal notation for execution frequency to clarify when expressions are evaluated:

| Operator | Symbol | Meaning | Definition |
|----------|--------|---------|------------|
| Once | ⚡¹ | Evaluated exactly once | Expression is evaluated exactly once, typically at initialization |
| OnEvery | ⚡* | Evaluated on every trigger | Expression is evaluated every time a specified trigger occurs |
| AtMost | ⚡≤ⁿ | Evaluated at most n times | Expression is evaluated no more than n times |
| Periodic | ⚡ᵖ | Evaluated periodically | Expression is evaluated at regular intervals |

## Binding Types

These operators define how values are bound:

| Binding | Symbol | Meaning | Definition |
|---------|--------|---------|------------|
| StaticBinding | ⚓ | Value bound once | Value is established once and remains constant |
| DynamicBinding | ⚓* | Value bound dynamically | Value is re-established when dependencies change |

## Temporal Logic Operators

These operators define relationships over time:

| Operator | Symbol | Meaning | Definition |
|----------|--------|---------|------------|
| Always | □ | Always true | Expression is true at all points in time |
| Eventually | ◇ | Eventually true | Expression becomes true at some point |
| Until | U | True until | First expression is true until second becomes true |
| Next | ○ | True next | Expression is true at the next point in time |

## Execution Lifecycle Phases

NSQL defines standard phases in a reactive application lifecycle:

1. **Initialization Phase** (t₀):
   - Static bindings are established
   - DOM structure is created
   - Initial state is set

2. **Event Processing Phase** (t₁...tₙ):
   - Input changes trigger reactive updates
   - Dynamic bindings are recalculated
   - Outputs are updated

3. **Termination Phase** (t_end):
   - Resources are released
   - State is saved
   - Application terminates

## Combining Set Theory with Temporal Logic

NSQL uniquely combines set theory with temporal logic:

1. **Static Union**:
   ```
   UI = ⚓(A ∪ B ∪ C)
   ```
   The union occurs once at initialization, creating a static result.

2. **Dynamic Filtering**:
   ```
   FilteredData = ⚓*(Data ∩ {x ∈ Data | Condition(x)})
   ```
   The intersection is recalculated whenever Data or Condition changes.

3. **Lifecycle-Dependent Properties**:
   ```
   Visibility(component) = ⚓*(component ∈ VisibleSet)
   ```
   Visibility is recalculated based on membership in the VisibleSet, which can change over time.

## Applying to Shiny Components

These concepts directly map to Shiny's execution model:

1. **UI Definition (Once/StaticBinding)**:
   ```r
   ui <- ⚡¹(fluidPage(...))  # Evaluated once at startup
   ```

2. **Reactive Expression (DynamicBinding)**:
   ```r
   data_filtered <- ⚓*(filter(data, input$variable))  # Re-evaluated when dependencies change
   ```

3. **Render Functions (OnEvery/DynamicBinding)**:
   ```r
   output$plot <- ⚡*(renderPlot(...))  # Runs every time reactive dependencies change
   ```

## Implementation Example: Union Component

A union component using temporal logic notation:

```r
# Mathematically:
# unionUI = ⚓(⋃ components) with ⚓*(visibility(component))

unionUI <- function(..., id = NULL) {
  # Static binding (⚓) - Evaluated ONCE at initialization
  components <- list(...)
  
  # Create container with all components included
  container <- div(
    id = id,
    # Static union operation (⋃) - All components are included in DOM
    lapply(names(components), function(name) {
      div(
        id = paste0(id, "_", name),
        style = "display: none;",  # Initially hidden
        components[[name]]
      )
    })
  )
  
  return(container)
}

# Server-side uses dynamic binding (⚓*)
unionServer <- function(id, visibility_conditions) {
  moduleServer(id, function(input, output, session) {
    # Dynamic binding (⚓*) - Re-evaluated when dependencies change
    observe({
      for (name in names(visibility_conditions)) {
        is_visible <- visibility_conditions[[name]]()
        if (is_visible) {
          shinyjs::show(paste0(id, "_", name))
        } else {
          shinyjs::hide(paste0(id, "_", name))
        }
      }
    })
  })
}
```

This example demonstrates both static binding of the union structure and dynamic binding of the visibility conditions.

## Using Temporal Operators in Documentation

NSQL recommends using temporal operators in code documentation:

```r
#' Create a UI Component Union
#'
#' @description
#' ⚡¹ Creates a union of UI components with all elements in DOM.
#' ⚓ The component structure is statically bound at initialization.
#' ⚓* Component visibility is dynamically bound to conditions.
#'
#' @param ... Named UI components
#' @return Union component
unionUI <- function(...) { ... }
```

This makes the execution model explicitly clear in the documentation.