# NSQL Temporal Logic

This directory contains temporal logic extensions for NSQL, focusing on execution timing, frequency, and lifecycle events in reactive applications.

## Overview

Temporal logic in NSQL extends set theory with concepts of time, order, and execution frequency. This is particularly important for reactive applications where components execute at different times and frequencies.

## Key Concepts

### Execution Frequency Operators

These operators define how often an expression is evaluated during the application lifecycle:

| Operator | Symbol | Definition | Example Use Case |
|----------|--------|------------|-----------------|
| Once | ⚡¹ | Expression evaluated exactly once | UI component creation |
| OnEvery | ⚡* | Expression evaluated on every occurrence of an event | Event handlers |
| AtMost | ⚡≤ⁿ | Expression evaluated at most n times | Limited operations |
| Periodically | ⚡ᵖ | Expression evaluated at regular intervals | Polling operations |

### Temporal Binding Types

| Binding | Symbol | Definition | Shiny Equivalent |
|---------|--------|------------|------------------|
| StaticBinding | ⚓ | Value bound once at initialization | UI definitions |
| DynamicBinding | ⚓* | Value re-evaluated when dependencies change | Reactive expressions |

## Application to Component Architecture

Different parts of an application execute at different frequencies:

1. **UI Structure** (Once/StaticBinding):
   ```
   UI = StaticBinding(⋃ components)
   ```
   
2. **Visibility Control** (DynamicBinding):
   ```
   Visibility(component) = DynamicBinding(f(state))
   ```
   
3. **Data Processing** (DynamicBinding):
   ```
   FilteredData = DynamicBinding(data ∩ {x ∈ data | filter(x)})
   ```

## Temporal Logic in Reactive Flow

Reactive applications follow this general flow:

1. **Initialization Phase** (t₀):
   - All UI components are created (Once)
   - Initial state is established
   
2. **Reactive Phase** (t₁...tₙ):
   - Inputs trigger reactive updates (OnEvery)
   - Dependencies recalculate (DynamicBinding)
   - Output updates propagate

## Shiny's Execution Model through Temporal Logic

Shiny's execution model perfectly illustrates these temporal concepts:

```
# UI definition (evaluated ONCE at startup)
ui <- StaticBinding(
  fluidPage(
    selectInput("var", "Variable", choices = c("A", "B", "C")),
    plotOutput("plot")
  )
)

# Server logic (reactive components evaluated MULTIPLE times)
server <- function(input, output, session) {
  # Reactive expression (re-evaluated when dependencies change)
  data_subset <- DynamicBinding(
    filter(dataset, variable == input$var)
  )
  
  # Output (re-evaluated when data_subset changes)
  output$plot <- DynamicBinding(
    renderPlot(ggplot(data_subset(), aes(x, y)) + geom_point())
  )
}
```

## Mathematical Representation

The temporal execution model can be expressed mathematically as:

- StaticBinding(expr) = {value | value = expr(t₀)}
- DynamicBinding(expr) = {value(t) | value(t) = expr(t) ∀t : dependencies(expr) changed at t}

## Implementation in NSQL

NSQL temporal logic provides a formal way to reason about and document when code executes. This helps prevent common bugs like assuming a component is reactive when it's actually static.