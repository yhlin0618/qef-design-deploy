# NSQL Temporal Adjectives

Temporal adjectives in NSQL provide a formal vocabulary for describing execution frequency. These adjectives can be applied to components, operations, and functions to precisely communicate when and how often they execute.

## Core Temporal Adjectives

| Adjective | Symbol | Definition | Example Usage |
|-----------|--------|------------|--------------|
| One-time | ⚡¹ | Executed exactly once, typically at initialization | One-time component creation |
| Two-time | ⚡² | Executed exactly twice | Two-time validation (client and server) |
| Three-time | ⚡³ | Executed exactly three times | Three-time verification process |
| N-time | ⚡ⁿ | Executed exactly n times | N-time iteration process |
| Many-time | ⚡* | Executed many times | Many-time reactive calculation |
| Every-time | ⚡ᵉ | Executed on every occurrence of a specific event | Every-time input handler |
| Zero-time | ⚡⁰ | Never executed (present but inactive) | Zero-time placeholder function |
| Conditional-time | ⚡? | Executed conditionally | Conditional-time feature flag |
| Periodic-time | ⚡ᵖ | Executed at regular intervals | Periodic-time polling function |

## Usage in Code Documentation

These adjectives can be used in documentation to precisely specify execution behavior:

```r
#' One-time Component Constructor
#'
#' @description
#' Creates a component that is initialized exactly once during application startup.
#' The internal structure is created as a ⚡¹(one-time) operation.
#'
#' @param id Component ID
#' @return The initialized component
oneTimeComponentUI <- function(id) {
  # Implementation...
}

#' Many-time Reactive Value Calculator
#'
#' @description
#' Calculates derived values from inputs in a ⚡*(many-time) execution pattern,
#' running whenever reactive dependencies change.
#'
#' @param input_value Reactive input value
#' @return Reactive expression with calculated result
manyTimeCalculator <- function(input_value) {
  # Implementation...
}
```

## Composition of Temporal Adjectives

Temporal adjectives can be composed to describe complex execution patterns:

| Composed Adjective | Symbol | Definition | Example |
|-------------------|--------|------------|---------|
| One-time + Many-time | ⚡¹→⚡* | Created once, used many times | UI component with reactive properties |
| Many-time + One-time | ⚡*→⚡¹ | Calculated many times, finalized once | Iterative algorithm with final result |
| Conditional-One-time | ⚡?¹ | Executed once if condition is met | Feature initialized on first use |
| Sequential-Many-time | ⚡*ˢ | Executed many times in sequence | Step-by-step wizard validation |
| Parallel-Many-time | ⚡*ᵖ | Executed many times in parallel | Concurrent data processing |

## Temporal Adjectives in Component Names

NSQL recommends incorporating temporal adjectives into component, function, and variable names:

```r
# One-time components (executed once at initialization)
oneTimeUnionUI <- function(...) { ... }
oneTimeInitializer <- function() { ... }

# Many-time functions (executed repeatedly)
manyTimeFilter <- function(data, condition) { ... }
manyTimeValidator <- function(input) { ... }

# Two-time operations (executed exactly twice)
twoTimeConverter <- function(data) { ... }

# Conditional-time features
conditionalTimeFeature <- function(condition) { ... }
```

## Mapping to Implementation Patterns

These adjectives map directly to implementation patterns:

### One-time (⚡¹) Pattern:
```r
# Executed once during initialization
oneTimeFunction <- function() {
  # Implementation that runs only at startup
}
```

### Many-time (⚡*) Pattern:
```r
# Executed repeatedly in response to reactive changes
manyTimeFunction <- reactive({
  # Implementation that runs every time dependencies change
})
```

### Conditional-time (⚡?) Pattern:
```r
# Executed only if condition is met
conditionalTimeFunction <- function() {
  if (!condition) return(NULL)
  # Implementation that runs conditionally
}
```

## Application to Major Component Types

| Component Type | Temporal Pattern | Description |
|----------------|------------------|-------------|
| UI Definition | One-time (⚡¹) | UI structure created once at initialization |
| Reactive Expression | Many-time (⚡*) | Recalculated whenever dependencies change |
| Event Handler | Every-time (⚡ᵉ) | Executed on each occurrence of an event |
| Initialization | One-time (⚡¹) | Application setup code runs once |
| Cleanup | One-time (⚡¹) | Cleanup code runs once at termination |
| Timer | Periodic-time (⚡ᵖ) | Runs at regular intervals |
| Feature Flag | Conditional-time (⚡?) | Runs only if feature is enabled |

## Integration with Set Theory

Temporal adjectives combine naturally with set operations:

| Operation | Temporal Pattern | Meaning |
|-----------|------------------|---------|
| One-time Union | ⚡¹(A ∪ B) | Union performed once at initialization |
| Many-time Intersection | ⚡*(A ∩ B) | Intersection recalculated reactively |
| Conditional-time Difference | ⚡?(A \ B) | Difference calculated conditionally |
| One-time Many-element Set | ⚡¹{a₁, a₂, ..., aₙ} | Set created once with many elements |

For example, our unionUI component becomes:

```
oneTimeUnionUI = ⚡¹(A ∪ B ∪ C) with manyTimeVisibility
```

Which precisely describes its execution behavior: the union operation happens once at initialization, while visibility is controlled many times during app execution.