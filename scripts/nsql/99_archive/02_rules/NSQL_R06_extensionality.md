---
id: "NSQL_R06"
title: "NSQL Extensionality Principle"
type: "rule"
date_created: "2025-04-03"
date_modified: "2025-12-24"
author: "Claude"
previous_id: "R61"
---

# NSQL_R06: NSQL Extensionality Principle

> **Note**: This rule was previously R61 in the MAMBA principles system.

## Definition
In NSQL, entities are determined by their elements, behaviors, or outputs. Two entities are considered equal if and only if they contain the same elements, exhibit identical behavior under all conditions, or produce the same outputs for all possible inputs.

## Formal Expression
For sets A and B:
- A = B ⟺ ∀x(x ∈ A ⟺ x ∈ B)

For functions f and g:
- f = g ⟺ ∀x(f(x) = g(x))

For components C and D:
- C ≡ D ⟺ ∀inputs(Output(C, inputs) = Output(D, inputs))

Where:
- = represents extensional equality
- ≡ represents behavioral equivalence
- ∈ represents set membership
- ∀ represents universal quantification ("for all")
- ⟺ represents logical equivalence ("if and only if")

## Explanation
The extensionality principle provides a rigorous foundation for determining equality and equivalence across various NSQL domains. It establishes that entities should be identified by their "extension" (what they contain or produce) rather than by their "intension" (how they are defined or implemented).

## Applications in NSQL

### Data Collections Extensionality
```nsql
# Two datasets with identical records are extensionally equal
Dataset1 = {record1, record2, record3}
Dataset2 = {record1, record3, record2}
# Since they contain the same elements, Dataset1 = Dataset2
```

### Query Extensionality
```nsql
# Two queries are equivalent if they always produce the same results
Query1 = SELECT * FROM Sales WHERE region = 'North' AND amount > 1000
Query2 = SELECT * FROM Sales WHERE amount > 1000 AND region = 'North'
# Since they always return the same results, Query1 ≡ Query2
```

### Component Extensionality
```nsql
# Two components are equivalent if they behave identically
Component1 = FilterPanel(data, {region: true, date: true})
Component2 = CustomFilterPanel(data, ["region", "date"])
# If they produce identical UI and behavior for all inputs, Component1 ≡ Component2
```

### Function Extensionality
```nsql
# Two functions are equivalent if they return the same values for all inputs
sum_squares1(x, y) = x² + y²
sum_squares2(a, b) = a² + b²
# Since they return the same result for all inputs, sum_squares1 ≡ sum_squares2
```

## Practical Implementation

### Data Equivalence Testing
```r
# Test if two datasets are extensionally equal
is_extensionally_equal <- function(dataset1, dataset2) {
  if (length(dataset1) != length(dataset2)) return(FALSE)
  all(sort(dataset1) == sort(dataset2))
}
```

### Component Equivalence Testing
```r
# Test if two components are behaviorally equivalent
is_component_equivalent <- function(comp1, comp2, test_cases) {
  all(sapply(test_cases, function(input) {
    identical(render_output(comp1, input), render_output(comp2, input))
  }))
}
```

### Query Optimization
```r
# Replace a query with an extensionally equivalent but more efficient version
optimize_query <- function(query) {
  # For each optimization rule
  for (rule in optimization_rules) {
    # If the rule produces an extensionally equivalent query
    if (is_extensionally_equivalent(query, rule$transform(query))) {
      # Use the optimized version
      query <- rule$transform(query)
    }
  }
  return(query)
}
```

## Applications and Benefits

### 1. Component Substitution
The extensionality principle allows for the substitution of components with equivalent behavior:

```r
# If two components are extensionally equivalent
if (is_component_equivalent(LegacyTable, EnhancedTable)) {
  # We can safely substitute one for the other
  replace_component(LegacyTable, EnhancedTable)
}
```

### 2. Query Optimization
Extensionality enables query optimization by transformation to equivalent forms:

```r
# Transform a query to an extensionally equivalent but more efficient form
efficient_query <- optimize_to_equivalent(original_query)
```

### 3. Testing and Verification
Extensionality provides a formal basis for testing component behavior:

```r
# Verify that a refactored component behaves identically to the original
test_that("refactored component is extensionally equivalent", {
  expect_true(is_component_equivalent(original_component, refactored_component))
})
```

### 4. Cache Invalidation
Extensionality helps determine when cached results need to be refreshed:

```r
# Only recompute if the input has extensionally changed
if (!is_extensionally_equal(current_input, cached_input)) {
  refresh_results(current_input)
}
```

## Exceptions and Limitations

1. **Performance Characteristics**: Extensional equality doesn't account for performance differences
2. **Side Effects**: Pure extensional equivalence doesn't consider side effects
3. **Implementation Complexity**: Proving extensional equivalence can be computationally expensive

## Related Concepts

- **Intensional Definition**: Defining an entity by its properties rather than its elements
- **Observational Equivalence**: Two entities are equivalent if they cannot be distinguished by any observation
- **Behavioral Subtyping**: A concept related to the Liskov Substitution Principle

## Related Principles and Rules

- MP28: NSQL Set Theory Foundations
- R59: Component Effect Propagation Rule
- R60: UI Component Effect Propagation
- R23: Mathematical Precision