---
id: "NSQL_R03"
title: "Mathematical Precision Priority"
type: "rule"
date_created: "2025-04-03"
date_modified: "2025-12-24"
author: "Claude"
previous_id: "R23"
---

# NSQL_R03: Mathematical Precision Priority

> **Note**: This rule was previously R23 in the MAMBA principles system.

## Definition
When multiple terminology options exist in NSQL, prefer the more mathematically precise term that unambiguously represents the underlying concept. Mathematical formalism takes precedence over natural language convenience when clarity is at stake.

## Explanation
Natural language is inherently ambiguous, while mathematical notation is designed for precision. NSQL bridges these worlds, but when conflicts arise, mathematical clarity should prevail. This rule ensures that terminology choices minimize cognitive load through precise, unambiguous expression.

## Implementation Guidelines

### 1. Set Theory Terminology Preference

When naming components and operations related to sets, prefer formal set theory terms:

```r
# PREFERRED: Mathematical precision
sidebarUnionUI <- function(id) { ... }  # Union operation is precise

# AVOID: Ambiguous terminology
sidebarCommonUI <- function(id) { ... }  # "Common" could mean intersection or shared or general
```

### 2. Function Naming Hierarchy

Follow this hierarchy when naming functions:

1. Mathematical terms (e.g., `union`, `intersection`, `complement`)
2. Formal computing terms (e.g., `merge`, `join`, `filter`)
3. Domain-specific technical terms (e.g., `aggregate`, `summarize`)
4. General natural language (e.g., `combine`, `process`)

### 3. Documentation with Formal Notation

Supplement implementation with formal mathematical notation:

```r
#' @description
#' Implements: Result = A ∪ B
#' Where:
#'  - A represents the common filters
#'  - B represents the tab-specific filters
unionFilters <- function(common_filters, tab_filters) { ... }
```

### 4. Decision Matrix for Term Selection

| Consideration | Higher Priority | Lower Priority |
|---------------|-----------------|----------------|
| Precision | Unambiguous mathematical term | Ambiguous term with multiple interpretations |
| Universality | Well-established in mathematics | Domain-specific or colloquial |
| Computability | Directly translatable to code | Requires interpretation |
| Consistency | Aligns with NSQL dictionary | Novel terminology |

### 5. Refactoring Guidance

When refactoring to achieve mathematical precision:

```r
# Original ambiguous naming
# dataLoader.R (What type of loading? How is data organized?)

# Refactored with mathematical precision
# setProjection.R (Clearly indicates a projection operation on a set)
```

## Examples

### 1. Set Operations

| NSQL Preferred Term | Ambiguous Alternatives | Mathematical Basis |
|---------------------|------------------------|-------------------|
| `union` | `combine`, `merge` | A ∪ B |
| `intersection` | `common`, `shared` | A ∩ B |
| `difference` | `exclude`, `remove` | A \ B |
| `complement` | `inverse`, `opposite` | A' or Ac |
| `product` | `pair`, `cross` | A × B |

### 2. Real Code Examples

```r
# Before applying R23
filterInputs <- function(data, conditions) {
  # Ambiguous: What kind of filtering? Is it selection, projection, or something else?
}

# After applying R23
setSelection <- function(data, predicate) {
  # Precise: Implements σ_predicate(data) from relational algebra
}
```

### 3. Component Hierarchy Representation

```r
# Mathematical representation of UI structure
# Dashboard = Header ∪ Sidebar ∪ MainContent
# Sidebar = CommonFilters ∪ TabSpecificFilters(ActiveTab)
# TabSpecificFilters = {MicroFilters, MacroFilters, TargetFilters}
```

## Benefits

1. **Reduced Ambiguity**: Mathematical terms have precise, established meanings
2. **Improved Communication**: Team members share the same mental model
3. **Better Maintainability**: Intentions are clear through mathematical definition
4. **Enhanced Reasoning**: Enables formal reasoning about code behavior
5. **Deeper Insight**: Exposes underlying structure of operations

## Handling Exceptions

In some cases, domain-specific terminology may be required for stakeholder communication:

```r
# Exception: Domain-specific term with mathematical explanation
#' @description
#' Implements customer segmentation algorithm
#' Mathematically: Segmentation = Partition(Customers) where
#' Partition P = {P₁, P₂, ..., Pₙ} such that:
#' - ∀i, Pᵢ ≠ ∅ (no empty segments)
#' - ⋃ᵢ₌₁ⁿPᵢ = Customers (covers all customers)
#' - ∀i≠j, Pᵢ ∩ Pⱼ = ∅ (segments are disjoint)
segmentCustomers <- function(customers, criteria) { ... }
```

## Case Study: Sidebar Filter Component

The application of R23 to our sidebar component demonstrates mathematical precision:

**Original Naming**: `sidebarCommonUI`
- "Common" suggests these are general filters shared across tabs
- But it doesn't precisely describe the component's operation on filter sets
- Could be misinterpreted as "filters that are common to all tabs" (intersection)

**Revised Naming**: `sidebarUnionUI`
- "Union" directly references the set operation being performed
- Mathematically: Sidebar = CommonFilters ∪ TabSpecificFilters[ActiveTab]
- Precisely describes that the sidebar combines two distinct sets of filters

**Implementation Impact**:
```r
# Before: Name doesn't convey the mathematical operation
sidebar_filters <- sidebarCommonServer("sidebar", active_tab)
# Combined filters available to: common_filters and tab_filters? How?

# After: Name clearly conveys the union operation 
sidebar_filters <- sidebarUnionServer("sidebar", active_tab)
# Return value is clearly the union of common and tab-specific filters
```

## Anti-Patterns

### 1. Ambiguous Natural Language Names

Avoid:
```r
# Bad: Unclear what "common" means mathematically
getCommonValues <- function(list1, list2) {
  # Actually performs set intersection
  return(intersect(list1, list2))
}
```

Prefer:
```r
# Good: Clear mathematical operation
setIntersection <- function(set1, set2) {
  return(intersect(set1, set2))
}
```

### 2. Mixed Mathematical Models

Avoid:
```r
# Bad: Mixes graph theory and set theory in confusing ways
treeUnion <- function(tree1, tree2) {
  # Not a mathematical union operation on trees
}
```

Prefer:
```r
# Good: Clear which mathematical domain applies
forestUnion <- function(forest1, forest2) {
  # A forest is a set of trees, so union has clear meaning
}

# Or
graphMerge <- function(tree1, tree2) {
  # Clear that we're doing a graph operation, not set union
}
```

### 3. Hiding Mathematical Operations

Avoid:
```r
# Bad: Hides the underlying set operations
processUserData <- function(user_data, preferences) {
  # Performs multiple set operations internally
}
```

Prefer:
```r
# Good: Makes set operations explicit
userDataSetOperations <- function(user_data, preferences) {
  eligible_users <- setIntersection(user_data, active_users)
  relevant_preferences <- setProjection(preferences, preference_keys)
  return(setJoin(eligible_users, relevant_preferences))
}
```

## Integration with NSQL Dictionary

When implementing R23, update the NSQL dictionary with mathematical definitions:

```yaml
terms:
  - term: "union"
    nsql_meaning: "Combines two sets to form a set containing all elements from both"
    mathematical_notation: "A ∪ B"
    examples:
      - "sidebarUnionUI (UI component combining filter sets)"
      - "unionDatasets (data operation combining record sets)"
  
  - term: "intersection"
    nsql_meaning: "Creates a set containing only elements common to both input sets"
    mathematical_notation: "A ∩ B"
    examples:
      - "filterIntersection (returns records matching multiple criteria)"
```

## Related Rules

- R21: NSQL Dictionary
- R22: NSQL Update
- MP24: Natural SQL Language
- MP27: Specialized Natural SQL Language