---
id: "NSQL_R04"
title: "Component Effect Propagation Rule"
type: "rule"
date_created: "2025-04-03"
date_modified: "2025-12-24"
author: "Claude"
previous_id: "R59"
---

# NSQL_R04: Component Effect Propagation Rule

> **Note**: This rule was previously R59 in the MAMBA principles system.

## Definition
In a component hierarchy, if component A affects component B, then A may affect any subcomponent of B. Formally: if B is a function of A, then any component of B may also be a function of A, with effect propagation flowing downward through the component hierarchy.

## Formal Expression
Given components A, B, and C where C is a subcomponent of B:

1. A → B (A affects B)
2. C ⊂ B (C is a subcomponent of B)
3. ⊢ A →? C (Therefore, A may affect C)

Where:
- → represents "affects" or "is a function of"
- →? represents "may affect"
- ⊂ represents "is a subcomponent of"

## Explanation
This inference rule describes how effects propagate through component hierarchies in software systems. It establishes that when a component affects another component, this effect may cascade down to the affected component's subcomponents.

For example, if a configuration parameter affects a UI container, we can infer that it may also affect individual UI elements within that container. This propagation is not necessarily guaranteed (hence "may affect"), as subcomponents might be isolated from certain effects through encapsulation.

## Application in NSQL

### Component Hierarchies
```nsql
# Configuration affects sidebar
Config → Sidebar
# Filters are part of sidebar
Filters ⊂ Sidebar
# Therefore, configuration may affect filters
∴ Config →? Filters
```

### UI Component Example
```nsql
# Theme affects main container
Theme → MainContainer
# Button is part of main container
Button ⊂ MainContainer
# Therefore, theme may affect button
∴ Theme →? Button
```

### Data Transformation Example
```nsql
# Raw data affects aggregated data
RawData → AggregatedData
# Specific metrics are part of aggregated data
Metrics ⊂ AggregatedData
# Therefore, raw data may affect metrics
∴ RawData →? Metrics
```

## Practical Implementation

### Dependency Tracking
```r
# Define component dependency
register_dependency("config", "sidebar")

# Infer potential subcomponent dependencies
for (subcomponent in get_subcomponents("sidebar")) {
  infer_potential_dependency("config", subcomponent)
}
```

### Change Impact Analysis
```r
# When element A changes
if (has_changed("config")) {
  # Mark direct dependencies for update
  mark_for_update("sidebar")
  
  # Consider potential impacts on subcomponents
  for (subcomponent in get_subcomponents("sidebar")) {
    mark_for_potential_update(subcomponent, source = "config")
  }
}
```

## Exceptions and Limitations

1. **Encapsulation**: Well-encapsulated subcomponents may be shielded from certain effects of their parent components.

2. **Independence**: Some subcomponents are designed to be independent of specific aspects of their parent components.

3. **Non-Deterministic Propagation**: The "may affect" relationship is inherently probabilistic, not deterministic.

## Benefits

1. **Change Impact Prediction**: Helps anticipate how changes might propagate through a system
2. **Debugging Assistance**: Guides troubleshooting by identifying potential effect chains
3. **Design Insight**: Encourages thinking about component relationships and effect isolation

## Related Principles and Rules

- MP28: NSQL Set Theory Foundations
- MP41: Configuration-Driven UI Composition
- R58: Evolution Over Replacement