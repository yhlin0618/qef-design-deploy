---
id: "NSQL_R05"
title: "UI Component Effect Propagation"
type: "rule"
date_created: "2025-04-03"
date_modified: "2025-12-24"
author: "Claude"
previous_id: "R60"
---

# NSQL_R05: UI Component Effect Propagation

> **Note**: This rule was previously R60 in the MAMBA principles system.

## Definition
In a UI component hierarchy, effects propagate downward from parent to child components. When a property affects a container component, it may affect any of its child components. This propagation creates a cascade of effects through the UI tree structure.

## Formal Expression
Given UI components P (parent), C (child), and property X:

1. C ⊂ P (C is a child component of P)
2. X → P (Property X affects parent P)
3. ⊢ X →? C (Therefore, property X may affect child C)

Where:
- → represents "directly affects"
- →? represents "may affect"
- ⊂ represents "is a child component of"

## Explanation
This rule describes how properties and effects cascade through UI component trees. UI components are typically organized in a hierarchical structure (tree), and many properties naturally propagate downward through this hierarchy. Understanding these propagation paths is crucial for designing and debugging UI systems.

## Propagation Patterns in UI Components

### Style Propagation
```nsql
# Theme affects dashboard
Theme → Dashboard
# Header is a component of dashboard
Header ⊂ Dashboard
# Therefore, theme may affect header
∴ Theme →? Header
```

### Visibility Propagation
```nsql
# Tab visibility affects tab content container
TabVisibility → TabContentContainer
# Charts are components within tab content
Charts ⊂ TabContentContainer
# Therefore, tab visibility may affect charts
∴ TabVisibility →? Charts
```

### State Propagation
```nsql
# Filter state affects results panel
FilterState → ResultsPanel
# Table is a component within results panel
Table ⊂ ResultsPanel
# Therefore, filter state may affect table
∴ FilterState →? Table
```

## Practical Implementation in UI Code

### CSS Propagation
```css
/* Parent component style */
.sidebar {
  color: var(--primary-color);
  /* Color propagates to children unless overridden */
}

/* Child component inherits parent's color */
.sidebar-item {
  /* Inherits color from parent */
  padding: 10px;
}
```

### React/Shiny Component Propagation
```jsx
// Parent component passes props to children
function Dashboard({ theme }) {
  // Theme affects Dashboard
  return (
    <div className={`dashboard ${theme}`}>
      {/* Theme may affect Header via props or context */}
      <Header theme={theme} />
      {/* Theme may affect Content via className inheritance */}
      <Content />
    </div>
  );
}
```

### Union Component Propagation
```r
# In oneTimeUnionUI, visibility of the union container affects all components
oneTimeUnionUI(
  component1 = div(...),
  component2 = div(...),
  id = "container"
)

# When container visibility changes, all components are affected
shinyjs::hide("container") # Affects all child components
```

## Types of Effect Propagation in UI

1. **Implicit Propagation**: Effects that automatically cascade through the DOM hierarchy
   - CSS inheritance (colors, fonts, etc.)
   - HTML attribute inheritance (lang, dir, etc.)
   - DOM event bubbling

2. **Explicit Propagation**: Effects that are manually passed to child components
   - Props passing in React/Shiny
   - Context providers
   - Element queries and selector-based styling

3. **Conditional Propagation**: Effects that propagate only under certain conditions
   - CSS cascade with specificity rules
   - Conditional rendering based on propagated state

## Practical UI Design Implications

### Component Design
- Child components should either:
  - Accept and adapt to parent-propagated effects, or
  - Explicitly isolate themselves from unwanted parent effects

### Debugging Strategies
- When troubleshooting UI issues, trace the property propagation path
- Check if unexpected behavior is due to inherited properties from parent components

### Performance Considerations
- Property propagation can have performance implications (e.g., excessive re-renders)
- Consider using memoization or selective updates to optimize propagation

## Exceptions and Limitations

1. **Style Isolation**: CSS encapsulation techniques (Shadow DOM, CSS Modules, etc.) can limit style propagation
2. **Component Boundaries**: Well-designed components may deliberately block certain propagation paths
3. **State Management**: State hoisting or global state can create propagation paths that don't follow the UI hierarchy

## Benefits

1. **Consistent UI**: Leveraging propagation ensures consistent styling and behavior
2. **Reduced Duplication**: Properties can be defined once and propagate to multiple components
3. **Simplified Mental Model**: Predictable propagation paths make UI systems easier to reason about

## Related Principles and Rules

- R59: Component Effect Propagation Rule
- MP28: NSQL Set Theory Foundations
- MP41: Configuration-Driven UI Composition
- P22: CSS Controls Over Shiny Conditionals