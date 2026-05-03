# shiny Usage Guide

## Package Overview

Shiny is an R package that makes it easy to build interactive web applications directly from R. It's the foundation of our precision marketing application's user interface and interactive components.

## Core Components

| Component | Description | Documentation Link |
|-----------|-------------|-------------------|
| Reactive Programming | Understanding reactivity | [reactivity.md](./reactivity.md) |
| UI Components | Common UI elements | [ui_components.md](./ui_components.md) |
| Shiny Modules | Modular UI patterns | [modules.md](./modules.md) |
| Observers & Handlers | Event handling | [observers.md](./observers.md) |
| App Structure | Application organization | [app_structure.md](./app_structure.md) |
| Integration Examples | Project-specific examples | [integration.md](./integration.md) |
| bs4Dash Examples | Dashboard examples | [bs4dash_examples.R](./bs4dash_examples.R) |

## bs4Dash Examples

The `bs4dash_examples.R` file contains the following examples:

1. **minimal_bs4dash_example()**: A minimal bs4Dash application with custom theming
2. **bs4dash_with_sidebar_example()**: An example with sidebar menu and dynamic content 

To run these examples:

```r
source("20_R_packages/shiny/bs4dash_examples.R")
minimal_bs4dash_example()
# or
bs4dash_with_sidebar_example() 
```

These examples follow the principles and rules established in the Precision Marketing framework, particularly:

- **P79**: State Management 
- **P78**: Component Composition
- **R90**: bs4dash Structure Adherence
- **R92**: bs4dash Direct Navigation

## Version Information

- **Required Version**: shiny >= 1.7.0
- **Current Project Version**: 1.7.5
- **Key Dependencies**: htmltools, httpuv, rlang, bs4Dash

## Related Principles

- **MP52**: Unidirectional Data Flow
- **MP54**: UI Server Correspondence
- **R09**: UI Server Defaults Triple
- **R88**: Shiny Module ID Handling
- **R102**: Shiny Reactive Observation Rule
- **R11**: Hybrid Sidebar Pattern
- **R90**: bs4dash Structure Adherence
- **R92**: bs4dash Direct Navigation
- **R105**: Shiny App Templates