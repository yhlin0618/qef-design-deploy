# Sidebar Components

This directory contains a collection of sidebar components for the Precision Marketing application, organized according to the UI-Server-Defaults Triple Rule (R11) and Package Consistency Principle (MP19).

## Components Structure

Each sidebar module follows the folder-based organization pattern defined in R11:

```
sidebars/
├── sidebarMain/
│   ├── sidebarMainUI.R
│   ├── sidebarMainServer.R
│   └── sidebarMainDefaults.R
├── sidebarMicro/
│   ├── sidebarMicroUI.R
│   ├── sidebarMicroServer.R
│   └── sidebarMicroDefaults.R
├── sidebarMacro/
│   ├── sidebarMacroUI.R
│   ├── sidebarMacroServer.R
│   └── sidebarMacroDefaults.R
├── sidebarTarget/
│   ├── sidebarTargetUI.R
│   ├── sidebarTargetServer.R
│   └── sidebarTargetDefaults.R
└── sidebarFactory/
    ├── sidebarFactoryUI.R
    ├── sidebarFactoryServer.R
    └── sidebarFactoryDefaults.R
```

## Usage

Each sidebar module exports three main functions:

1. `sidebar*UI()` - The UI component definition
2. `sidebar*Server()` - The server-side logic
3. `sidebar*Defaults()` - Default values for fallback rendering

### Basic Usage Example

```r
# UI with a micro sidebar
ui <- page_sidebar(
  title = "App Title",
  sidebar = sidebarMicroUI("sidebar"),
  
  # Main content
  h1("Main Content")
)

# Server logic
server <- function(input, output, session) {
  # Initialize sidebar
  sidebarMicroServer("sidebar", data_source = reactive({
    # Provide data source
    list(
      customers = reactive({ customer_data })
    )
  }))
}
```

### Factory Pattern Usage

The sidebar factory allows dynamic switching between sidebar types:

```r
# UI with a dynamic sidebar
ui <- page_sidebar(
  title = "App Title",
  sidebar = sidebarFactoryUI("sidebar", section = "main"),
  
  # Main content with navigation
  navset_pill(
    id = "nav",
    nav_panel("Overview", ...),
    nav_panel("Micro", ...),
    nav_panel("Macro", ...),
    nav_panel("Target", ...)
  )
)

# Server with dynamic sidebar
server <- function(input, output, session) {
  # Initialize with factory
  sidebarFactoryServer("sidebar", 
                     section = "main",
                     data_source = reactive({ data_list }))
                     
  # Change sidebar when tab changes
  observeEvent(input$nav, {
    # Map nav value to sidebar section
    section <- switch(input$nav,
      "Overview" = "main",
      "Micro" = "micro",
      "Macro" = "macro",
      "Target" = "target"
    )
    
    # Replace sidebar
    # See examples/sidebar_usage_example.R for implementation details
  })
}
```

## Available Sidebars

1. **Main Sidebar** (`sidebarMain/`)
   - General application filters
   - Date range selection
   - Basic configuration options
   
2. **Micro Sidebar** (`sidebarMicro/`)
   - Customer-level filters
   - RFM filters
   - Customer segment and lifecycle stage filters
   
3. **Macro Sidebar** (`sidebarMacro/`)
   - Aggregation level selection
   - Metric selection
   - Comparison options
   - Chart type selection
   
4. **Target Sidebar** (`sidebarTarget/`)
   - Campaign creation and management
   - Target audience selection
   - Campaign settings configuration
   - Performance projection

5. **Sidebar Factory** (`sidebarFactory/`)
   - Dynamic sidebar switching
   - Unified interface for all sidebar types

See the `examples/` directory for detailed usage patterns.

## Implementation Details

- All sidebars follow the Shiny module pattern with namespaced IDs
- Each sidebar implements the UI-Server-Defaults Triple Rule (R11)
- Function naming follows the Package Consistency Principle (MP19)
- All components handle data source validation and provide fallback values