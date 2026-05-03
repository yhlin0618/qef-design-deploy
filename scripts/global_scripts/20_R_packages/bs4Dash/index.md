# bs4Dash Package Reference

*This documentation follows the Package Documentation Reference Rule (R103)*

## Overview

`bs4Dash` is a Bootstrap 4 implementation of the AdminLTE3 template for Shiny dashboards. It provides an enhanced and modernized alternative to the `shinydashboard` package with a more contemporary look, additional components, and improved functionality.

## Standard Usage Patterns

### Basic Dashboard Structure

The standard pattern for creating a bs4Dash application follows this structure:

```r
library(shiny)
library(bs4Dash)

ui <- dashboardPage(
  dashboardHeader(title = "Dashboard Title"),
  dashboardSidebar(
    sidebarMenu(
      menuproduct("Dashboard", tabName = "dashboard", icon = icon("dashboard")),
      menuproduct("Analysis", tabName = "analysis", icon = icon("chart-bar"))
    )
  ),
  dashboardBody(
    tabproducts(
      tabproduct(tabName = "dashboard",
        # Dashboard content
      ),
      tabproduct(tabName = "analysis",
        # Analysis content
      )
    )
  ),
  controlbar = dashboardControlbar(),
  footer = dashboardFooter()
)

server <- function(input, output, session) {
  # Server logic
}

shinyApp(ui, server)
```

### Preferred Component Patterns

#### Box Components

Use these patterns for box components:

```r
# Standard box
box(
  title = "Box Title",
  width = 6,
  solidHeader = TRUE,
  status = "primary",
  collapsible = TRUE,
  plotOutput("plot")
)

# Value Box
valueBox(
  value = "75%",
  subtitle = "Task Completion",
  icon = icon("tasks"),
  width = 3,
  color = "info"
)

# Info Box
infoBox(
  title = "New Customers",
  value = 120,
  icon = icon("users"),
  color = "success",
  width = 3
)

# Box with sidebar
boxSidebar(
  id = "myboxsidebar",
  startOpen = FALSE,
  sliderInput("obs", "Number of observations:", 
              min = 1, max = 1000, value = 500)
)
```

#### Menu Components

Navigation menus should follow this pattern:

```r
sidebarMenu(
  id = "sidebar",
  menuproduct(
    text = "Dashboard",
    tabName = "dashboard",
    icon = icon("dashboard")
  ),
  menuproduct(
    text = "Charts",
    tabName = "charts",
    icon = icon("chart-bar"),
    # Nested menus for subcategories
    menuSubproduct(
      text = "Line Charts",
      tabName = "line_charts"
    ),
    menuSubproduct(
      text = "Bar Charts",
      tabName = "bar_charts"
    )
  )
)
```

#### Control Components

Standard control bar pattern:

```r
dashboardControlbar(
  id = "controlbar",
  skin = "light",
  pinned = TRUE,
  overlay = FALSE,
  controlbarMenu(
    id = "controlbarMenu",
    type = "pills",
    controlbarproduct(
      "Settings",
      sliderInput("slider", "Number of observations:", 1, 100, 50)
    ),
    controlbarproduct(
      "Help",
      helpText("This is a help text")
    )
  )
)
```

## Configuration Standards

### Color System

Use the following standard color options for consistency:

```r
# Standard colors
validColors <- c("primary", "secondary", "success", "warning", 
                "danger", "info", "dark", "light")

# Status colors for semantic meaning
validStatuses <- c("primary", "success", "warning", "danger", "info")
```

### Skin Configuration

Use a consistent skin configuration:

```r
# Create theme with bs4Dash
dashboardPage(
  # ... other components
  freshTheme = create_theme(
    bs4dash_vars(
      navbar_light_color = "#bec5cb",
      navbar_light_active_color = "#FFF",
      navbar_light_hover_color = "#FFF"
    ),
    bs4dash_yiq(
      contrasted_threshold = 10,
      text_dark = "#FFF",
      text_light = "#272c30"
    ),
    bs4dash_layout(
      main_bg = "#f4f6f9"
    ),
    bs4dash_sidebar_light(
      bg = "#272c30",
      color = "#bec5cb",
      hover_color = "#FFF"
    ),
    bs4dash_status(
      primary = "#5E81AC", 
      danger = "#BF616A", 
      warning = "#EBCB8B",
      success = "#A3BE8C",
      info = "#88C0D0"
    )
  )
)
```

### Function Selection Guidelines

#### Dashboard Components

| Function | When to Use |
|----------|-------------|
| `dashboardPage()` | Main container for all bs4Dash apps |
| `dashboardHeader()` | Top navigation bar |
| `dashboardSidebar()` | Left navigation menu |
| `dashboardBody()` | Main content area |
| `dashboardControlbar()` | Right sidebar for controls (Optional) |
| `dashboardFooter()` | Bottom footer (Optional) |

#### Box Components

| Function | When to Use |
|----------|-------------|
| `box()` | General purpose content container |
| `valueBox()` | Display KPIs and key metrics |
| `infoBox()` | Similar to valueBox but with different styling |
| `tabBox()` | Content with tabs for switching between views |
| `userBox()` | Display user information |
| `socialBox()` | Display social media stats |

#### Navigation Components

| Function | When to Use |
|----------|-------------|
| `sidebarMenu()` | Container for menu products |
| `menuproduct()` | Individual menu options |
| `menuSubproduct()` | Nested menu products |
| `navbarMenu()` | Top navigation bar dropdown |
| `tabproducts()` | Container for tabproduct content |
| `tabproduct()` | Content associated with a menu tab |

## Integration Examples

### Integration with DataTable

```r
library(shiny)
library(bs4Dash)
library(DT)

ui <- dashboardPage(
  dashboardHeader(title = "DataTable Integration"),
  dashboardSidebar(),
  dashboardBody(
    box(
      title = "DataTable", 
      status = "primary", 
      width = 12,
      DTOutput("table")
    )
  )
)

server <- function(input, output) {
  output$table <- renderDT({
    datatable(mtcars, options = list(
      pageLength = 10,
      autoWidth = TRUE
    ))
  })
}

shinyApp(ui, server)
```

### Integration with plotly

```r
library(shiny)
library(bs4Dash)
library(plotly)

ui <- dashboardPage(
  dashboardHeader(title = "Plotly Integration"),
  dashboardSidebar(),
  dashboardBody(
    box(
      title = "Plotly Chart",
      width = 12,
      status = "info",
      plotlyOutput("plot")
    )
  )
)

server <- function(input, output) {
  output$plot <- renderPlotly({
    plot_ly(data = iris, x = ~Sepal.Length, y = ~Petal.Length, 
            color = ~Species, type = "scatter", mode = "markers")
  })
}

shinyApp(ui, server)
```

## Versioning Requirements

Our project requires **bs4Dash >= 2.0.0** which uses the AdminLTE3 framework. The current recommended version is 2.3.4.

Key version features:
- 2.0.0: Initial BS4 upgrade
- 2.1.0: Fresh theming implementation
- 2.2.0: Enhanced control bar
- 2.3.0: Improved sidebar
- 2.3.4: Bug fixes and stability improvements

## Project-Specific Standards

### Dashboard Layout Structure

For all applications in this project, follow this file organization:

1. Define UI components in separate files
2. Use a `ui.R` file for assembling components
3. Define server logic for each tab in separate files
4. Use a `server.R` file to connect all server logic

### Component Naming Conventions

Use these naming conventions for consistency:

- Box IDs: `box_[purpose]_[number]` (e.g., `box_overview_1`)
- Tab Names: `tab_[purpose]` (e.g., `tab_analysis`)
- Input IDs: `input_[type]_[purpose]` (e.g., `input_date_range`)
- Output IDs: `output_[type]_[purpose]` (e.g., `output_plot_trends`)

### Project Color Theme

The standard project theme uses these colors:

- Primary: #5E81AC
- Secondary: #3B4252
- Success: #A3BE8C
- Warning: #EBCB8B
- Danger: #BF616A
- Info: #88C0D0
- Light: #ECEFF4
- Dark: #2E3440

## Common Pitfalls and Solutions

| Issue | Solution |
|-------|----------|
| Box content overflow | Use `overflow-y: auto` in the box's style attribute |
| Sidebar products not highlighting | Make sure tabName matches exactly in menuproduct and tabproduct |
| AdminLTE JS conflicts | Always use bs4Dash's JS functions, not direct AdminLTE JS |
| Nested boxes not working | Use box() inside column() inside fluidRow() |
| Theme not applying | Ensure freshTheme is properly configured in dashboardPage() |

## Related Resources

- [Official bs4Dash Documentation](https://rinterface.github.io/bs4Dash/)
- [AdminLTE3 Documentation](https://adminlte.io/docs/3.0/)
- [Bootstrap 4 Documentation](https://getbootstrap.com/docs/4.6/getting-started/introduction/)

## Example Files

- [bs4dash_examples.R](bs4dash_examples.R): Basic examples demonstrating theming and layout
- [bs4dash_interaction_examples.R](bs4dash_interaction_examples.R): More advanced examples focused on interaction and navigation
- [minimal_examples.R](minimal_examples.R): Minimal examples for individual components following P105

## Available Examples

### Basic Examples

1. **minimal_bs4dash_example()**: A minimal bs4Dash application with custom theming
2. **bs4dash_with_sidebar_example()**: An example with sidebar menu and dynamic content

### Interaction Examples

1. **bs4dash_sidebar_toggle_example()**: Demonstrates sidebar toggling and state management
2. **bs4dash_conditional_menu_example()**: Shows how to create conditional menu products
3. **bs4dash_complex_navigation_example()**: Demonstrates advanced navigation techniques

### Minimal Component Examples (P105)

1. **minimal_dashboard()**: The absolute minimal functioning bs4Dash dashboard
2. **minimal_single_tab()**: Minimal example with a single tab
3. **minimal_two_tabs()**: Minimal example with two tabs
4. **minimal_box_components()**: Various box component examples
5. **minimal_value_boxes()**: Value and info box examples
6. **minimal_controlbar()**: Minimal controlbar implementation
7. **minimal_tabbox()**: TabBox component examples
8. **minimal_card()**: Card component examples
9. **minimal_social_box()**: SocialBox component example
10. **minimal_user_messages()**: User notification examples

## Related Principles

- **R90**: bs4dash Structure Adherence
- **R92**: bs4dash Direct Navigation
- **P78**: Component Composition
- **P79**: State Management
- **P105**: Minimal Example Construction
- **MP52**: Unidirectional Data Flow
- **MP54**: UI-Server Correspondence
- **R103**: Package Documentation Reference Rule
- **R104**: Package Function Reference Rule