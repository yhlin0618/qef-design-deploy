# Sidebar Usage Example
# This file demonstrates how to use the different sidebar components

library(shiny)
library(bslib)

# UI definition with sidebar
ui <- page_sidebar(
  title = "Precision Marketing App",
  sidebar = sidebarFactoryUI("sidebar", section = "main"),
  
  # Main layout with navigation
  navset_pill(
    id = "nav",
    nav_panel(
      "Overview",
      h2("Main Overview"),
      p("This page would show high-level metrics and KPIs.")
    ),
    nav_panel(
      "Micro",
      # When switching to this tab, sidebar will change to micro
      h2("Micro-Level Analysis"),
      p("Customer-level data and analysis would be displayed here.")
    ),
    nav_panel(
      "Macro",
      # When switching to this tab, sidebar will change to macro
      h2("Macro-Level Analysis"),
      p("Aggregate data and trends would be displayed here.")
    ),
    nav_panel(
      "Target",
      # When switching to this tab, sidebar will change to target
      h2("Target Marketing"),
      p("Campaign planning and execution would be handled here.")
    )
  )
)

# Server logic
server <- function(input, output, session) {
  # Initialize sidebar based on current tab
  current_section <- reactiveVal("main")
  
  # Watch for tab changes to update sidebar
  observeEvent(input$nav, {
    # Map nav value to sidebar section
    section <- switch(input$nav,
      "Overview" = "main",
      "Micro" = "micro",
      "Macro" = "macro",
      "Target" = "target",
      "main" # Default if no match
    )
    
    # Update current section
    current_section(section)
    
    # Remove old sidebar
    removeUI(selector = "#sidebar-content")
    
    # Insert new sidebar based on selected section
    insertUI(
      selector = "#sidebar-placeholder",
      where = "afterBegin",
      ui = div(
        id = "sidebar-content",
        sidebarFactoryUI(paste0("sidebar_", section), section = section)
      )
    )
    
    # Initialize new sidebar server
    sidebarFactoryServer(
      paste0("sidebar_", section), 
      section = section,
      data_source = reactive({
        # In a real app, this would be section-specific data
        list()
      })
    )
  })
  
  # Initialize the default sidebar
  sidebarFactoryServer(
    "sidebar", 
    section = "main",
    data_source = reactive({
      # Initial data source
      list()
    })
  )
}

# Run the app
shinyApp(ui, server)