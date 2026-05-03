library(shiny)
library(bs4Dash)

#' @title bs4Dash Sidebar Toggle Example
#' @description Demonstrates controlling and reacting to sidebar state
#' @author Claude
#' @date 2025-04-14
#' @export
bs4dash_sidebar_toggle_example <- function() {
  shiny::shinyApp(
    ui = bs4Dash::bs4DashPage(
      header = bs4Dash::bs4DashNavbar(
        title = "Sidebar Toggle Demo"
      ),
      sidebar = bs4Dash::bs4DashSidebar(
        id = "sidebar",
        skin = "light",
        status = "primary",
        bs4Dash::bs4SidebarMenu(
          bs4Dash::bs4SidebarMenuproduct(
            text = "Content",
            tabName = "content",
            icon = shiny::icon("file")
          ),
          bs4Dash::bs4SidebarMenuproduct(
            text = "Settings",
            tabName = "settings",
            icon = shiny::icon("cog")
          )
        )
      ),
      body = bs4Dash::bs4DashBody(
        bs4Dash::bs4Card(
          title = "Sidebar Control",
          status = "primary",
          solidHeader = TRUE,
          width = 12,
          shiny::actionButton(
            inputId = "sidebarToggle", 
            label = "Toggle Sidebar",
            icon = shiny::icon("bars"),
            class = "mb-3"
          ),
          shiny::tags$hr(),
          shiny::h4("Sidebar Status:"),
          shiny::verbatimTextOutput("sidebarStatus")
        )
      )
    ),
    server = function(input, output, session) {
      # React to sidebar state changes
      shiny::observeEvent(input$sidebar, {
        if (input$sidebar) {
          shiny::showModal(shiny::modalDialog(
            title = "Alert",
            "The sidebar is now opened.",
            easyClose = TRUE,
            footer = NULL
          ))
        } else {
          shiny::showNotification(
            "Sidebar closed",
            type = "info",
            duration = 2
          )
        }
      })
      
      # Toggle sidebar when button is clicked
      shiny::observeEvent(input$sidebarToggle, {
        bs4Dash::updatebs4Sidebar(id = "sidebar", session = session)
      })
      
      # Display current sidebar state
      output$sidebarStatus <- shiny::renderText({
        if (is.null(input$sidebar)) {
          return("Sidebar state: initializing...")
        }
        
        if (input$sidebar) {
          return("Sidebar state: OPEN")
        } else {
          return("Sidebar state: CLOSED")
        }
      })
    }
  )
}

#' @title bs4Dash Conditional Menu product Example
#' @description Demonstrates conditional menu products based on user input
#' @author Claude
#' @date 2025-04-14
#' @export
bs4dash_conditional_menu_example <- function() {
  ui <- bs4Dash::bs4DashPage(
    header = bs4Dash::bs4DashNavbar(
      title = "Conditional Menu Demo"
    ),
    sidebar = bs4Dash::bs4DashSidebar(
      bs4Dash::bs4SidebarMenu(
        id = "sidebarMenu",
        bs4Dash::bs4SidebarMenuproduct(
          text = "Main View",
          tabName = "tab1",
          icon = shiny::icon("home")
        ),
        bs4Dash::bs4SidebarMenuproduct(
          condition = "input.show == true",
          text = "Hidden View",
          tabName = "tab2",
          icon = shiny::icon("eye")
        )
      )
    ),
    body = bs4Dash::bs4DashBody(
      bs4Dash::bs4Tabproducts(
        bs4Dash::bs4Tabproduct(
          tabName = "tab1",
          bs4Dash::bs4Card(
            title = "Menu Control",
            status = "primary",
            solidHeader = TRUE,
            width = 12,
            shiny::h3("Welcome to the Conditional Menu Example!"),
            shiny::p("Toggle the checkbox below to show or hide the 'Hidden View' menu product."),
            shiny::checkboxInput("show", "Show Hidden View", FALSE),
            shiny::tags$hr(),
            shiny::p("This example demonstrates how to conditionally display menu products based on user input."),
            shiny::p("It's useful for creating context-sensitive navigation in admin dashboards.")
          )
        ),
        bs4Dash::bs4Tabproduct(
          tabName = "tab2",
          bs4Dash::bs4Card(
            title = "Hidden Content",
            status = "success",
            solidHeader = TRUE,
            width = 12,
            icon = shiny::icon("lock-open"),
            shiny::h3("Congratulations!"),
            shiny::p("You've found the hidden content."),
            shiny::p("This page is only accessible when you enable its menu product.")
          )
        )
      )
    ),
    controlbar = bs4Dash::bs4DashControlbar(
      shiny::div(
        class = "p-3",
        "This example demonstrates conditional menu products that appear or disappear based on user input."
      )
    )
  )
  
  server <- function(input, output, session) {
    # Track menu visibility changes
    shiny::observeEvent(input$show, {
      if (input$show) {
        shiny::showNotification("Hidden menu product is now visible", type = "success")
      } else {
        # If the hidden tab is currently selected, switch to tab1 when hiding
        if (!is.null(input$sidebarMenu) && input$sidebarMenu == "tab2") {
          bs4Dash::updatebs4Tabproducts(session, "sidebarMenu", "tab1")
        }
        shiny::showNotification("Hidden menu product is now invisible", type = "warning")
      }
    })
  }
  
  shiny::shinyApp(ui = ui, server = server)
}

#' @title bs4Dash Complex Navigation Example
#' @description Demonstrates complex navigation with nested menu products and tab management
#' @author Claude
#' @date 2025-04-14
#' @export
bs4dash_complex_navigation_example <- function() {
  shiny::shinyApp(
    ui = bs4Dash::bs4DashPage(
      header = bs4Dash::bs4DashNavbar(skin = "dark"),
      body = bs4Dash::bs4DashBody(
        bs4Dash::bs4Tabproducts(
          bs4Dash::bs4Tabproduct(
            tabName = "tab1",
            bs4Dash::bs4Card(
              title = "Histogram Generator",
              status = "primary",
              solidHeader = TRUE,
              collapsible = TRUE,
              width = 12,
              shiny::sliderInput("obs", "Number of observations:",
                min = 0, max = 1000, value = 500
              ),
              shiny::plotOutput("distPlot")
            )
          ),
          bs4Dash::bs4Tabproduct(
            tabName = "tab2",
            bs4Dash::bs4Card(
              title = "Car Data Explorer",
              status = "info",
              solidHeader = TRUE,
              collapsible = TRUE,
              width = 12,
              shiny::checkboxGroupInput(
                "variable", "Variables to show:",
                c(
                  "Cylinders" = "cyl",
                  "Transmission" = "am",
                  "Gears" = "gear"
                )
              ),
              shiny::tableOutput("data")
            )
          ),
          bs4Dash::bs4Tabproduct(
            tabName = "tab3",
            bs4Dash::bs4Card(
              title = "Simple Toggle",
              status = "warning",
              solidHeader = TRUE,
              width = 12,
              shiny::checkboxInput("val", "Toggle this value", FALSE),
              shiny::textOutput("value")
            )
          ),
          bs4Dash::bs4Tabproduct(
            tabName = "tab4",
            bs4Dash::bs4Card(
              title = "Information",
              status = "success",
              solidHeader = TRUE,
              width = 12,
              "This tab demonstrates a simple content page with no interactive elements."
            )
          ),
          bs4Dash::bs4Tabproduct(
            tabName = "tab5",
            bs4Dash::bs4Card(
              title = "Group 2 - product 1",
              status = "primary",
              solidHeader = TRUE,
              width = 12,
              "Content for the first product in group 2."
            )
          ),
          bs4Dash::bs4Tabproduct(
            tabName = "tab6",
            bs4Dash::bs4Card(
              title = "Group 2 - product 2",
              status = "primary",
              solidHeader = TRUE,
              width = 12,
              "Content for the second product in group 2."
            )
          ),
          bs4Dash::bs4Tabproduct(
            tabName = "tab7",
            bs4Dash::bs4Card(
              title = "Home",
              status = "danger",
              solidHeader = TRUE,
              width = 12,
              shiny::h3("Welcome to the Complex Navigation Example"),
              shiny::p("This example demonstrates:"),
              shiny::tags$ul(
                shiny::tags$li("Nested sidebar menu products"),
                shiny::tags$li("Using the control bar to navigate between tabs"),
                shiny::tags$li("Managing tab states and selection"),
                shiny::tags$li("Tracking expanded/collapsed menu products")
              )
            )
          )
        )
      ),
      sidebar = bs4Dash::bs4DashSidebar(
        skin = "light",
        inputId = "sidebarState",
        bs4Dash::bs4SidebarMenu(
          id = "sidebar",
          bs4Dash::bs4SidebarMenuproduct(
            text = "Data Visualization",
            tabName = "tab1",
            icon = shiny::icon("chart-bar")
          ),
          bs4Dash::bs4SidebarMenuproduct(
            text = "Data Table",
            tabName = "tab2",
            icon = shiny::icon("table"),
            selected = TRUE
          ),
          bs4Dash::bs4SidebarMenuproduct(
            text = "Group 1",
            icon = shiny::icon("folder"),
            startExpanded = TRUE,
            bs4Dash::bs4SidebarMenuSubproduct(
              text = "Toggle Example",
              tabName = "tab3",
              icon = shiny::icon("toggle-on")
            ),
            bs4Dash::bs4SidebarMenuSubproduct(
              text = "Information",
              tabName = "tab4",
              icon = shiny::icon("info-circle")
            )
          ),
          bs4Dash::bs4SidebarMenuproduct(
            text = "Group 2",
            icon = shiny::icon("folder"),
            startExpanded = FALSE,
            bs4Dash::bs4SidebarMenuSubproduct(
              text = "product 1",
              tabName = "tab5",
              icon = shiny::icon("file")
            ),
            bs4Dash::bs4SidebarMenuSubproduct(
              text = "product 2",
              tabName = "tab6",
              icon = shiny::icon("file")
            )
          ),
          bs4Dash::bs4SidebarMenuproduct(
            text = "Home",
            tabName = "tab7",
            icon = shiny::icon("home")
          )
        )
      ),
      controlbar = bs4Dash::bs4DashControlbar(
        skin = "light",
        pinned = TRUE,
        shiny::div(
          class = "p-3",
          shiny::h4("Navigation Controller"),
          shiny::sliderInput(
            inputId = "controller",
            label = "Jump to tab:",
            min = 1,
            max = 7,
            value = 2,
            step = 1,
            ticks = TRUE
          )
        ),
        shiny::div(
          class = "p-3 border-top",
          shiny::h4("Menu State"),
          shiny::verbatimTextOutput("menuState"),
          shiny::verbatimTextOutput("expandedState")
        )
      ),
      footer = bs4Dash::bs4DashFooter(
        left = "bs4Dash Complex Navigation Example",
        right = "Precision Marketing Framework"
      )
    ),
    server = function(input, output, session) {
      # Display menu state information
      output$menuState <- shiny::renderText({
        paste("Active tab:", input$sidebar)
      })
      
      output$expandedState <- shiny::renderText({
        expanded <- input$sidebarproductExpanded
        if (is.null(expanded)) {
          "No menu products expanded"
        } else {
          paste("Expanded products:", paste(expanded, collapse = ", "))
        }
      })
      
      # Update tab selection from the control panel
      shiny::observeEvent(input$controller, {
        bs4Dash::updatebs4Tabproducts(
          session,
          inputId = "sidebar",
          selected = paste0("tab", input$controller)
        )
      }, ignoreInit = TRUE)
      
      # Generate the histogram
      output$distPlot <- shiny::renderPlot({
        hist(rnorm(input$obs), 
             main = "Normal Distribution",
             col = "#5E81AC",
             border = "white")
      })
      
      # Generate the data table
      output$data <- shiny::renderTable({
        mtcars[, c("mpg", input$variable), drop = FALSE]
      }, rownames = TRUE)
      
      # Display checkbox value
      output$value <- shiny::renderText({
        if (input$val) "ON" else "OFF"
      })
    }
  )
}