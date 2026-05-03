#' @file minimal_examples.R
#' @principle RC04 Package Documentation Reference Code
#' @principle P105 Minimal Example Construction Principle
#' @author Claude
#' @date 2025-04-14
#' @description Minimal examples for bs4Dash components following the P105 principle
#' @export

library(shiny)
library(bs4Dash)

#' @title Minimal Dashboard Example
#' @description The absolute minimal functioning bs4Dash dashboard
#' @export
minimal_dashboard <- function() {
  shinyApp(
    ui = dashboardPage(
      header = dashboardHeader(title = "Minimal Dashboard"),
      sidebar = dashboardSidebar(),
      body = dashboardBody(
        box(title = "Hello World")
      )
    ),
    server = function(input, output) { }
  )
}

#' @title Minimal Single-Tab Example
#' @description Minimal example showing a dashboard with a single tab
#' @export
minimal_single_tab <- function() {
  shinyApp(
    ui = dashboardPage(
      header = dashboardHeader(title = "Single Tab Example"),
      sidebar = dashboardSidebar(
        sidebarMenu(
          menuproduct(text = "Dashboard", tabName = "dashboard", icon = icon("gauge"))
        )
      ),
      body = dashboardBody(
        tabproducts(
          tabproduct(
            tabName = "dashboard",
            box(title = "Dashboard Content", width = 12)
          )
        )
      )
    ),
    server = function(input, output) { }
  )
}

#' @title Minimal Two-Tab Example
#' @description Minimal example showing a dashboard with two tabs
#' @export
minimal_two_tabs <- function() {
  shinyApp(
    ui = dashboardPage(
      header = dashboardHeader(title = "Two Tabs Example"),
      sidebar = dashboardSidebar(
        sidebarMenu(
          id = "sidebar",
          menuproduct(text = "Tab 1", tabName = "tab1", icon = icon("table")),
          menuproduct(text = "Tab 2", tabName = "tab2", icon = icon("chart-bar"))
        )
      ),
      body = dashboardBody(
        tabproducts(
          tabproduct(
            tabName = "tab1",
            box(title = "Tab 1 Content", width = 12)
          ),
          tabproduct(
            tabName = "tab2",
            box(title = "Tab 2 Content", width = 12)
          )
        )
      )
    ),
    server = function(input, output, session) {
      # Output current tab for demonstration
      output$currentTab <- renderText({
        paste("Current tab:", input$sidebar)
      })
    }
  )
}

#' @title Minimal Box Components Example
#' @description Minimal example showing different box components and layouts
#' @export
minimal_box_components <- function() {
  shinyApp(
    ui = dashboardPage(
      header = dashboardHeader(title = "Box Components"),
      sidebar = dashboardSidebar(),
      body = dashboardBody(
        fluidRow(
          # Standard box
          box(
            title = "Standard Box",
            "This is a standard box.",
            width = 6
          ),
          
          # Box with status color
          box(
            title = "Status Box",
            "This box has a status color.",
            status = "primary",
            width = 6
          )
        ),
        fluidRow(
          # Solid header box
          box(
            title = "Solid Header Box",
            "This box has a solid header.",
            solidHeader = TRUE,
            status = "info",
            width = 6
          ),
          
          # Collapsible box
          box(
            title = "Collapsible Box",
            "This box can be collapsed.",
            collapsible = TRUE,
            width = 6
          )
        ),
        fluidRow(
          # Box with custom tools
          box(
            title = "Box with Tools",
            "This box has custom tools.",
            status = "warning",
            width = 12,
            solidHeader = TRUE,
            boxToolSize = "sm",
            boxToolbar = tagList(
              boxDropdown(
                boxDropdownproduct("product 1"),
                boxDropdownproduct("product 2")
              ),
              boxTool(uiOutput("tool"))
            )
          )
        )
      )
    ),
    server = function(input, output) {
      output$tool <- renderUI({
        tags$i(class = "fas fa-gear")
      })
    }
  )
}

#' @title Minimal Value Boxes Example
#' @description Minimal example showing different value box components
#' @export
minimal_value_boxes <- function() {
  shinyApp(
    ui = dashboardPage(
      header = dashboardHeader(title = "Value Boxes"),
      sidebar = dashboardSidebar(),
      body = dashboardBody(
        fluidRow(
          # Basic value box
          valueBox(
            value = 150,
            subtitle = "New Orders",
            icon = icon("shopping-cart"),
            width = 4
          ),
          
          # Value box with color
          valueBox(
            value = "$10,350",
            subtitle = "Revenue",
            icon = icon("dollar-sign"),
            color = "success",
            width = 4
          ),
          
          # Value box with footer
          valueBox(
            value = "25%",
            subtitle = "Conversion Rate",
            icon = icon("percentage"),
            color = "primary",
            footer = "Increased by 5%",
            width = 4
          )
        ),
        br(),
        fluidRow(
          # Info box (different style)
          infoBox(
            title = "CPU Usage",
            value = "25%",
            subtitle = "Last updated: 1 min ago",
            icon = icon("microchip"),
            color = "primary",
            width = 6
          ),
          
          # Gradient info box
          infoBox(
            title = "Memory",
            value = "6.5 GB",
            subtitle = "Free: 3.5 GB",
            icon = icon("memory"),
            color = "danger",
            gradient = TRUE,
            width = 6
          )
        )
      )
    ),
    server = function(input, output) { }
  )
}

#' @title Minimal Controlbar Example
#' @description Minimal example showing a controlbar with basic functionality
#' @export
minimal_controlbar <- function() {
  shinyApp(
    ui = dashboardPage(
      header = dashboardHeader(title = "Controlbar Example"),
      sidebar = dashboardSidebar(),
      body = dashboardBody(
        box(
          title = "Main Content",
          width = 12,
          "Adjust settings using the controlbar on the right. Click the gear icon to open it."
        ),
        verbatimTextOutput("settings")
      ),
      controlbar = dashboardControlbar(
        id = "controlbar",
        collapsed = TRUE,
        overlay = TRUE,
        div(
          class = "p-3",
          h4("Settings"),
          sliderInput(
            "slider", 
            "Number of observations:", 
            min = 10, 
            max = 100, 
            value = 50
          ),
          checkboxInput(
            "toggle", 
            "Show Advanced Options", 
            value = FALSE
          )
        )
      )
    ),
    server = function(input, output) {
      output$settings <- renderPrint({
        list(
          slider_value = input$slider,
          show_advanced = input$toggle
        )
      })
    }
  )
}

#' @title Minimal Tabbox Example
#' @description Minimal example showing a tabBox component with multiple tabs
#' @export
minimal_tabbox <- function() {
  shinyApp(
    ui = dashboardPage(
      header = dashboardHeader(title = "TabBox Example"),
      sidebar = dashboardSidebar(),
      body = dashboardBody(
        fluidRow(
          tabBox(
            id = "tabbox1",
            title = "Basic TabBox",
            width = 12,
            tabPanel(
              tabName = "Tab 1",
              "Content for tab 1"
            ),
            tabPanel(
              tabName = "Tab 2",
              "Content for tab 2"
            )
          )
        ),
        fluidRow(
          tabBox(
            id = "tabbox2",
            title = "TabBox with Icons",
            width = 12,
            side = "right",
            selected = "Tab B",
            status = "primary",
            solidHeader = TRUE,
            type = "tabs",
            tabPanel(
              tabName = "Tab A",
              icon = icon("table"),
              "Content for tab A with table icon"
            ),
            tabPanel(
              tabName = "Tab B",
              icon = icon("chart-line"),
              "Content for tab B with chart icon"
            ),
            tabPanel(
              tabName = "Tab C",
              icon = icon("gear"),
              "Content for tab C with gear icon"
            )
          )
        )
      )
    ),
    server = function(input, output) {
      observe({
        print(paste("TabBox 1 active tab:", input$tabbox1))
        print(paste("TabBox 2 active tab:", input$tabbox2))
      })
    }
  )
}

#' @title Minimal Card Example
#' @description Minimal example showing different card components
#' @export
minimal_card <- function() {
  shinyApp(
    ui = dashboardPage(
      header = dashboardHeader(title = "Card Examples"),
      sidebar = dashboardSidebar(),
      body = dashboardBody(
        fluidRow(
          # Basic card
          bs4Card(
            title = "Basic Card",
            "This is a basic card component.",
            width = 6
          ),
          
          # Card with status and solid header
          bs4Card(
            title = "Status Card",
            "This card has a status color and solid header.",
            status = "primary",
            solidHeader = TRUE,
            width = 6
          )
        ),
        fluidRow(
          # Card with footer
          bs4Card(
            title = "Card with Footer",
            "This card has a footer element.",
            footer = "Last updated: Today",
            elevation = 3,
            width = 6
          ),
          
          # Card with tools
          bs4Card(
            title = "Interactive Card",
            "This card has interactive tools.",
            status = "warning",
            width = 6,
            solidHeader = TRUE,
            collapsible = TRUE,
            closable = TRUE,
            maximizable = TRUE
          )
        )
      )
    ),
    server = function(input, output) { }
  )
}

#' @title Minimal SocialBox Example
#' @description Minimal example showing socialBox components
#' @export
minimal_social_box <- function() {
  shinyApp(
    ui = dashboardPage(
      header = dashboardHeader(title = "SocialBox Example"),
      sidebar = dashboardSidebar(),
      body = dashboardBody(
        fluidRow(
          socialBox(
            title = "John Doe",
            subtitle = "CTO",
            "Expert in data visualization and R programming.",
            src = "https://adminlte.io/themes/v3/dist/img/user1-128x128.jpg",
            stats = "90 followers, 30 following",
            width = 6
          ),
          socialBox(
            title = "Jane Smith",
            subtitle = "Marketing Lead",
            "Makes meaningful connections with clients.",
            src = "https://adminlte.io/themes/v3/dist/img/user3-128x128.jpg",
            stats = "150 followers, 45 following",
            width = 6,
            gradient = TRUE,
            background = "primary"
          )
        )
      )
    ),
    server = function(input, output) { }
  )
}

#' @title Minimal UserMessages Example
#' @description Minimal example showing different types of user messages
#' @export
minimal_user_messages <- function() {
  shinyApp(
    ui = dashboardPage(
      header = dashboardHeader(title = "User Messages"),
      sidebar = dashboardSidebar(),
      body = dashboardBody(
        fluidRow(
          box(
            title = "Message Examples",
            width = 12,
            actionButton("info_btn", "Show Info Message"),
            actionButton("success_btn", "Show Success Message"),
            actionButton("warning_btn", "Show Warning Message"),
            actionButton("error_btn", "Show Error Message"),
            actionButton("toast_btn", "Show Toast"),
            actionButton("modal_btn", "Show Modal")
          )
        ),
        fluidRow(
          box(
            title = "Callout Examples",
            width = 12,
            bs4Callout(
              title = "Info Callout",
              "This is an informational callout.",
              status = "info"
            ),
            bs4Callout(
              title = "Danger Callout",
              "This is a danger callout.",
              status = "danger"
            ),
            bs4Callout(
              title = "Success Callout",
              "This is a success callout.",
              status = "success"
            )
          )
        )
      )
    ),
    server = function(input, output, session) {
      observeEvent(input$info_btn, {
        sendSweetAlert(
          session = session,
          title = "Information",
          text = "This is an informational message.",
          type = "info"
        )
      })
      
      observeEvent(input$success_btn, {
        sendSweetAlert(
          session = session,
          title = "Success!",
          text = "The operation completed successfully.",
          type = "success"
        )
      })
      
      observeEvent(input$warning_btn, {
        sendSweetAlert(
          session = session,
          title = "Warning!",
          text = "This action might have consequences.",
          type = "warning"
        )
      })
      
      observeEvent(input$error_btn, {
        sendSweetAlert(
          session = session,
          title = "Error!",
          text = "An error occurred during processing.",
          type = "error"
        )
      })
      
      observeEvent(input$toast_btn, {
        createBS4Toast(
          title = "Notification",
          body = "This is a toast notification.",
          status = "info",
          autohide = TRUE,
          animation = TRUE,
          delay = 2000
        )
      })
      
      observeEvent(input$modal_btn, {
        showModal(
          modal(
            title = "Modal Dialog",
            "This is a modal dialog with custom content.",
            footer = tagList(
              actionButton("close_modal", "Close", class = "btn-default"),
              actionButton("ok_modal", "OK", class = "btn-primary")
            ),
            size = "m",
            easyClose = TRUE
          )
        )
      })
      
      observeEvent(input$close_modal, {
        removeModal()
      })
      
      observeEvent(input$ok_modal, {
        removeModal()
        createBS4Toast(
          title = "Success",
          body = "Modal action confirmed!",
          status = "success"
        )
      })
    }
  )
}