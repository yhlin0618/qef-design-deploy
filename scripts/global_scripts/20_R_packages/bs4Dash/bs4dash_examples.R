library(shiny)
library(bs4Dash)

#' @title Minimal bs4Dash Example
#' @description A minimal example of a bs4Dash application with custom theming
#' @author Claude
#' @date 2025-04-14
#' @export
minimal_bs4dash_example <- function() {
  shiny::shinyApp(
    ui = bs4Dash::bs4DashPage(
      freshTheme = bs4Dash::bs4DashTheme(
        bs4Dash::bs4dash_vars(
          navbar_light_color = "#bec5cb",
          navbar_light_active_color = "#FFF",
          navbar_light_hover_color = "#FFF"
        ),
        bs4Dash::bs4dash_yiq(
          contrasted_threshold = 10,
          text_dark = "#FFF",
          text_light = "#272c30"
        ),
        bs4Dash::bs4dash_layout(
          main_bg = "#353c42"
        ),
        bs4Dash::bs4dash_sidebar_light(
          bg = "#272c30",
          color = "#bec5cb",
          hover_color = "#FFF",
          submenu_bg = "#272c30",
          submenu_color = "#FFF",
          submenu_hover_color = "#FFF"
        ),
        bs4Dash::bs4dash_status(
          primary = "#5E81AC", danger = "#BF616A", light = "#272c30"
        ),
        bs4Dash::bs4dash_color(
          gray_900 = "#FFF", white = "#272c30"
        )
      ),
      options = NULL,
      header = bs4Dash::bs4DashHeader(
        title = bs4Dash::bs4DashBrand(
          title = "My dashboard",
          color = "primary",
          href = "https://adminlte.io/themes/v3",
          image = "https://adminlte.io/themes/v3/dist/img/AdminLTELogo.png"
        )
      ),
      sidebar = bs4Dash::bs4DashSidebar(),
      body = bs4Dash::bs4DashBody(
        bs4Dash::bs4Card(status = "danger"),
        bs4Dash::bs4Card(status = "primary"),
        bs4Dash::bs4Card(status = "orange")
      ),
      controlbar = bs4Dash::bs4DashControlbar(),
      title = "DashboardPage"
    ),
    server = function(input, output) { }
  )
}

#' @title bs4Dash Example with Sidebar and Dynamic Content
#' @description An example of bs4Dash with sidebar menu and dynamic content
#' @author Claude
#' @date 2025-04-14
#' @export
bs4dash_with_sidebar_example <- function() {
  shiny::shinyApp(
    ui = bs4Dash::bs4DashPage(
      dark = TRUE,
      header = bs4Dash::bs4DashHeader(
        title = bs4Dash::bs4DashBrand(
          title = "Precision Marketing",
          color = "primary"
        ),
        leftUi = shiny::tagList(
          bs4Dash::bs4DropdownMenu(
            type = "notifications",
            badgeStatus = "warning",
            bs4Dash::bs4Notificationproduct(
              text = "5 new customers today",
              icon = shiny::icon("users"),
              status = "success"
            ),
            bs4Dash::bs4Notificationproduct(
              text = "Sales increased by 12%",
              icon = shiny::icon("chart-line"),
              status = "info"
            )
          )
        )
      ),
      sidebar = bs4Dash::bs4DashSidebar(
        width="300px",
        bs4Dash::bs4SidebarMenu(
          bs4Dash::bs4SidebarMenuproduct(
            text = "Dashboard",
            tabName = "dashboard",
            icon = shiny::icon("dashboard")
          ),
          bs4Dash::bs4SidebarMenuproduct(
            text = "Customer Analysis",
            tabName = "customers",
            icon = shiny::icon("users")
          ),
          bs4Dash::bs4SidebarMenuproduct(
            text = "DNA Analysis",
            tabName = "dna",
            icon = shiny::icon("dna")
          ),
          bs4Dash::bs4SidebarMenuproduct(
            text = "Settings",
            tabName = "settings",
            icon = shiny::icon("cogs")
          )
        )
      ),
      body = bs4Dash::bs4DashBody(
        bs4Dash::bs4Tabproducts(
          bs4Dash::bs4Tabproduct(
            tabName = "dashboard",
            shiny::fluidRow(
              bs4Dash::valueBox(
                value = "150",
                subtitle = "New Customers",
                icon = shiny::icon("users"),
                color = "primary",
                width = 4
              ),
              bs4Dash::valueBox(
                value = "$15,200",
                subtitle = "Revenue",
                icon = shiny::icon("dollar-sign"),
                color = "success",
                width = 4
              ),
              bs4Dash::valueBox(
                value = "24%",
                subtitle = "Conversion Rate",
                icon = shiny::icon("percentage"),
                color = "info",
                width = 4
              )
            ),
            shiny::fluidRow(
              bs4Dash::bs4Card(
                title = "Sales Trend",
                status = "primary",
                width = 6,
                solidHeader = TRUE,
                shiny::plotOutput("salesPlot", height = "250px")
              ),
              bs4Dash::bs4Card(
                title = "Top Products",
                status = "info",
                width = 6,
                solidHeader = TRUE,
                shiny::tableOutput("topProducts")
              )
            )
          ),
          bs4Dash::bs4Tabproduct(
            tabName = "customers",
            bs4Dash::bs4Card(
              title = "Customer Distribution",
              status = "primary",
              solidHeader = TRUE,
              width = 12,
              "Customer analysis content will appear here"
            )
          ),
          bs4Dash::bs4Tabproduct(
            tabName = "dna",
            bs4Dash::bs4Card(
              title = "DNA Analysis",
              status = "warning",
              solidHeader = TRUE,
              width = 12,
              "DNA analysis content will appear here"
            )
          ),
          bs4Dash::bs4Tabproduct(
            tabName = "settings",
            bs4Dash::bs4Card(
              title = "Application Settings",
              status = "danger",
              solidHeader = TRUE,
              width = 12,
              "Settings content will appear here"
            )
          )
        )
      ),
      controlbar = bs4Dash::bs4DashControlbar(
        pinned = TRUE,
        bs4Dash::bs4SkinSelector(),
        bs4Dash::bs4ControlbarMenu(
          id = "controlbarMenu",
          type = "pills",
          bs4Dash::bs4Controlbarproduct(
            title = "Theme",
            icon = shiny::icon("paint-brush"),
            shiny::selectInput(
              inputId = "theme",
              label = "Choose Theme:",
              choices = c("Default", "Blue", "Purple", "Green"),
              selected = "Default"
            )
          ),
          bs4Dash::bs4Controlbarproduct(
            title = "Data",
            icon = shiny::icon("database"),
            shiny::dateRangeInput(
              inputId = "dateRange",
              label = "Select Date Range:",
              start = Sys.Date() - 30,
              end = Sys.Date()
            )
          )
        )
      ),
      title = "Precision Marketing Dashboard"
    ),
    server = function(input, output) {
      output$salesPlot <- shiny::renderPlot({
        # Create a sample plot for the example
        plot(1:10, main = "Sales Trend", xlab = "Month", ylab = "Sales ($)")
      })
      
      output$topProducts <- shiny::renderTable({
        # Create a sample table for the example
        data.frame(
          Product = c("Product A", "Product B", "Product C", "Product D", "Product E"),
          Sales = c("$5,200", "$3,400", "$2,800", "$2,300", "$1,500")
        )
      })
    }
  )
}
