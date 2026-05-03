#' @file customer_dna_production_app.R
#' @principle R102 Shiny Reactive Observation Rule
#' @principle R105 Shiny App Templates Rule
#' @principle R91 Universal Data Access Pattern
#' @principle P76 Error Handling Patterns
#' @principle P77 Performance Optimization
#' 
#' @title Customer DNA Analysis Production Application
#' @description
#' A production-ready Shiny application that displays customer DNA analysis using the microCustomer
#' components and bs4Dash framework. This app is optimized for deployment with no debug features.
#'
#' @required_libraries shiny, bs4Dash, dplyr, shinyjs
#'
#' @features
#' - Uses modular microCustomer components
#' - Universal data accessor for flexible data connections
#' - Error handling with fallback values
#' - Responsive bs4Dash layout
#' - Multi-language support
#' - Production-optimized with no debug elements
#'
#' @usage
#' 1. Ensure the microCustomer.R file is properly sourced
#' 2. Modify the data connection to use your production data source
#' 3. Update translation functions as needed
#' 4. Deploy to your Shiny server

# =======================================================================
# Following MP31 (Initialization First) and R95 (Import Requirements Rule):
# Initialize before doing anything else with centralized package management
# =======================================================================

# Source initialization script first - this will handle:
# 1. Loading all required packages through centralized initialization (R95)
# 2. Loading configuration (translation, marketing channels, and product categories)
# 3. Setting up environment variables

find_and_set_project_root <- function() {
  current_path <- getwd()
  
  # 向上尋找 precision_marketing_app 目錄
  while (!endsWith(current_path, "precision_marketing_app") && 
         !file.exists(file.path(current_path, "precision_marketing_app"))) {
    parent_path <- dirname(current_path)
    if (parent_path == current_path) {
      # 已到達根目錄，無法再向上
      warning("找不到 precision_marketing_app 目錄")
      return(FALSE)
    }
    current_path <- parent_path
  }
  
  # 如果找到的是包含 precision_marketing_app 的目錄，則進入該目錄
  if (file.exists(file.path(current_path, "precision_marketing_app"))) {
    current_path <- file.path(current_path, "precision_marketing_app")
  }
  
  # 設定找到的路徑為工作目錄
  setwd(current_path)
  message("工作目錄已設定為: ", current_path)
  return(TRUE)
}

# 在應用程式開始時呼叫此函數
find_and_set_project_root()

init_script_path <- file.path("update_scripts", "global_scripts", "00_principles", 
                              "sc_initialization_app_mode.R")
source(init_script_path)

# Verify packages were properly initialized
if (!exists("PACKAGES_INITIALIZED") || !is.list(PACKAGES_INITIALIZED)) {
  stop("Package initialization failed. Please check initialization script.")
}

# Define the CSS directory path
css_dir <- file.path("update_scripts", "global_scripts", "19_CSS")

# Set up the CSS directory as Shiny's static directory
options(shiny.staticPath = css_dir)

# CSS styling for the application - following P99: Single-Line UI Elements and P101: Minimal CSS Usage
# Note: We use only necessary CSS files and minimal styling
css_dependencies <- tags$head(
  # Base CSS with minimal styling
  #tags$link(rel = "stylesheet", type = "text/css", href = "minimal.css"),
  
  # Navbar CSS for P99: Single-Line UI Elements
  #tags$link(rel = "stylesheet", type = "text/css", href = "navbar.css"),
  
  # CSS for union component visibility (R19 Component Visibility Guarantee)
  #tags$link(rel = "stylesheet", type = "text/css", href = "union_component_visibility.css"),
  
  # Custom CSS to fix selectInput overflow issues in sidebar without breaking functionality
  tags$style(HTML("
    /* Add horizontal scrolling for selectize inputs in sidebar only when needed */
    # .sidebar .selectize-dropdown {
    #   width: auto !important;
    #   min-width: 100% !important;
    # }
    # 
    # /* Make dropdown content wider than the input if needed */
    # .sidebar .selectize-dropdown-content {
    #   min-width: 100%;
    # }
  ")),
  
  # Other CSS files are kept commented for minimal approach
  #tags$link(rel = "stylesheet", type = "text/css", href = "main.css"),
  #tags$link(rel = "stylesheet", type = "text/css", href = "sidebar-fixes.css"),
  #tags$link(rel = "stylesheet", type = "text/css", href = "component-styles.css")
)



# =======================================================================
# Following MP45 (Automatic Data Availability Detection): Runtime data inspection
# =======================================================================

# Initialize data availability registry and perform detection
app_connection <- connect_to_app_database()

# ======================================================
# TRANSLATION AND LOCALIZATION
# ======================================================
translate <- function(text) {
  return(text)
}

# ======================================================
# UI DEFINITION
# ======================================================
ui <- dashboardPage(
  title = translate("AI Marketing Technology Platform"),
  fullscreen = TRUE,
  dark = FALSE,
  help = FALSE,
  # 
  # # Fresh theme implementation following the bs4dash examples
  # freshTheme = create_theme(
  #   bs4dash_vars(
  #     navbar_light_color = "#bec5cb",
  #     navbar_light_active_color = "#FFF",
  #     navbar_light_hover_color = "#FFF"
  #   ),
  #   bs4dash_yiq(
  #     contrasted_threshold = 10,
  #     text_dark = "#FFF",
  #     text_light = "#272c30"
  #   ),
  #   bs4dash_layout(
  #     main_bg = "#f4f6f9"
  #   ),
  #   bs4dash_sidebar_light(
  #     bg = "#272c30",
  #     color = "#bec5cb",
  #     hover_color = "#FFF",
  #     submenu_bg = "#272c30",
  #     submenu_color = "#FFF",
  #     submenu_hover_color = "#FFF"
  #   ),
  #   bs4dash_status(
  #     primary = "#5E81AC", 
  #     danger = "#BF616A", 
  #     warning = "#EBCB8B",
  #     success = "#A3BE8C",
  #     info = "#88C0D0",
  #     light = "#272c30"
  #   ),
  #   bs4dash_color(
  #     gray_900 = "#FFF", 
  #     white = "#272c30"
  #   )
  # ),
  
  # bs4Dash standard header with navbar navigation - following R90: bs4Dash Structure Adherence Rule
  # and implementing R92: bs4Dash Direct Navigation Rule with navbarMenu and navbarTab
  header = dashboardHeader(
    title = translate("AI Marketing Platform"),
    # title = dashboardBrand(
    #   title = translate("AI Marketing Platform"),
    #   color = "primary"
    # ), ## it looks great but have bug, needs to be fixed
    skin = "light",
    status = "primary",
    fixed = FALSE,
    
    # Notification in the top right
    rightUi = tagList(
      dropdownMenu(
        type = "notifications",
        badgeStatus = "success",
        notificationproduct(
          text = "Welcome to Precision Marketing",
          icon = icon("info"),
          status = "primary"
        )
      )
    )
  ),
  
  sidebar = dashboardSidebar(
    #skin = "dark",
    status = "primary",
    elevation = 3,
    minified = FALSE,
    collapsed = FALSE,
    width = "300px",
    sidebarMenu(
      id = "sidebar_menu",
       sidebarHeader("Application Settings"),
      # menuproduct(
      #   text = "Dashboard",
      #   tabName = "dashboard",
      #   icon = icon("tachometer-alt")
      # ),
      menuproduct(
        text = "Customer DNA",
        tabName = "microCustomer",
        icon = icon("users")
      )
      # menuproduct(
      #   text = "DNA Analysis",
      #   tabName = "dna",
      #   icon = icon("dna")
      # )
      # menuproduct(
      #   text = "Settings",
      #   tabName = "settings",
      #   icon = icon("cogs")
      # )
    ),
    # Put the filter UI directly in the sidebar
    uiOutput("conditionalCustomerFilter")
  ),
  
  body = dashboardBody(
    css_dependencies, # Include the CSS dependencies
    useShinyjs(), # Enable shinyjs for UI interactions
    
    tabproducts(
      # Dashboard tab
      tabproduct(
        tabName = "dashboard",
        fluidRow(
          valueBox(
            value = uiOutput("total_customers"),
            subtitle = "Total Customers",
            icon = icon("users"),
            color = "primary",
            width = 4
          ),
          valueBox(
            value = uiOutput("main_force"),
            subtitle = "Main Force Customers",
            icon = icon("star"),
            color = "success",
            width = 4
          ),
          valueBox(
            value = uiOutput("inactive"),
            subtitle = "Inactive Customers",
            icon = icon("user-clock"),
            color = "warning",
            width = 4
          )
        ),
        fluidRow(
          box(
            title = "Customer Overview",
            status = "primary",
            solidHeader = TRUE,
            width = 12,
            "Dashboard overview content will be displayed here."
          )
        )
      ),
      
      # Customer Analysis tab
      tabproduct(
        tabName = "microCustomer",
        fluidRow(
          column(
            width = 12,
            box(
              title = "Customer DNA Analysis",
              status = "primary",
              solidHeader = TRUE,
              width = NULL,
              elevation = 3,
              uiOutput("customer_display_ui")
            )
          )
        )
      ),
      
      # DNA Analysis tab
      tabproduct(
        tabName = "dna",
        fluidRow(
          column(
            width = 12,
            box(
              title = "DNA Analysis Details",
              status = "info",
              solidHeader = TRUE,
              width = 12,
              "Detailed DNA analysis will be displayed here."
            )
          )
        )
      ),
      
      # Settings tab
      tabproduct(
        tabName = "settings",
        box(
          title = "Application Settings",
          status = "primary",
          solidHeader = TRUE,
          width = 12,
          elevation = 3,
          fluidRow(
            column(
              width = 6,
              selectInput(
                "language",
                "Language",
                choices = c("English", "中文"),
                selected = "English"
              )
            ),
            column(
              width = 6,
              selectInput(
                "theme",
                "Theme",
                choices = c("Light", "Dark"),
                selected = "Light"
              )
            )
          ),
          hr(),
          actionButton(
            "save_settings",
            "Save Settings",
            icon = icon("save"),
            class = "btn-primary"
          )
        )
      )
    )
  ),
  
  footer = dashboardFooter(
    fixed = TRUE,
    right = "Version 1.0.0 | 2025",
    left = span(
      "Powered by", 
      "Precision Marketing Team"
    )
  ),
  
  controlbar = dashboardControlbar(
    skin = "dark",
    pinned = FALSE,
    overlay = TRUE,
    
    # Controlbar content with tabs
    controlbarMenu(
      id = "controlbarMenu",
      type = "pills",
      controlbarproduct(
        title = "Info",
        icon = icon("info-circle"),
        bs4Card(
          title = "About DNA Analysis",
          "This application provides customer DNA analysis based on RFM (Recency, Frequency, Monetary) metrics and additional customer behavior indicators.",
          width = 12
        )
      ),
      controlbarproduct(
        title = "Settings",
        icon = icon("sliders-h"),
        selectInput(
          "controlbar_theme",
          "App Theme:",
          choices = c("Default", "Blue", "Purple", "Green"),
          selected = "Default"
        ),
        switchInput(
          "show_debug",
          "Show Debug Info",
          value = FALSE
        )
      )
    ),
    
    # Footer product for help
    footer = actionButton(
      "help", 
      "Get Help", 
      icon = icon("question-circle"),
      width = "100%",
      class = "btn-info"
    )
  )
)

# ======================================================
# SERVER LOGIC - 優化渲染部分
# ======================================================
server <- function(input, output, session) {

  
  # Initialize the microCustomer component
  customer_component <- microCustomerComponent(
    id = "customer_analysis", 
    app_data_connection = app_connection,
    translate = translate
  )
  
  # Generate Customer Filter based on sidebar menu selection
  output$conditionalCustomerFilter <- renderUI({
    if (!is.null(input$sidebar_menu) && input$sidebar_menu == "microCustomer") {
      div(
        class = "sidebar-filter-container p-3 mt-2",
        h5("Customer Filter", class = "text-bold mb-3"),
        # Use the filter UI from the customer component
        customer_component$ui$filter
      )
    } else if (!is.null(input$sidebar_menu) && input$sidebar_menu == "dna") {
      div(
        class = "sidebar-filter-container p-3 mt-2",
        h5("DNA Analysis Filter", class = "text-bold mb-3"),
        # DNA-specific filtering options
        selectInput(
          "dna_segment", 
          "Customer Segment",
          choices = c("All Segments", "Main Force", "Potential", "Inactive"),
          selected = "All Segments"
        ),
        sliderInput(
          "dna_threshold",
          "Analysis Threshold",
          min = 0,
          max = 1,
          value = 0.5,
          step = 0.1
        )
      )
    }
  })
  
  # Render the customer display UI when the customer tab is selected
  output$customer_display_ui <- renderUI({
    if (!is.null(input$sidebar_menu) && input$sidebar_menu == "microCustomer") {
      customer_component$ui$display
    }
  })
  
  # Initialize the customer component server logic
  filtered_customer_data <- customer_component$server(input, output, session)
  
  # Process filtered data only when the customer tab is selected
  observe({
    req(input$sidebar_menu)
    if (input$sidebar_menu == "microCustomer") {
      filtered_customer_data()
    }
  })
  
  # Value box outputs
  output$total_customers <- renderUI({
    # Check if app_connection is a list and has the customer_profile element
    if (!is.list(app_connection) || is.null(app_connection[["customer_profile"]])) {
      return(h3("0", class="text-bold m-0"))
    }
    
    # Try to get count safely
    tryCatch({
      count <- nrow(app_connection[["customer_profile"]])
      return(h3(format(count, big.mark=","), class="text-bold m-0"))
    }, error = function(e) {
      return(h3("0", class="text-bold m-0"))
    })
  })
  
  output$main_force <- renderUI({
    # Safely check for data availability
    if (!is.list(app_connection) || is.null(app_connection[["dna_by_customer"]])) {
      return(tagList(
        h3("0", class="text-bold m-0"),
        p("0%", class="m-0 small")
      ))
    }
    
    # Try to calculate counts safely
    tryCatch({
      dna_data <- app_connection[["dna_by_customer"]]
      if (is.data.frame(dna_data)) {
        count <- sum(dna_data$nes_status == "主力型", na.rm = TRUE)
        percentage <- round(count/nrow(dna_data) * 100, 1)
        return(tagList(
          h3(format(count, big.mark=","), class="text-bold m-0"),
          p(paste0(percentage, "%"), class="m-0 small")
        ))
      } else {
        return(tagList(
          h3("0", class="text-bold m-0"),
          p("0%", class="m-0 small")
        ))
      }
    }, error = function(e) {
      return(tagList(
        h3("0", class="text-bold m-0"),
        p("0%", class="m-0 small")
      ))
    })
  })
  
  output$inactive <- renderUI({
    # Safely check for data availability
    if (!is.list(app_connection) || is.null(app_connection[["dna_by_customer"]])) {
      return(tagList(
        h3("0", class="text-bold m-0"),
        p("0%", class="m-0 small")
      ))
    }
    
    # Try to calculate counts safely
    tryCatch({
      dna_data <- app_connection[["dna_by_customer"]]
      if (is.data.frame(dna_data)) {
        count <- sum(dna_data$cai_label == "不活躍", na.rm = TRUE)
        percentage <- round(count/nrow(dna_data) * 100, 1)
        return(tagList(
          h3(format(count, big.mark=","), class="text-bold m-0"),
          p(paste0(percentage, "%"), class="m-0 small")
        ))
      } else {
        return(tagList(
          h3("0", class="text-bold m-0"),
          p("0%", class="m-0 small")
        ))
      }
    }, error = function(e) {
      return(tagList(
        h3("0", class="text-bold m-0"),
        p("0%", class="m-0 small")
      ))
    })
  })
  
  # Handle settings changes
  observeEvent(input$save_settings, {
    showNotification("Settings saved successfully", type = "success")
    
    # Apply theme changes
    theme <- input$theme
    if (theme == "Dark") {
      session$sendCustomMessage("bs4Dash-theme", list(theme = "dark"))
    } else {
      session$sendCustomMessage("bs4Dash-theme", list(theme = "light"))
    }
  })
  
  # Handle controlbar theme changes
  observeEvent(input$controlbar_theme, {
    selected_theme <- input$controlbar_theme
    theme_colors <- list(
      "Default" = list(primary = "#5E81AC", info = "#88C0D0"),
      "Blue" = list(primary = "#0073b7", info = "#3c8dbc"),
      "Purple" = list(primary = "#6f42c1", info = "#9966CC"),
      "Green" = list(primary = "#28a745", info = "#20c997")
    )
    
    # Apply the selected theme colors
    session$sendCustomMessage(
      "bs4Dash-theme-update", 
      theme_colors[[selected_theme]]
    )
  })
  
  # Handle help button
  observeEvent(input$help, {
    showModal(modalDialog(
      title = "Help Center",
      div(
        class = "p-3",
        h4("Customer DNA Analysis Application", class = "mb-3"),
        p("This application provides customer DNA analysis based on RFM (Recency, Frequency, Monetary) metrics and additional customer behavior indicators."),
        hr(),
        h5("Key Features:"),
        tags$ul(
          tags$li("Customer DNA Profiling"),
          tags$li("Segmentation Analysis"),
          tags$li("Behavior Pattern Detection"),
          tags$li("Trend Analysis and Forecasting")
        ),
        hr(),
        p("For assistance, please contact:"),
        p(icon("envelope"), " support@precisionmarketing.example.com", class = "text-primary"),
        p(icon("phone"), " +1-800-PRECISION", class = "text-primary")
      ),
      size = "lg",
      easyClose = TRUE,
      footer = modalButton("Close")
    ))
  })
  
  # Handle application startup
  observe({
    # Check and report data availability when the app starts
    tryCatch({
      if (is.list(app_connection)) {
        data_status <- list(
          customer_profile = !is.null(app_connection[["customer_profile"]]),
          dna_by_customer = !is.null(app_connection[["dna_by_customer"]])
        )
        
        # Log data availability
        if (all(unlist(data_status))) {
          message("All required data sources are available")
        } else {
          missing_sources <- names(data_status)[!unlist(data_status)]
          warning("Some data sources are not available: ", paste(missing_sources, collapse = ", "))
        }
      } else {
        warning("app_connection is not a list. Data sources cannot be checked.")
      }
    }, error = function(e) {
      warning("Error checking data availability: ", e$message)
    })
  })
}

shinyApp(ui, server)