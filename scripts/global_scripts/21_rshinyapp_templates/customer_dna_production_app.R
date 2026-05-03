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
  
  # 檢查找到的是否是包含 precision_marketing_app 的目錄
  # 只有在當前目錄名不是 "precision_marketing_app" 但包含該子目錄時才進入子目錄
  if (!endsWith(current_path, "precision_marketing_app") && 
      file.exists(file.path(current_path, "precision_marketing_app"))) {
    target_path <- file.path(current_path, "precision_marketing_app")
    # 先確認目錄存在再設置
    if (dir.exists(target_path)) {
      current_path <- target_path
    } else {
      warning("找到的 precision_marketing_app 目錄似乎不是有效目錄")
    }
  }
  
  # 設定找到的路徑為工作目錄
  if (dir.exists(current_path)) {
    setwd(current_path)
    message("工作目錄已設定為: ", current_path)
    return(TRUE)
  } else {
    warning("無法設置工作目錄，路徑不存在: ", current_path)
    return(FALSE)
  }
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
translate <<- function(text) {
  return(text)
}

# ======================================================
# UI DEFINITION
# ======================================================
shinyjs::useShinyjs()
ui <- bs4Dash::bs4DashPage(
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
  header = bs4Dash::bs4DashNavbar(
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
      bs4Dash::bs4DropdownMenu(
        type = "notifications",
        badgeStatus = "success",
        bs4Dash::notificationproduct(
          text = "Welcome to Precision Marketing",
          icon = shiny::icon("info"),
          status = "primary"
        )
      )
    )
  ),
  
  sidebar = bs4Dash::bs4DashSidebar(
    #skin = "dark",
    status = "primary",
    elevation = 3,
    minified = FALSE,
    collapsed = FALSE,
    width = "300px",
    bs4Dash::sidebarMenu(
      id = "sidebar_menu",
      bs4Dash::bs4SidebarHeader("Application Settings"),
      # menuproduct(
      #   text = "Dashboard",
      #   tabName = "dashboard",
      #   icon = icon("tachometer-alt")
      # ),
      bs4Dash::bs4SidebarMenuproduct(
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
    uiOutput("CommonFilter"),
    uiOutput("conditionalCustomerFilter")
  ),
  
  body = bs4Dash::bs4DashBody(
    css_dependencies, # Include the CSS dependencies
    shinyjs::useShinyjs(), # Enable shinyjs for UI interactions
    
    bs4Dash::bs4Tabproducts(
      # Dashboard tab
      bs4Dash::bs4Tabproduct(
        tabName = "dashboard",
        shiny::fluidRow(
          bs4Dash::valueBox(
            value = uiOutput("total_customers"),
            subtitle = "Total Customers",
            icon = shiny::icon("users"),
            color = "primary",
            width = 4
          ),
          bs4Dash::valueBox(
            value = uiOutput("main_force"),
            subtitle = "Main Force Customers",
            icon = shiny::icon("star"),
            color = "success",
            width = 4
          ),
          bs4Dash::valueBox(
            value = uiOutput("inactive"),
            subtitle = "Inactive Customers",
            icon = shiny::icon("user-clock"),
            color = "warning",
            width = 4
          )
        ),
        shiny::fluidRow(
          bs4Dash::bs4Card(
            title = "Customer Overview",
            status = "primary",
            solidHeader = TRUE,
            width = 12,
            "Dashboard overview content will be displayed here."
          )
        )
      ),
      
      # Customer Analysis tab
      bs4Dash::bs4Tabproduct(
        tabName = "microCustomer",
        shiny::fluidRow(
          shiny::column(
            width = 12,
            bs4Dash::bs4Card(
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
      bs4Dash::bs4Tabproduct(
        tabName = "dna",
        shiny::fluidRow(
          shiny::column(
            width = 12,
            bs4Dash::bs4Card(
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
      bs4Dash::bs4Tabproduct(
        tabName = "settings",
        bs4Dash::bs4Card(
          title = "Application Settings",
          status = "primary",
          solidHeader = TRUE,
          width = 12,
          elevation = 3,
          shiny::fluidRow(
            shiny::column(
              width = 6,
              shiny::selectInput(
                "language",
                "Language",
                choices = c("English", "中文"),
                selected = "English"
              )
            ),
            shiny::column(
              width = 6,
              shiny::selectInput(
                "theme",
                "Theme",
                choices = c("Light", "Dark"),
                selected = "Light"
              )
            )
          ),
          shiny::hr(),
          shiny::actionButton(
            "save_settings",
            "Save Settings",
            icon = shiny::icon("save"),
            class = "btn-primary"
          )
        )
      )
    )
  ),
  
  footer = bs4Dash::dashboardFooter(
    fixed = TRUE,
    right = "Version 1.0.0 | 2025",
    left = shiny::span(
      "Powered by", 
      "Precision Marketing Team"
    )
  ),
  
  controlbar = bs4Dash::bs4DashControlbar(
    skin = "light",
    pinned = FALSE,
    overlay = TRUE,
    
    # Controlbar content with tabs
    bs4Dash::controlbarMenu(
      id = "controlbarMenu",
      type = "pills",
      bs4Dash::controlbarproduct(
        title = "Info",
        icon = shiny::icon("info-circle"),
        bs4Dash::bs4Card(
          title = "About DNA Analysis",
          "This application provides customer DNA analysis based on RFM (Recency, Frequency, Monetary) metrics and additional customer behavior indicators.",
          width = 12
        )
      ),
      bs4Dash::controlbarproduct(
        title = "Settings",
        icon = shiny::icon("sliders-h"),
        shiny::selectInput(
          "controlbar_theme",
          "App Theme:",
          choices = c("Default", "Blue", "Purple", "Green"),
          selected = "Default"
        ),
        shinyWidgets::switchInput(
          "show_debug",
          "Show Debug Info",
          value = FALSE
        )
      )
    ),
    
    # Footer product for help
    footer = shiny::actionButton(
      "help", 
      "Get Help", 
      icon = shiny::icon("question-circle"),
      width = "100%",
      class = "btn-info"
    )
  )
)

# ======================================================
# SERVER LOGIC - 優化渲染部分
# ======================================================
server <- function(input, output, session) {
  # Initialize a reactive value for debug mode
  debug_mode <- reactiveVal(FALSE)
  
  # Update debug_mode when show_debug changes
  observeEvent(input$show_debug, {
    debug_mode(input$show_debug)
    # When debug mode is toggled, show a notification
    if (input$show_debug) {
      showNotification(
        "Debug mode enabled - detailed information will be shown",
        type = "message",
        duration = 3
      )
    } else {
      showNotification(
        "Debug mode disabled - returning to normal display",
        type = "message",
        duration = 3
      )
    }
  }, ignoreInit = FALSE) # Catch the initial value too

  
  # Initialize the microCustomer component with reactive configuration
  customer_component <- reactive({
    # Add debug logging to track when this reactive runs (only when debug mode is enabled)
    if (debug_mode()) {
      cat("DEBUG TRACE: customer_component reactive is being run\n")
    }
    
    # Create the component with the current configuration
    # Note: We pass the reactive function itself, not its value
    result <- microCustomerComponent(
      id = "customer_analysis", 
      app_data_connection = app_connection,
      config = component_config,  # Pass the reactive expression itself
      translate = translate
    )
    
    cat("DEBUG TRACE: microCustomerComponent created\n")
    return(result)
  })
  
  # In the main app
  output$CommonFilter <- renderUI({
    div(
      class = "sidebar-filter-container p-3 mt-2",
      h5("Marketing Platform", class = "text-bold mb-3"),
      radioButtons(
        inputId = "platform",
        label = NULL,
        # Using platform IDs (three-letter codes) as the actual values sent to the server
        choices = list("eBay" = "eby", "Cyberbiz" = "cbz", "All Platforms" = "all"),
        selected = "eby",  # Make sure this matches your initial default
        inline = FALSE
      )
    )
  })
  
  # Add debug logging and handling for platform selection
  observeEvent(input$platform, {
    platform_id <- as.character(input$platform)
    if (debug_mode()) {
      cat("DEBUG: Platform selection changed to:", platform_id, "\n")
    }
    
    # Notify user about platform change
    platform_label <- switch(platform_id,
                         "eby" = "eBay",
                         "cbz" = "Cyberbiz",
                         "all" = "All Platforms",
                         paste("Platform", platform_id))
    
    showNotification(
      paste("Switched to", platform_label, "platform data"),
      type = "message",
      duration = 3
    )
  }, ignoreInit = FALSE)  # Log even at initialization
  
  # Create a reactive configuration object that includes the platform filter
  component_config <- reactive({
    # Get the platform ID selected by the user
    platform_id <- as.character(input$platform)
    
    # Create a configuration object with filter settings
    config <- list(
      filters = list(
        platform_id = platform_id
      )
    )
    
    # Add debug logging to track when this reactive runs (only in debug mode)
    if (debug_mode()) {
      cat("DEBUG TRACE: component_config reactive updating with platform_id =", platform_id, "\n")
    }
    
    return(config)
  })
  
  # Generate Customer Filter based on sidebar menu selection
  output$conditionalCustomerFilter <- shiny::renderUI({
    if (!is.null(input$sidebar_menu) && input$sidebar_menu == "microCustomer") {
      # Get the current component with current config
      current_component <- customer_component()
      
      shiny::div(
        class = "sidebar-filter-container p-3 mt-2",
        shiny::h5("Customer Filter", class = "text-bold mb-3"),
        # Use the filter UI from the current customer component
        current_component$ui$filter
      )
    } else if (!is.null(input$sidebar_menu) && input$sidebar_menu == "dna") {
      shiny::div(
        class = "sidebar-filter-container p-3 mt-2",
        shiny::h5("DNA Analysis Filter", class = "text-bold mb-3"),
        # DNA-specific filtering options
        shiny::selectInput(
          "dna_segment", 
          "Customer Segment",
          choices = c("All Segments", "Main Force", "Potential", "Inactive"),
          selected = "All Segments"
        ),
        shiny::sliderInput(
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
  output$customer_display_ui <- shiny::renderUI({
    # Ensure we have both required inputs
    shiny::req(input$sidebar_menu, input$platform)
    
    if (input$sidebar_menu == "microCustomer") {
      # Get the current component with current config
      current_component <- customer_component()
      
      # Create a nice container for the display
      div(
        class = "customer-display-container",
        # Display the customer component UI
        current_component$ui$display,
        # Add a notice about platform filtering - only when debug info is enabled
        if (debug_mode() && !is.null(input$platform) && nzchar(input$platform) && input$platform != "all") {
          shiny::div(
            class = "platform-filter-notice",
            style = "margin-top: 20px; padding: 10px; background-color: #f8f9fa; border-left: 4px solid #5E81AC; border-radius: 0 4px 4px 0;",
            tags$h5(paste0("Filtering by Platform ID: ", input$platform),
                   style = "margin: 0; color: #5E81AC;"),
            tags$p("Change this filter in the sidebar to view different platform data.", 
                  style = "margin: 0; padding-top: 5px; color: #666;")
          )
        }
      )
    }
  })
  
  # Create a reactive expression for the server logic
  filtered_customer_data <- shiny::reactive({
    # Make sure we have platform selection
    shiny::req(input$platform)
    
    # Get the current component instance with latest configuration
    current_component <- customer_component()
    
    # Initialize the server and return the result - explicitly passing config
    result <- current_component$server(input, output, session)
    
    # For debugging - log platform filter (only in debug mode)
    platform_id <- as.character(input$platform)
    if (debug_mode()) {
      if (!is.null(platform_id) && platform_id != "all") {
        message("Server using platform filter code: ", platform_id)
      } else {
        message("Server using all platforms data")
      }
    }
    
    return(result)
  })
  
  # Process filtered data only when the customer tab is selected
  shiny::observe({
    shiny::req(input$sidebar_menu, input$platform)
    if (input$sidebar_menu == "microCustomer") {
      # Get the selected platform value (code)
      selected_platform <- as.character(input$platform)
      
      # Log the platform filtering (only in debug mode)
      if (!is.null(selected_platform) && selected_platform != "all") {
        if (debug_mode()) {
          message("Using platform filter code: ", selected_platform)
        }
        
        # Show notification about platform change
        platform_label <- switch(selected_platform,
                               "eby" = "eBay",
                               "cbz" = "Cyberbiz", 
                               "all" = "All Platforms",
                               paste("Platform", selected_platform))
        showNotification(
          paste("Filtering data for platform:", platform_label),
          type = "message",
          duration = 3
        )
      } else {
        if (debug_mode()) {
          message("Using all platforms data")
        }
        
        # Show notification about using all platforms
        showNotification(
          "Showing data from all platforms",
          type = "message",
          duration = 3
        )
      }
      
      # Process the filtered data from the component
      filtered_customer_data()
    }
  })
  
  # Value box outputs
  output$total_customers <- shiny::renderUI({
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
  
  output$main_force <- shiny::renderUI({
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
  
  output$inactive <- shiny::renderUI({
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
  shiny::observeEvent(input$save_settings, {
    shiny::showNotification("Settings saved successfully", type = "success")
    
    # Apply theme changes
    theme <- input$theme
    if (theme == "Dark") {
      session$sendCustomMessage("bs4Dash-theme", list(theme = "dark"))
    } else {
      session$sendCustomMessage("bs4Dash-theme", list(theme = "light"))
    }
  })
  
  # Handle controlbar theme changes
  shiny::observeEvent(input$controlbar_theme, {
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
  shiny::observeEvent(input$help, {
    shiny::showModal(shiny::modalDialog(
      title = "Help Center",
      shiny::div(
        class = "p-3",
        shiny::h4("Customer DNA Analysis Application", class = "mb-3"),
        shiny::p("This application provides customer DNA analysis based on RFM (Recency, Frequency, Monetary) metrics and additional customer behavior indicators."),
        shiny::hr(),
        shiny::h5("Key Features:"),
        shiny::tags$ul(
          shiny::tags$li("Customer DNA Profiling"),
          shiny::tags$li("Segmentation Analysis"),
          shiny::tags$li("Behavior Pattern Detection"),
          shiny::tags$li("Trend Analysis and Forecasting")
        ),
        shiny::hr(),
        shiny::p("For assistance, please contact:"),
        shiny::p(shiny::icon("envelope"), " support@precisionmarketing.example.com", class = "text-primary"),
        shiny::p(shiny::icon("phone"), " +1-800-PRECISION", class = "text-primary")
      ),
      size = "lg",
      easyClose = TRUE,
      footer = shiny::modalButton("Close")
    ))
  })
  
  # Handle application startup
  shiny::observe({
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
