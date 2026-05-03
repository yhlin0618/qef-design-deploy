# =============================================================
# positionDNAPlotly_production_test.R
# 2025‑05‑22 – Created DNA Plotly component test following MP56 principle
# =============================================================

# IMPORTANT CODE MAINTENANCE NOTES:
# 1. This component follows MP56 (Connected Component Principle)
# 2. Uses R116 (Enhanced Data Access) with tbl2 pattern
# 3. Implements MP73 (Interactive Visualization Preference) with Plotly
# 4. Follows MP88 (Immediate Feedback) for real-time filtering
# =============================================================

# ---- 0. 初始化 ------------------------------------------------------------
find_and_set_project_root <- function(){
  p <- getwd();
  while(!endsWith(p,"precision_marketing_app") && !file.exists(file.path(p,"precision_marketing_app"))){
    nxt <- dirname(p); if(nxt==p){ warning("找不到 precision_marketing_app 目錄"); return(FALSE) }; p <- nxt }
  if(!endsWith(p,"precision_marketing_app")) p <- file.path(p,"precision_marketing_app")
  if(dir.exists(p)){ setwd(p); message("工作目錄已設定為: ", p); TRUE } else FALSE }
find_and_set_project_root()
autoinit()
stopifnot(exists("PACKAGES_INITIALIZED"), is.list(PACKAGES_INITIALIZED))

# ---- 1. 套件 --------------------------------------------------------------
library(shiny); library(bs4Dash); library(plotly); library(dplyr); library(shinyjs); library(DBI); library(tidyr)

# ---- 2. 連線 --------------------------------------------------------------
# Connect to the app database using the Enhanced Data Access approach (R116)
connect_to_app_database <- function(db_path = "app_data/app_data.duckdb") {
  tryCatch({
    if (!requireNamespace("DBI", quietly = TRUE)) {
      message("Installing DBI package...")
      install.packages("DBI")
    }
    
    if (!requireNamespace("duckdb", quietly = TRUE)) {
      message("Installing duckdb package...")
      install.packages("duckdb")
    }
    
    # Check if file exists
    if (!file.exists(db_path)) {
      stop("Database file does not exist at path: ", db_path)
    }
    
    # Close any existing connections
    if (exists("conn")) {
      DBI::dbDisconnect(conn, shutdown = TRUE)
    }
    
    # Create DuckDB driver with read-only access
    drv_ro <- duckdb::duckdb(
      dbdir     = db_path,
      read_only = TRUE
    )
    
    # Create connection
    conn_ro <- DBI::dbConnect(drv_ro)
    
    return(conn_ro)
  }, error = function(e) {
    message("Error connecting to database: ", e$message)
    return(NULL)
  })
}

app_connection <- connect_to_app_database()

# ---- 3. Component 載入 ----------------------------------------------------
# Load the positionDNAPlotly component
source(file.path("update_scripts", "global_scripts", "10_rshinyapp_components", "position", "positionDNAPlotly", "positionDNAPlotly.R"))

# Translation function
translate <- function(x) x

# ---- 4. CSS ---------------------------------------------------------------
css_deps <- tags$head(tags$style(HTML(
  ".sidebar .selectize-dropdown{width:auto!important;min-width:100%!important}.sidebar .selectize-dropdown-content{min-width:100%}"
)))

# ---- 5. UI ---------------------------------------------------------------
ui <- bs4DashPage(
  title = translate("DNA Visualization Test"), fullscreen = TRUE,
  header = bs4DashNavbar(title = translate("DNA Visualization Test"), skin="light", status="primary"),
  sidebar = bs4DashSidebar(status="primary", width="300px", elevation = 3, minified = FALSE,
                           sidebarMenu(id="sidebar_menu",
                                       bs4SidebarHeader("Application Settings"),
                                       bs4SidebarMenuItem("DNA Visualization", tabName="dna_plot", icon=icon("chart-line")) ),
                           # Shared Platform and Product Line Filters
                           div(class="sidebar-section p-3 mt-2",
                               h5("Platform", class="mb-3"),
                               radioButtons(
                                 inputId = "platform",
                                 label = NULL,
                                 choices = df_platform %>%
                                   filter(platform_id %in% c("all", names(app_configs$platforms))) %>%
                                   { setNames(.$platform_id, .$platform_name_english) },
                                 selected = "all"
                               ),
                               h5("Product line", class="mb-3"),
                               radioButtons(
                                 inputId = "product_line",
                                 label = NULL,
                                 choices = rbind(
                                   df_product_line[df_product_line$product_line_id == "all", , drop = FALSE],
                                   get_active_product_lines()
                                 ) %>% { setNames(.$product_line_id, .$product_line_name_english) },
                                 selected = "all"
                               )),
                           # Dynamic filter container
                           uiOutput("dynamic_filter") ),
  
  body = bs4DashBody(css_deps, useShinyjs(),
                     bs4TabItems(
                       bs4TabItem(tabName="dna_plot", 
                                 fluidRow(
                                   column(12, 
                                          bs4Card(title="Brand DNA Visualization", 
                                                 status="primary", 
                                                 width=12, 
                                                 solidHeader=TRUE, 
                                                 elevation=3, 
                                                 uiOutput("dna_plot_display"))))) ) ),
  
  footer = dashboardFooter(fixed=TRUE, right="Version 1.0.0 | 2025") )

# ---- 6. Server -----------------------------------------------------------
server <- function(input, output, session){
  # ---- 6.1 Shared Configuration ----------------------------------------------
  comp_config <- reactive({
    list(
      filters    = list(platform_id = input$platform,
                        product_line_id_chosen = input$product_line),
      active_tab = input$sidebar_menu
    )
  })
  
  # ---- 6.2 Component Instance -----------------------------------------
  dna_comp <- positionDNAPlotlyComponent("dna_plot", app_connection, comp_config, translate)
  
  # ---- 6.3 Dynamic Filter Injection --------------------------------------------
  output$dynamic_filter <- renderUI({ 
    switch(input$sidebar_menu,
           "dna_plot" = dna_comp$ui$filter,
           NULL) 
  })
  
  # ---- 6.4 Component UI -------------------------------------------------
  output$dna_plot_display <- renderUI(dna_comp$ui$display)
  
  # ---- 6.5 Start Component Server ---------------------------------------
  dna_res <- dna_comp$server(input, output, session)
  
  # ---- 6.6 Platform Notifications ---------------------------------------------
  observeEvent(input$platform, {
    # Map platform ID to platform name
    platform_map <- c(
      "all" = "All Platforms",
      "amz" = "Amazon",
      "eby" = "eBay"
    )
    
    # Get the platform name using the platform ID as key
    platform_id <- as.character(input$platform)
    platform_name <- platform_map[platform_id]
    
    # Show notification with the platform name
    showNotification(paste("Switched to", platform_name), type="message", duration=3)
  }, ignoreInit = TRUE)
  
  # ---- 6.7 Product Line Notifications --------------------------------------
  observeEvent(input$product_line, {
    if (input$product_line != "all") {
      showNotification("DNA visualization updated for selected product line", 
                      type="message", duration=3)
    }
  }, ignoreInit = TRUE)
  
  # ---- 6.8 Session Cleanup -------------------------------------------------
  session$onSessionEnded(function(){
    if(!is.null(app_connection) && inherits(app_connection, "DBIConnection")) {
      try(DBI::dbDisconnect(app_connection), silent=TRUE)
    }
  })
}

# ---- 7. Run --------------------------------------------------------------
shinyApp(ui, server)
