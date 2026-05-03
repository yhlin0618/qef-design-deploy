#' Build App From Configuration
#'
#' This utility function builds a Shiny application based on a YAML configuration.
#' It implements the App Construction Function principle and follows the Component Reuse Rule.
#'
#' @param config_file The filename of the YAML configuration file (without path)
#' @param base_path Base path to the app_configs directory (default: "app_configs")
#'
#' @return A Shiny application object
#' @export
#'
#' @examples
#' # Build an app from configuration
#' app <- buildAppFromConfig("customer_dna_app.yaml")
#' # Then run with: shiny::runApp(app)
buildAppFromConfig <- function(config_file, base_path = "app_configs") {
  # Initialize in APP_MODE if not already done
  if (!exists("INITIALIZATION_COMPLETED") || !INITIALIZATION_COMPLETED) {
    source(file.path("update_scripts", "global_scripts", "00_principles", "sc_initialization_app_mode.R"))
    OPERATION_MODE <- "APP_MODE"
    message("Running in APP_MODE - Production Environment")
  }
  
  # Load configuration
  config <- readYamlConfig(config_file, base_path)
  
  # Extract app title and theme settings
  app_title <- config$title %||% "Shiny Application"
  theme_settings <- config$theme %||% list(version = 5, bootswatch = "default")
  
  # Create get_data_function for components
  get_data <- function(table_name) {
    getDataForTable(table_name)
  }
  
  # Build UI based on components specified in config
  ui <- page_navbar(
    title = app_title,
    theme = do.call(bs_theme, theme_settings),
    navbar_options = navbar_options(bg = "#f8f9fa"),
    
    # Add components based on configuration
    if (!is.null(config$components$micro$customer_profile)) {
      microCustomerUI("customer_module")
    }
    
    # More components would be added here based on config
  )
  
  # Build server function
  server <- function(input, output, session) {
    # Initialize components based on configuration
    if (!is.null(config$components$micro$customer_profile)) {
      microCustomerServer(
        id = "customer_module",
        data_source = config$components$micro$customer_profile
      )
    }
    
    # More components would be initialized here based on config
  }
  
  # Return the Shiny app
  shinyApp(ui, server)
}

# Helper function equivalent to %||% (null-coalescing operator)
`%||%` <- function(a, b) if (is.null(a)) b else a