#' Conditional UI Rendering Function
#' 
#' Creates a conditional renderUI that only renders the component when the target tab is selected.
#' This provides performance optimization by preventing unnecessary component initialization and 
#' offers better user experience with loading messages.
#' 
#' Following principles:
#' - MP88: Immediate Feedback (shows loading states)
#' - MP47: Functional Programming (pure function design)
#' - R67: Functional Encapsulation (self-contained logic)
#'
#' @param current_tab reactive or character. The currently selected tab (usually input$sidebar_menu)
#' @param target_tab character. The tab name that triggers component rendering
#' @param ui_component shiny.tag. The UI component to render when tab is active
#' @param loading_message shiny.tag. Optional custom loading message (defaults to generic message)
#' @param loading_icon character. Font Awesome icon name for loading state (default: "hourglass-half")
#' @return function. A renderUI function ready for assignment to output
#' 
#' @examples
#' # Basic usage
#' output$position_ms_display <- renderUI2(
#'   current_tab = input$sidebar_menu,
#'   target_tab = "positionMS", 
#'   ui_component = position_ms_comp$ui$display
#' )
#' 
#' # With custom loading message
#' output$dna_display <- renderUI2(
#'   current_tab = input$sidebar_menu,
#'   target_tab = "dna",
#'   ui_component = dna_comp$ui$display,
#'   loading_message = div(class = "custom-loading", "Preparing DNA analysis...")
#' )
#' 
#' # With custom icon
#' output$strategy_display <- renderUI2(
#'   current_tab = input$sidebar_menu,
#'   target_tab = "positionStrategy",
#'   ui_component = strategy_comp$ui$display,
#'   loading_icon = "compass"
#' )
#' 
#' @export
renderUI2 <- function(current_tab, target_tab, ui_component, loading_message = NULL, loading_icon = "hourglass-half") {
  
  # Validate inputs
  if (missing(current_tab) || missing(target_tab) || missing(ui_component)) {
    stop("renderUI2: current_tab, target_tab, and ui_component are required parameters")
  }
  
  if (!is.character(target_tab) || length(target_tab) != 1) {
    stop("renderUI2: target_tab must be a single character string")
  }
  
  # Return the renderUI function
  renderUI({
    # Handle reactive current_tab
    current <- if (is.reactive(current_tab)) current_tab() else current_tab
    
    # Check if current tab matches target
    if (identical(current, target_tab)) {
      # Render the actual component
      ui_component
    } else {
      # Render loading/placeholder message
      if (!is.null(loading_message)) {
        loading_message
      } else {
        # Default loading message
        target_display_name <- switch(target_tab,
          "microCustomer" = "Customer Analysis",
          "dna" = "DNA Distribution",
          "position" = "Position Analysis", 
          "positionDNA" = "Position DNA Visualization",
          "positionMS" = "Market Segmentation Analysis",
          "positionKFE" = "Key Factor Analysis",
          "positionIdealRate" = "Ideal Rate Analysis",
          "positionStrategy" = "Strategy Analysis",
          "microMacroKPI" = "Key Performance Indicators",
          paste(target_tab, "Analysis")  # fallback
        )
        
        div(
          style = "text-align: center; padding: 50px; color: #666; background-color: #f8f9fa; border-radius: 8px; margin: 20px;", 
          div(
            style = "margin-bottom: 20px;",
            icon(loading_icon, class = "fa-2x", style = "color: #007bff;")
          ),
          h4(target_display_name, style = "color: #495057; margin-bottom: 15px;"),
          p(
            paste("Click on", gsub("([a-z])([A-Z])", "\\1 \\2", target_tab), "to load analysis..."),
            style = "color: #6c757d; font-size: 14px;"
          ),
          div(
            style = "margin-top: 15px; font-size: 12px; color: #adb5bd;",
            "Component will initialize when selected"
          )
        )
      }
    }
  })
}

#' Create a batch of conditional UI renderers
#' 
#' Helper function to create multiple renderUI2 functions at once for better code organization
#' 
#' @param current_tab_reactive reactive. The reactive current tab value
#' @param components list. Named list where names are target_tabs and values are ui_components
#' @param loading_messages list. Optional named list of custom loading messages
#' @return list. Named list of renderUI functions
#' 
#' @examples
#' # Batch creation
#' ui_renderers <- renderUI2_batch(
#'   current_tab_reactive = reactive(input$sidebar_menu),
#'   components = list(
#'     "positionMS" = position_ms_comp$ui$display,
#'     "positionDNA" = position_dna_comp$ui$display,
#'     "positionStrategy" = position_strategy_comp$ui$display
#'   )
#' )
#' 
#' # Assign to outputs
#' output$position_ms_display <- ui_renderers$positionMS
#' output$position_dna_display <- ui_renderers$positionDNA
#' output$position_strategy_display <- ui_renderers$positionStrategy
#' 
#' @export
renderUI2_batch <- function(current_tab_reactive, components, loading_messages = NULL) {
  
  if (!is.reactive(current_tab_reactive)) {
    stop("renderUI2_batch: current_tab_reactive must be a reactive expression")
  }
  
  if (!is.list(components) || is.null(names(components))) {
    stop("renderUI2_batch: components must be a named list")
  }
  
  # Create renderUI2 functions for each component
  result <- list()
  
  for (target_tab in names(components)) {
    custom_loading <- if (!is.null(loading_messages) && target_tab %in% names(loading_messages)) {
      loading_messages[[target_tab]]
    } else {
      NULL
    }
    
    result[[target_tab]] <- renderUI2(
      current_tab = current_tab_reactive,
      target_tab = target_tab,
      ui_component = components[[target_tab]],
      loading_message = custom_loading
    )
  }
  
  return(result)
}

#' Conditional Server Execution Function
#' 
#' Creates a conditional reactive that only executes the server function when the target tab is selected.
#' This provides performance optimization by preventing unnecessary server-side computations and 
#' resource usage when components are not active.
#' 
#' Following principles:
#' - MP47: Functional Programming (pure function design)
#' - R67: Functional Encapsulation (self-contained logic)
#' - Performance optimization through conditional execution
#'
#' @param current_tab reactive or character. The currently selected tab (usually input$sidebar_menu)
#' @param target_tab character. The tab name that triggers server execution
#' @param server_function function. The server function to execute when tab is active
#' @param always_execute logical. If TRUE, always execute regardless of tab (default: FALSE)
#' @param input shiny input object. Passed to server function
#' @param output shiny output object. Passed to server function  
#' @param session shiny session object. Passed to server function
#' @return reactive. A reactive expression that returns server result or NULL
#' 
#' @examples
#' # Basic usage
#' position_ms_res <- reactive2(
#'   current_tab = input$sidebar_menu,
#'   target_tab = "positionMS",
#'   server_function = position_ms_comp$server,
#'   input = input,
#'   output = output, 
#'   session = session
#' )
#' 
#' # Always execute (for core components)
#' position_res <- reactive2(
#'   current_tab = input$sidebar_menu,
#'   target_tab = "position",
#'   server_function = position_comp$server,
#'   always_execute = TRUE,
#'   input = input,
#'   output = output,
#'   session = session
#' )
#' 
#' @export
reactive2 <- function(current_tab, target_tab, server_function, always_execute = FALSE, 
                      input, output, session) {
  
  # Validate inputs
  if (missing(current_tab) || missing(target_tab) || missing(server_function)) {
    stop("reactive2: current_tab, target_tab, and server_function are required parameters")
  }
  
  if (missing(input) || missing(output) || missing(session)) {
    stop("reactive2: input, output, and session are required parameters")
  }
  
  if (!is.character(target_tab) || length(target_tab) != 1) {
    stop("reactive2: target_tab must be a single character string")
  }
  
  if (!is.function(server_function)) {
    stop("reactive2: server_function must be a function")
  }
  
  # For always_execute components, initialize immediately
  if (always_execute) {
    return(server_function(input, output, session))
  }
  
  # For conditional components, use lazy initialization with observer
  server_result <- reactiveVal(NULL)
  server_initialized <- reactiveVal(FALSE)
  
  # Observer to initialize server when tab becomes active
  observe({
    # Handle reactive current_tab
    current <- if (is.reactive(current_tab)) current_tab() else current_tab
    
    # Initialize server when tab becomes active (only once)
    if (identical(current, target_tab) && !server_initialized()) {
      tryCatch({
        result <- server_function(input, output, session)
        server_result(result)
        server_initialized(TRUE)
      }, error = function(e) {
        warning("reactive2: Error initializing server for tab '", target_tab, "': ", e$message)
        server_result(NULL)
      })
    }
  })
  
  # Return reactive that provides access to server result
  reactive({
    server_result()
  })
}

#' Create a batch of conditional server executors
#' 
#' Helper function to create multiple reactive2 functions at once for better code organization
#' 
#' @param current_tab_reactive reactive. The reactive current tab value
#' @param server_configs list. Named list with server configuration for each tab
#' @param input shiny input object
#' @param output shiny output object  
#' @param session shiny session object
#' @return list. Named list of reactive expressions
#' 
#' @examples
#' # Batch creation with mixed execution modes
#' server_reactives <- reactive2_batch(
#'   current_tab_reactive = reactive(input$sidebar_menu),
#'   server_configs = list(
#'     "position" = list(
#'       server_function = position_comp$server,
#'       always_execute = TRUE  # Core component
#'     ),
#'     "positionMS" = list(
#'       server_function = position_ms_comp$server,
#'       always_execute = FALSE  # Conditional component
#'     ),
#'     "positionStrategy" = list(
#'       server_function = position_strategy_comp$server,
#'       always_execute = FALSE
#'     )
#'   ),
#'   input = input,
#'   output = output,
#'   session = session
#' )
#' 
#' # Access results
#' position_res <- server_reactives$position
#' position_ms_res <- server_reactives$positionMS
#' 
#' @export
reactive2_batch <- function(current_tab_reactive, server_configs, input, output, session) {
  
  if (!is.reactive(current_tab_reactive)) {
    stop("reactive2_batch: current_tab_reactive must be a reactive expression")
  }
  
  if (!is.list(server_configs) || is.null(names(server_configs))) {
    stop("reactive2_batch: server_configs must be a named list")
  }
  
  # Create reactive2 functions for each server
  result <- list()
  
  for (target_tab in names(server_configs)) {
    config <- server_configs[[target_tab]]
    
    # Extract configuration with defaults
    server_function <- config$server_function
    always_execute <- config$always_execute %||% FALSE
    
    if (is.null(server_function)) {
      stop("reactive2_batch: server_function is required for tab '", target_tab, "'")
    }
    
    result[[target_tab]] <- reactive2(
      current_tab = current_tab_reactive,
      target_tab = target_tab,
      server_function = server_function,
      always_execute = always_execute,
      input = input,
      output = output,
      session = session
    )
  }
  
  return(result)
}