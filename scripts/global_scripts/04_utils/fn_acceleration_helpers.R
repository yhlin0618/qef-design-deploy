#LOCK ON
# Acceleration Helper Functions - P106: Performance Acceleration Principle
# These functions apply performance optimizations based on the current acceleration settings

# Optimized rendering for plots with automatic debouncing
renderPlotOptimized <- function(expr, env = parent.frame(), quoted = FALSE, 
                              options = list()) {
  # Check if quoted
  if (!quoted) {
    expr <- substitute(expr)
  }
  
  # Apply acceleration if enabled
  if (is_acceleration_enabled("ui", "debounce_inputs")) {
    # Determine debounce time based on settings
    debounce_ms <- if (exists("get_acceleration_param")) {
      get_acceleration_param("reactive", "debounce_reactives", 500)
    } else {
      500 # Default debounce
    }
    
    # Use debounced rendering for better performance
    return(shiny::renderPlot({
      shinyjs::debounce(eval(expr, env), debounce_ms)
    }, options = options))
  } else {
    # Use standard rendering without acceleration
    return(shiny::renderPlot(expr, env = env, quoted = TRUE, options = options))
  }
}

# Optimized rendering for tables with automatic debouncing
renderTableOptimized <- function(expr, env = parent.frame(), quoted = FALSE, 
                               options = list()) {
  # Check if quoted
  if (!quoted) {
    expr <- substitute(expr)
  }
  
  # Apply acceleration if enabled
  if (is_acceleration_enabled("ui", "debounce_inputs")) {
    # Determine debounce time based on settings
    debounce_ms <- if (exists("get_acceleration_param")) {
      get_acceleration_param("reactive", "debounce_reactives", 300)
    } else {
      300 # Default debounce
    }
    
    # Use debounced rendering for better performance
    return(shiny::renderTable({
      shinyjs::debounce(eval(expr, env), debounce_ms)
    }, options = options))
  } else {
    # Use standard rendering without acceleration
    return(shiny::renderTable(expr, env = env, quoted = TRUE, options = options))
  }
}

# Component loading with acceleration support
getComponentWithAcceleration <- function(component_name, params) {
  # Check if component exists
  if (!exists(component_name)) {
    message("Component ", component_name, " does not exist")
    return(NULL)
  }
  
  # Check if lazy loading is enabled
  if (is_acceleration_enabled("ui", "lazy_rendering")) {
    # Create a placeholder component that loads the real component when needed
    placeholder_ui <- function(id) {
      shiny::div(
        id = shiny::NS(id, "placeholder"),
        style = "min-height: 200px; display: flex; align-items: center; justify-content: center;",
        shiny::p(class = "text-muted", "Loading component..."),
        shiny::tags$script(shiny::HTML(sprintf(
          "$(document).ready(function() {
            setTimeout(function() { 
              Shiny.setInputValue('%s', true, {priority: 'event'});
            }, 100);
          });",
          shiny::NS(id, "load_component")
        )))
      )
    }
    
    placeholder_server <- function(id, connection, session) {
      shiny::moduleServer(id, function(input, output, session) {
        component_loaded <- shiny::reactiveVal(FALSE)
        
        # When the component should be loaded
        shiny::observeEvent(input$load_component, {
          if (!component_loaded()) {
            # Dynamically load the actual component
            tryCatch({
              actual_component <- get(component_name)(id, params)
              
              # Replace placeholder with actual component
              shiny::insertUI(
                selector = paste0("#", shiny::NS(id, "placeholder")),
                where = "replace",
                ui = actual_component$ui(id)
              )
              
              # Initialize server
              actual_component$server(id, connection, session)
              
              component_loaded(TRUE)
            }, error = function(e) {
              # If error, display error message
              shiny::insertUI(
                selector = paste0("#", shiny::NS(id, "placeholder")),
                where = "replace",
                ui = shiny::div(
                  class = "alert alert-danger",
                  "Error loading component: ", e$message
                )
              )
            })
          }
        })
      })
    }
    
    return(list(
      ui = placeholder_ui,
      server = placeholder_server
    ))
  } else {
    # Standard component loading without acceleration
    return(get(component_name)(params))
  }
}

# Batch UI updates for better performance
batchUI <- function(func) {
  if (is_acceleration_enabled("ui", "batch_ui_updates")) {
    # Use shinyjs's delay function to batch UI updates
    return(function(...) {
      shinyjs::delay(0, func(...))
    })
  } else {
    return(func)
  }
}

# Optimized version of observeEvent that isolates expensive computations
observeEventOptimized <- function(eventExpr, handlerExpr, 
                                event.env = parent.frame(), 
                                handler.env = parent.frame(),
                                event.quoted = FALSE, handler.quoted = FALSE, 
                                ...) {
  
  # If isolation of expensive chains is enabled
  if (is_acceleration_enabled("reactive", "isolate_expensive_chains")) {
    # Quote expressions if needed
    if (!event.quoted) eventExpr <- substitute(eventExpr)
    if (!handler.quoted) handlerExpr <- substitute(handlerExpr)
    
    # Create observer that isolates portions of the handler
    return(shiny::observeEvent(
      eventExpr, 
      # Wrap non-UI related calculations in isolate
      {
        # Extract UI update expressions (typically output$... assignments)
        is_ui_update <- function(expr) {
          if (is.call(expr) && length(expr) >= 1) {
            # Check for assignment to output
            if (identical(expr[[1]], as.name("<-")) || identical(expr[[1]], as.name("="))) {
              if (is.call(expr[[2]]) && length(expr[[2]]) >= 2 && identical(expr[[2]][[1]], as.name("$"))) {
                if (identical(expr[[2]][[2]], as.name("output"))) {
                  return(TRUE)
                }
              }
            }
          }
          return(FALSE)
        }
        
        # If handler is a brace expression (multiple statements)
        if (is.call(handlerExpr) && identical(handlerExpr[[1]], as.name("{"))) {
          # Separate UI updates from calculations
          ui_updates <- list()
          calculations <- list()
          
          for (i in 2:length(handlerExpr)) {
            if (is_ui_update(handlerExpr[[i]])) {
              ui_updates <- c(ui_updates, handlerExpr[i])
            } else {
              calculations <- c(calculations, handlerExpr[i])
            }
          }
          
          # Perform calculations in isolate
          if (length(calculations) > 0) {
            calc_expr <- as.call(c(as.name("{"), calculations))
            shiny::isolate(eval(calc_expr, handler.env))
          }
          
          # Perform UI updates without isolation
          if (length(ui_updates) > 0) {
            update_expr <- as.call(c(as.name("{"), ui_updates))
            eval(update_expr, handler.env)
          }
        } else {
          # If not a brace expression, just evaluate directly
          eval(handlerExpr, handler.env)
        }
      },
      event.env = event.env,
      handler.env = handler.env,
      event.quoted = TRUE, 
      handler.quoted = TRUE,
      ...
    ))
  } else {
    # Use standard observeEvent if optimization disabled
    return(shiny::observeEvent(eventExpr, handlerExpr, 
                              event.env = event.env, 
                              handler.env = handler.env,
                              event.quoted = event.quoted, 
                              handler.quoted = handler.quoted, 
                              ...))
  }
}

# Clean up the cache periodically
scheduleAccelerationMaintenance <- function(session) {
  # Set up observer to periodically clean cache
  if (is_acceleration_enabled("data", "use_caching")) {
    observe({
      # Clean up every 10 minutes
      invalidateLater(10 * 60 * 1000, session)
      
      # Count objects in cache
      cache_size <- length(ls(.cache_env))
      if (cache_size > 0) {
        message("Acceleration cache maintenance: ", cache_size, " cached products")
        
        # If more than 50 products, remove oldest ones
        if (cache_size > 50) {
          # Get list of cached products
          cache_products <- ls(.cache_env)
          # Remove the first 20% of products (oldest)
          to_remove <- cache_products[1:max(1, round(cache_size * 0.2))]
          rm(list = to_remove, envir = .cache_env)
          message("Removed ", length(to_remove), " old cached products")
        }
      }
    })
  }
}

# Register acceleration status in session
registerAccelerationStatus <- function(session) {
  # Send acceleration status to client
  session$sendCustomMessage("acceleration_status", list(
    enabled = is_acceleration_enabled(NULL),
    level = if (exists("acceleration_config")) acceleration_config$level else 0
  ))
}

# Export the acceleration helper functions to global environment
for (fn_name in c("renderPlotOptimized", "renderTableOptimized", 
                 "getComponentWithAcceleration", "batchUI", 
                 "observeEventOptimized", "scheduleAccelerationMaintenance",
                 "registerAccelerationStatus")) {
  if (exists(fn_name)) {
    assign(fn_name, get(fn_name), envir = .GlobalEnv)
  }
}

message("Loaded acceleration helper functions (P106: Performance Acceleration Principle)")
#LOCK OFF