# Common components for the precision marketing application
# Following MP56: Connected Component Principle
# Following R09: UI-Server-Defaults Triple Rule
# Following P62: Separation of Concerns

#' @title Common Filters Component
#' @description Common filters that are always shown regardless of the active tab
#' @export
#' @principle MP56 Connected Component Principle
#' @principle R09 UI-Server-Defaults Triple Rule
commonFiltersComponent <- function(id, app_data_connection = NULL, config = NULL, translate = function(x) x) {
  # Return the components as a structured list following R09
  list(
    ui = list(
      filter = function(id) {
        ns <- NS(id)
        div(
          id = ns("common_filters"),
          class = "common-filters sidebar-filters sidebar-section",
          
          # Title
          h4(translate("Common Filters"), class = "sidebar-section-title"),
          
          # Marketing Channel filter
          if (exists("channel_availability")) {
            # Determine which channel is available and select the first available one as default
            available_channels <- names(marketing_channels)[sapply(marketing_channels, function(ch) {
              !is.null(channel_availability[[ch]]) && channel_availability[[ch]]
            })]
            
            default_channel <- if (length(available_channels) > 0) marketing_channels[available_channels[1]] else marketing_channels[1]
            
            # Create choice names with availability indicators
            choice_names <- lapply(names(marketing_channels), function(name) {
              channel_id <- marketing_channels[[name]]
              is_available <- !is.null(channel_availability[[channel_id]]) && channel_availability[[channel_id]]
              
              if (!is_available) {
                HTML(paste(name, '<span class="unavailable-tag"> (No Data)</span>'))
              } else {
                name
              }
            })
            
            # Create radio buttons with availability indicators
            radioButtons(
              inputId = ns("common_channel"),
              label = translate("Select Channel:"),
              choiceNames = choice_names,
              choiceValues = unname(marketing_channels),
              selected = default_channel
            )
          } else {
            # Fallback if channel_availability is not available
            radioButtons(
              inputId = ns("common_channel"),
              label = translate("Select Channel:"),
              choices = marketing_channels,
              selected = marketing_channels[1]
            )
          },
          
          # Product Category filter
          selectizeInput(
            ns("common_category"),
            translate("Select Category:"),
            choices = product_categories,
            selected = product_categories[1]
          )
        )
      },
      display = function(id) { div() }  # Empty display for filters
    ),
    server = function(input, output, session) {
      # Return channel and category selections
      reactive({
        list(
          channel = input$common_channel,
          category = input$common_category
        )
      })
    },
    defaults = function() {
      list(
        channel = if (exists("marketing_channels") && length(marketing_channels) > 0) marketing_channels[1] else NULL,
        category = if (exists("product_categories") && length(product_categories) > 0) product_categories[1] else NULL
      )
    }
  )
}

#' @title Create Placeholder Component
#' @description Creates a placeholder component following the UI-Server-Defaults Triple Rule (R09)
#' @param component_name The name of the component to display
#' @return A list containing ui, server, and defaults functions
#' @export
#' @principle R09 UI-Server-Defaults Triple Rule
createPlaceholderComponent <- function(component_name) {
  list(
    ui = list(
      filter = function(id) { div() },  # Empty filter
      display = function(id) { 
        div(
          class = "placeholder-component",
          style = "padding: 20px; background-color: #f8f9fa; border-radius: 5px; margin: 15px;",
          h3(paste("Placeholder for", component_name)),
          p(paste("This component (", component_name, ") is not available or hasn't been implemented yet."))
        )
      }
    ),
    server = function(input, output, session) {
      # Return an empty reactive value
      reactive({})
    },
    defaults = function() {
      # Return an empty list for defaults
      list()
    }
  )
}

#' @title Get Component Or Placeholder
#' @description Gets the specified component if it exists, otherwise returns a placeholder
#' @param component_fn The component function name to look for
#' @param args List of arguments to pass to the component function if it exists
#' @return The component or a placeholder
#' @export
#' @principle R09 UI-Server-Defaults Triple Rule
#' @principle MP56 Connected Component Principle
getComponentOrPlaceholder <- function(component_fn, args = list()) {
  # Check if the function exists in the global environment
  if (exists(component_fn, mode = "function")) {
    # Get the function
    fn <- get(component_fn, mode = "function")
    # Call the function with the provided arguments
    component <- do.call(fn, args)
    
    # Ensure component has all required functions (ui.filter, ui.display, server, defaults)
    if (!is.list(component)) {
      warning(paste0("Component ", component_fn, " did not return a proper component list structure"))
      return(createPlaceholderComponent(component_fn))
    }
    
    # Ensure component has UI functions
    if (!("ui" %in% names(component))) {
      component$ui <- list(
        filter = function(id) { div() },
        display = function(id) { div() }
      )
    } else {
      # Check filter and display functions
      if (!("filter" %in% names(component$ui))) {
        component$ui$filter <- function(id) { div() }
      }
      if (!("display" %in% names(component$ui))) {
        component$ui$display <- function(id) { div() }
      }
    }
    
    # Ensure component has server function
    if (!("server" %in% names(component))) {
      component$server <- function(input, output, session) {
        reactive({})
      }
    }
    
    # Ensure component has defaults function
    if (!("defaults" %in% names(component))) {
      component$defaults <- function() {
        list()
      }
    }
    
    return(component)
  } else {
    # Return a placeholder component
    createPlaceholderComponent(component_fn)
  }
}