#' Lazy Component Initialization for Shiny Modules
#'
#' @description
#' Provides utilities for lazy (deferred) initialization of Shiny components.
#' Components are only initialized when the user first navigates to their tab,
#' reducing initial load time and memory usage.
#'
#' @principles
#' - P77: Performance Optimization
#' - MP56: Connected Component Principle
#' - UI_R001: UI-Server-Defaults Triple Pattern
#'
#' @created 2026-01-26
#' @author Claude Code

#' Create a lazy component wrapper
#'
#' @description
#' Wraps a component creation function to defer initialization until first use.
#' The component is created on-demand when the user navigates to the corresponding tab.
#'
#' @param component_fn Function. The component creation function (e.g., microCustomerComponent).
#' @param id Character. Module ID for the component.
#' @param ... Additional arguments passed to component_fn.
#' @return A list with lazy UI and server functions.
#' @export
#'
#' @examples
#' # Instead of:
#' # customer_comp <- microCustomerComponent("cust", conn, config, translate)
#' #
#' # Use:
#' # customer_comp <- lazyComponent(microCustomerComponent, "cust", conn, config, translate)
lazyComponent <- function(component_fn, id, ...) {
  # Store the creation arguments
  args <- list(...)

  # Track initialization state
  initialized <- FALSE
  component <- NULL

  # Create a function that initializes on first call
  get_component <- function() {
    if (!initialized) {
      component <<- do.call(component_fn, c(list(id), args))
      initialized <<- TRUE
      message("Lazy initialized component: ", id)
    }
    component
  }

  # Return a list that mimics the component structure
  # but defers actual creation
  list(
    # UI is created immediately (needed for page structure)
    # but uses placeholder loading state
    ui = list(
      filter = NULL,  # Will be populated on demand
      display = NULL  # Will be populated on demand
    ),
    # Server is lazy
    server = function(input, output, session) {
      # This will be called later, triggering initialization
      comp <- get_component()
      if (!is.null(comp) && !is.null(comp$server)) {
        comp$server(input, output, session)
      }
    },
    # Helper to check if initialized
    is_initialized = function() initialized,
    # Force initialization
    initialize = function() {
      get_component()
    },
    # Get the actual component (initializes if needed)
    get = get_component
  )
}

# Note: renderUI2 is defined in fn_renderUI2.R
# This file focuses on additional lazy loading utilities

#' Create a tab-aware component initializer
#'
#' @description
#' Creates a reactive that initializes a component only when its tab becomes active.
#' This is useful for expensive components that shouldn't be created at startup.
#'
#' @param session Shiny session object.
#' @param sidebar_menu Character. Input ID for the sidebar menu.
#' @param tab_mappings Named list. Maps tab names to component creation functions.
#' @return A reactive that returns the initialized components.
#' @export
#'
#' @examples
#' # In server function:
#' # components <- tabAwareInitializer(
#' #   session = session,
#' #   sidebar_menu = "sidebar_menu",
#' #   tab_mappings = list(
#' #     "dna" = function() microDNADistributionComponent("dna", conn, config),
#' #     "customer" = function() microCustomerComponent("cust", conn, config)
#' #   )
#' # )
tabAwareInitializer <- function(session, sidebar_menu, tab_mappings) {
  # Track which components have been initialized
  initialized_tabs <- reactiveValues()

  # Store initialized components
  components <- reactiveValues()

  # Watch for tab changes
  observeEvent(session$input[[sidebar_menu]], {
    current_tab <- session$input[[sidebar_menu]]

    # Check if this tab has a mapping and hasn't been initialized
    if (!is.null(current_tab) &&
        current_tab %in% names(tab_mappings) &&
        is.null(initialized_tabs[[current_tab]])) {

      # Initialize the component
      message("Initializing component for tab: ", current_tab)
      components[[current_tab]] <- tab_mappings[[current_tab]]()
      initialized_tabs[[current_tab]] <- TRUE
    }
  })

  # Return a function to get components
  function(tab_name) {
    if (!is.null(initialized_tabs[[tab_name]])) {
      components[[tab_name]]
    } else {
      NULL
    }
  }
}

#' Create a component pool for memory-efficient management
#'
#' @description
#' Manages a pool of components, automatically unloading inactive ones
#' to conserve memory. Useful for dashboards with many tabs.
#'
#' @param max_active Integer. Maximum number of components to keep active.
#' @return A component pool manager.
#' @export
componentPool <- function(max_active = 5) {
  pool <- new.env(parent = emptyenv())
  access_order <- character(0)

  list(
    # Get or create a component
    get = function(id, create_fn) {
      if (exists(id, envir = pool)) {
        # Move to end of access order (most recently used)
        access_order <<- c(setdiff(access_order, id), id)
        return(pool[[id]])
      }

      # Create new component
      pool[[id]] <- create_fn()
      access_order <<- c(access_order, id)

      # Evict oldest if over limit
      while (length(access_order) > max_active) {
        oldest <- access_order[1]
        rm(list = oldest, envir = pool)
        access_order <<- access_order[-1]
        message("Evicted component from pool: ", oldest)
      }

      pool[[id]]
    },

    # Check if component exists
    has = function(id) {
      exists(id, envir = pool)
    },

    # Get current pool size
    size = function() {
      length(access_order)
    },

    # Clear the pool
    clear = function() {
      rm(list = ls(pool), envir = pool)
      access_order <<- character(0)
    }
  )
}
