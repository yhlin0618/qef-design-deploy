#' Render an adaptive radio button control based on data availability
#'
#' @description
#' This function implements P70 (Complete Input Display Principle) by showing
#' all options but visually indicating and disabling those that are unavailable.
#' 
#' @note This function is deprecated. Use direct renderUI implementation instead.
#'
#' @param id The input ID
#' @param label The input label
#' @param choices Named list of choices
#' @param registry The availability registry
#' @param domain The domain to check (e.g., "channel")
#' @param session The Shiny session object (optional)
#' @return A rendered UI element
#' @export
#' @implements P70 Complete Input Display Principle
render_adaptive_radio_buttons <- function(id, label, choices, registry, domain, session = NULL) {
  # WARNING: This function is no longer used directly - see generateSidebar.R
  # It is kept for backward compatibility
  
  # Generate a warning to indicate this function is deprecated
  warning("render_adaptive_radio_buttons is deprecated. Use direct renderUI implementation instead.")
  
  shiny::renderUI({
    # Use req() to ensure registry is available and is a function
    shiny::req(registry)
    
    # Note: This function will cause errors if registry is not a reactive value
    # The direct implementation in generateSidebar.R should be used instead
    
    # This implementation is kept for backward compatibility only
    shiny::div("Error: Deprecated function. Update your code to use direct renderUI implementation.")
  })
}