#' Sidebar Factory UI Component
#'
#' This component provides a unified interface for creating section-specific sidebars
#' based on the current app section.
#'
#' IMPORTANT: According to the UI-Server Pairing Rule, this UI component MUST be used with
#' its corresponding server component sidebarFactoryServer(). All outputs defined here
#' must be fulfilled by the server component.
#'
#' @param id The module ID
#' @param section The app section (main, micro, macro, target)
#'
#' @return A sidebar UI component
#' @export
sidebarFactoryUI <- function(id, section = "main") {
  ns <- NS(id)
  
  # Validate section parameter
  section <- match.arg(section, c("main", "micro", "macro", "target"))
  
  # Return the appropriate sidebar UI based on section
  switch(section,
    "main" = sidebarMainUI(ns("main")),
    "micro" = sidebarMicroUI(ns("micro")),
    "macro" = sidebarMacroUI(ns("macro")),
    "target" = sidebarTargetUI(ns("target"))
  )
}