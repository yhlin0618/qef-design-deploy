#' Sidebar Factory Server Component
#'
#' This component provides a unified interface for initializing section-specific sidebar
#' servers based on the current app section.
#'
#' IMPORTANT: This server component fulfills all outputs defined in the matching
#' sidebarFactoryUI() function. According to the UI-Server Pairing Rule, both components
#' must always be used together in the application along with the sidebarFactoryDefaults.R
#' file that defines default values.
#'
#' @param id The module ID
#' @param section The app section (main, micro, macro, target)
#' @param data_source The data source specification
#'
#' @return None (server component with side effects)
#' @export
sidebarFactoryServer <- function(id, section = "main", data_source = NULL) {
  moduleServer(id, function(input, output, session) {
    # Validate section parameter
    section <- match.arg(section, c("main", "micro", "macro", "target"))
    
    # Get default values
    defaults <- sidebarFactoryDefaults()
    
    # Initialize the appropriate sidebar server based on section
    switch(section,
      "main" = sidebarMainServer("main", data_source),
      "micro" = sidebarMicroServer("micro", data_source),
      "macro" = sidebarMacroServer("macro", data_source),
      "target" = sidebarTargetServer("target", data_source)
    )
    
    # Return the section for reference
    reactive({
      section
    })
  })
}