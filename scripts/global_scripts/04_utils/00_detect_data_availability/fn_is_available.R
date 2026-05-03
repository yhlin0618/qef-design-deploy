#' Check if data is available for a specific dimension
#'
#' @description
#' Checks the global availability registry to determine if data is available
#' for a specific domain and dimension.
#'
#' @param domain The domain to check (e.g., "channel")
#' @param dimension The specific dimension to check (e.g., "amazon")
#' @return TRUE if the data is available, FALSE otherwise
#' @export
#' @implements MP45 Automatic Data Availability Detection
is_available <- function(domain, dimension = NULL) {
  # This function now uses the global availability variables
  # instead of a reactive registry
  
  if (domain == "channel") {
    # Check if channel_availability exists
    if (!exists("channel_availability", envir = .GlobalEnv)) {
      return(FALSE)
    }
    
    # Get channel availability from global variable
    avail <- get("channel_availability", envir = .GlobalEnv)
    
    # If no specific dimension requested, check if domain has any available dimensions
    if (is.null(dimension)) {
      return(any(unlist(avail)))
    }
    
    # Check specific dimension
    return(!is.null(avail[[dimension]]) && avail[[dimension]])
  }
  
  # For other domains, not yet implemented
  return(FALSE)
}