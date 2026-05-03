# Defaults for Union Sidebar
# Following P21 component n-tuple pattern and NSQL set theoretical naming

#' Default values for sidebar filters
#'
#' Contains default values for all filters in the union sidebar
#'
#' @return A list of default values for sidebar filters
#' @export
sidebarUnionDefaults <- function() {
  list(
    # Common filters defaults
    common = list(
      channel = "amazon",
      category = "000"  # All Products
    ),
    
    # Micro tab filter defaults
    micro = list(
      region = "000"  # All Regions
    ),
    
    # Macro tab filter defaults
    macro = list(
      aggregation = "Product Category",
      comparison = FALSE,
      date_range = c(Sys.Date() - 90, Sys.Date())
    ),
    
    # Target tab filter defaults
    target = list(
      campaign = "Summer Promotion",
      audience = "New Customers"
    )
  )
}