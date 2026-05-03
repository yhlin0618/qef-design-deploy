#' Get Product ID Label for Platform
#'
#' Returns the appropriate product ID label based on the current platform selection.
#' This follows R123 (Unified product ID Rule) for display purposes while maintaining
#' platform-specific terminology in the UI.
#'
#' @param platform_id Character. The platform ID (e.g., "amz", "eby", "all").
#' @param df_platform Data frame. Platform configuration data (optional, will use global if not provided).
#' @param translate Function. Translation function for UI text (default: identity).
#'
#' @return Character. The appropriate label for the product ID field.
#'
#' @examples
#' \dontrun{
#' # Get label for Amazon
#' get_product_id_label("amz")  # Returns "ASIN"
#' 
#' # Get label for eBay
#' get_product_id_label("eby")  # Returns "product Number"
#' 
#' # Get label for all platforms
#' get_product_id_label("all")  # Returns "product ID"
#' }
#'
#' @export
get_product_id_label <- function(platform_id, df_platform = NULL, translate = identity) {
  # Use global df_platform if not provided
  if (is.null(df_platform) && exists("df_platform", envir = .GlobalEnv)) {
    df_platform <- get("df_platform", envir = .GlobalEnv)
  }
  
  # Default label
  default_label <- translate("product ID")
  
  # Return default if no platform data
  if (is.null(df_platform) || is.null(platform_id)) {
    return(default_label)
  }
  
  # Handle "all" platform
  if (platform_id == "all" || platform_id == "0") {
    return(default_label)
  }
  
  # Look up platform-specific coding
  platform_row <- df_platform[df_platform$platform_id == platform_id, ]
  
  if (nrow(platform_row) == 0) {
    return(default_label)
  }
  
  product_id_coding <- platform_row$product_id_coding[1]
  
  # Map coding to user-friendly labels
  label_map <- c(
    "asin" = translate("ASIN"),
    "ebay_product_number" = translate("product Number"),
    "sku" = translate("SKU"),
    "product_id" = translate("Product ID")
  )
  
  # Return mapped label or the coding itself if not in map
  if (!is.na(product_id_coding) && product_id_coding != "") {
    mapped_label <- label_map[product_id_coding]
    if (!is.null(mapped_label) && !is.na(mapped_label)) {
      return(as.character(unname(mapped_label)))
    } else {
      return(translate(toupper(product_id_coding)))
    }
  }
  
  return(default_label)
}