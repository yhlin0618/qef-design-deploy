#' Default Values for Main Sidebar Component
#'
#' This file provides standard default values for the main sidebar component.
#' These defaults ensure that all UI outputs have appropriate values even when
#' data is unavailable or invalid, implementing the UI-Server Pairing Rule.
#'
#' @return Named list of output IDs and their default values
#' @export
sidebarMainDefaults <- function() {
  list(
    # Default product categories
    product_categories = list(
      "服裝 (Clothing)" = "clothing",
      "電子產品 (Electronics)" = "electronics",
      "家居用品 (Home Goods)" = "homegoods",
      "食品飲料 (Food & Beverage)" = "foodbeverage",
      "健康美容 (Health & Beauty)" = "healthbeauty"
    ),
    
    # Default geographic regions
    geographic_regions = list(
      "全部 (All)" = "all",
      "北部 (North)" = "north",
      "南部 (South)" = "south",
      "東部 (East)" = "east",
      "西部 (West)" = "west"
    ),
    
    # Default filter status
    filter_status = "未啟用篩選器"
  )
}