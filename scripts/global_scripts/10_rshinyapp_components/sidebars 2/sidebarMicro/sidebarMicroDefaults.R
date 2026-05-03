#' Default Values for Micro Sidebar Component
#'
#' This file provides standard default values for the micro sidebar component.
#' These defaults ensure that all UI outputs have appropriate values even when
#' data is unavailable or invalid, implementing the UI-Server Pairing Rule.
#'
#' @return Named list of output IDs and their default values
#' @export
sidebarMicroDefaults <- function() {
  list(
    # Default customer segments
    customer_segments = list(
      "高價值忠誠客戶 (High-Value Loyal)" = "high_value_loyal",
      "高頻率顧客 (High Frequency)" = "high_frequency",
      "間歇性購買者 (Occasional Buyers)" = "occasional",
      "休眠顧客 (Dormant)" = "dormant",
      "新顧客 (New Customers)" = "new_customers"
    ),
    
    # Default lifecycle stages
    lifecycle_stages = list(
      "首次購買 (First Purchase)" = "first_purchase",
      "成長階段 (Growth)" = "growth",
      "成熟階段 (Maturity)" = "maturity",
      "回流階段 (Reactivation)" = "reactivation",
      "流失風險 (At Risk)" = "at_risk",
      "已流失 (Churned)" = "churned"
    ),
    
    # Default filter status
    filter_status = "未啟用篩選器"
  )
}