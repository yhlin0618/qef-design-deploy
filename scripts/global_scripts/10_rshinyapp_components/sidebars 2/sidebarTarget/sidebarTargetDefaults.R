#' Default Values for Target Sidebar Component
#'
#' This file provides standard default values for the target sidebar component.
#' These defaults ensure that all UI outputs have appropriate values even when
#' data is unavailable or invalid, implementing the UI-Server Pairing Rule.
#'
#' @return Named list of output IDs and their default values
#' @export
sidebarTargetDefaults <- function() {
  list(
    # Default campaigns
    campaigns = list(
      "2025 春季促銷 (Spring Promotion 2025)" = "camp_spring2025",
      "新客戶歡迎計劃 (New Customer Welcome)" = "camp_newcustomer",
      "會員回饋活動 (Loyalty Rewards)" = "camp_loyalty",
      "節日特別優惠 (Holiday Special)" = "camp_holiday"
    ),
    
    # Default audience segments
    audiences = list(
      "高價值顧客 (High Value, 2,500 顧客)" = "high_value",
      "休眠客戶 (Dormant, 15,000 顧客)" = "dormant",
      "新進顧客 (New, 8,200 顧客)" = "new_customers",
      "流失風險 (At Risk, 3,600 顧客)" = "at_risk",
      "產品愛好者 (Product Enthusiasts, 4,800 顧客)" = "enthusiasts"
    )
  )
}