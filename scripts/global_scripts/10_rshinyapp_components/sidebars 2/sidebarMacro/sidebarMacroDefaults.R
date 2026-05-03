#' Default Values for Macro Sidebar Component
#'
#' This file provides standard default values for the macro sidebar component.
#' These defaults ensure that all UI outputs have appropriate values even when
#' data is unavailable or invalid, implementing the UI-Server Pairing Rule.
#'
#' @return Named list of output IDs and their default values
#' @export
sidebarMacroDefaults <- function() {
  list(
    # Default metrics
    metrics = list(
      "營收 (Revenue)" = "revenue",
      "客單價 (Average Order Value)" = "aov",
      "訂單數 (Order Count)" = "order_count",
      "客戶數 (Customer Count)" = "customer_count",
      "轉換率 (Conversion Rate)" = "conversion_rate",
      "退貨率 (Return Rate)" = "return_rate",
      "淨利潤 (Net Profit)" = "net_profit",
      "流失率 (Churn Rate)" = "churn_rate"
    )
  )
}