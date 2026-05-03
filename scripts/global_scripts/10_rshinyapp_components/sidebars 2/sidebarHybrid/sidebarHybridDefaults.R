#' Default Values for Hybrid Sidebar Component
#'
#' This file provides standard default values for the hybrid sidebar component.
#' These defaults ensure that all UI outputs have appropriate values even when
#' data is unavailable or invalid, implementing the UI-Server Pairing Rule.
#'
#' @return Named list of default values
#' @export
sidebarHybridDefaults <- function() {
  # Helper function to ensure source data is loaded
  ensure_source_data <- function() {
    if (!exists("source_dictionary") || length(source_dictionary) == 0) {
      # Try to load source data
      if (file.exists(file.path("app_data", "scd_type1", "source.xlsx"))) {
        if (requireNamespace("readxl", quietly = TRUE) && requireNamespace("dplyr", quietly = TRUE)) {
          source_data <- readxl::read_excel(file.path("app_data", "scd_type1", "source.xlsx"))
          
          if (nrow(source_data) > 0 && "source_english" %in% names(source_data) && "source_chinese" %in% names(source_data)) {
            source_dtah <- source_data %>%
              dplyr::mutate(source = tolower(gsub(" ", "", source_english)))
            
            language <- if (exists("language")) language else "chinese"
            
            source_dict <- as.list(setNames(source_dtah$source, 
                                            source_dtah[[paste0("source_", language)]]))
            
            return(source_dict)
          }
        }
      }
      
      # Fallback if source data couldn't be loaded
      return(list(
        "亞馬遜" = "amazon", 
        "官方網站" = "officialwebsite"
      ))
    } else {
      return(source_dictionary)
    }
  }
  
  # Get the source data
  sources <- ensure_source_data()
  
  list(
    # Add sources to the defaults
    distribution_channels = sources,
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
    
    # Default customer segments for micro view
    customer_segments = list(
      "高價值忠誠客戶 (High-Value Loyal)" = "high_value_loyal",
      "高頻率顧客 (High Frequency)" = "high_frequency",
      "間歇性購買者 (Occasional Buyers)" = "occasional",
      "休眠顧客 (Dormant)" = "dormant",
      "新顧客 (New Customers)" = "new_customers"
    ),
    
    # Default campaigns for target view
    campaigns = list(
      "2025 春季促銷 (Spring Promotion 2025)" = "camp_spring2025",
      "新客戶歡迎計劃 (New Customer Welcome)" = "camp_newcustomer",
      "會員回饋活動 (Loyalty Rewards)" = "camp_loyalty",
      "節日特別優惠 (Holiday Special)" = "camp_holiday"
    ),
    
    # Default audience segments for target view
    audiences = list(
      "高價值顧客 (High Value, 2,500 顧客)" = "high_value",
      "休眠客戶 (Dormant, 15,000 顧客)" = "dormant",
      "新進顧客 (New, 8,200 顧客)" = "new_customers",
      "流失風險 (At Risk, 3,600 顧客)" = "at_risk",
      "產品愛好者 (Product Enthusiasts, 4,800 顧客)" = "enthusiasts"
    ),
    
    # Default module labels
    module_labels = list(
      "main" = "總覽過濾器",
      "micro" = "顧客分析過濾器",
      "macro" = "總體分析過濾器",
      "target" = "行銷活動過濾器"
    )
  )
}