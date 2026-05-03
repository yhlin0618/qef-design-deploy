#' Safely Retrieve Object from Environment or File
#'
#' @title Safe Object Retrieval with Fallback Loading
#' @description Attempts to retrieve an object from the global environment, and if
#' not found or invalid (NULL or empty data frame), loads it from a saved RDS file.
#' This function provides a robust way to ensure required data objects are available,
#' with automatic fallback to persistent storage. Particularly useful in Shiny apps
#' and data pipelines where objects may or may not be pre-loaded.
#'
#' Following principles:
#' - R094: Roxygen2 documentation standard
#' - MP047: Functional programming
#' - R091: Universal data access pattern
#' - MP045: Automatic data availability detection
#'
#' @param object_name Character string specifying the name of the object to retrieve.
#'        This name is used both for environment lookup and file naming.
#' @param folder_path Character string specifying the directory path where RDS files
#'        are stored. Default is "app_data". Path can be relative or absolute.
#' @param suppress Logical indicating whether to suppress informational messages.
#'        Default is FALSE (messages shown). Set to TRUE for silent operation.
#'        
#' @return The requested object if found (either from environment or file).
#'         Returns an empty data.frame() if object cannot be found anywhere.
#'         Side effect: If loaded from file, assigns object to global environment.
#'         
#' @examples
#' # Basic usage - retrieve customer data
#' customer_data <- safe_get("customer_data")
#' 
#' # Specify custom data directory
#' sales_data <- safe_get("sales_2024", folder_path = "data/sales")
#' 
#' # Silent operation (no messages)
#' product_list <- safe_get("products", suppress = TRUE)
#' 
#' # In Shiny app context
#' reactive_data <- reactive({
#'   safe_get(input$dataset_name, folder_path = "datasets")
#' })
#' 
#' @export
#' @note This function modifies the global environment by assigning loaded objects
#' @principle R094 Roxygen2 documentation standard
#' @principle R091 Universal data access
#' @principle MP045 Automatic data availability detection
safe_get <- function(object_name, folder_path = "app_data", suppress = FALSE) {
  
  # 定義一個內部函數，根據 suppress 參數決定是否顯示訊息
  message_conditional <- function(msg) {
    if (!suppress) message(msg)
  }
  
  # 定義一個檢查物件是否無效的函數（NULL 或 nrow = 0）
  is_invalid <- function(obj) {
    is.null(obj) || (is.data.frame(obj) && nrow(obj) == 0)
  }
  
  # 使用 get0 檢查並嘗試取得物件
  r <- get0(object_name, envir = .GlobalEnv)
  
  # 如果 r 無效，則從指定資料夾中載入
  if (is_invalid(r)) {
    # 設定檔案路徑
    file_path <- file.path(folder_path, paste0(object_name, ".rds"))
    
    # 檢查檔案是否存在
    if (file.exists(file_path)) {
      message_conditional(paste(object_name, "不存在或為空，從", file_path, "載入。"))
      # 載入物件並存入全域環境
      r <- readRDS(file_path)
      assign(object_name, r, envir = .GlobalEnv)
    } else {
      warning(paste("無法找到", object_name, "。確保", file_path, "檔案存在。返回空的 data.frame()。"))
      # 返回空的 data.frame()
      r <- data.frame()
    }
  } else {
    message_conditional(paste(object_name, "已存在於環境中，直接取得。"))
  }
  
  return(r)
}
