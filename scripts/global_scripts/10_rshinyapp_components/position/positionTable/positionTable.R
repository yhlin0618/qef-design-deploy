#LOCK FILE
#
# positionTable.R
#
# Following principles:
# - MP56: Connected Component Principle (component structure)
# - MP73: Interactive Visualization Preference (DT for table visualizations)
# - MP81: Explicit Parameter Specification (function arguments)
# - R116: Enhanced Data Access with tbl2 (data access)
# - R09: UI-Server-Defaults Triple (component organization)
# - R117: Column NA Reduction (remove columns with >50% NA values)
# - MP88: Immediate Feedback (real-time filtering without Apply button)
#

#
# Features:
#   • Character-based platform IDs (e.g., "amz", "eby", "all")
#   • Dynamic filtering by platform, product_line, brand, and Item ID
#   • Integration with global filters (platform, product_line)
#   • Tabular data visualization with DT
# -----------------------------------------------------------------------------

# helper ----------------------------------------------------------------------
#' Paste operator for string concatenation
#' @param x Character string. First string to concatenate.
#' @param y Character string. Second string to concatenate.
#' @return Character string. The concatenated result of x and y.
`%+%` <- function(x, y) paste0(x, y)

#' NULL coalescing operator
#' @param x Any value. The value to use if not NULL.
#' @param y Any value. The fallback value to use if x is NULL.
#' @return Either x or y. Returns x if it's not NULL, otherwise returns y.
`%||%` <- function(x, y) if (is.null(x)) y else x

#' Simple Filter for Position Table (No Loop)
#' 
#' 實現簡單的兩步驟篩選邏輯：
#' 1. 移除完全空的row（所有屬性都是NA的產品）
#' 2. 移除大部分空的column（非NA比例低於threshold的屬性）
#' 
#' @param data data.frame. 原始 Position Table 資料
#' @param threshold numeric. 篩選門檻，預設 0.3 (30%)
#' @return data.frame. 篩選後的資料
simple_filter_position_table <- function(data, threshold = 0.3) {
  # 確保有資料
  if (is.null(data) || nrow(data) == 0) {
    return(data)
  }

  # ISSUE_113 Fix: Validate and clean brand field first (MP114 - input validation)
  if ("brand" %in% colnames(data)) {
    data <- data %>%
      dplyr::mutate(
        # Convert empty strings to NA first
        brand = dplyr::na_if(brand, ""),
        # Then handle NA values
        brand = tidyr::replace_na(brand, "UNKNOWN")
      )

    # Log validation (MP106 - console output transparency)
    empty_brand_count <- sum(data$brand == "", na.rm = TRUE)
    if (empty_brand_count > 0) {
      message("  ⚠️  Found ", empty_brand_count, " rows with empty brand - converting to 'UNKNOWN'")
    }
  }

  # 識別必須保留的基本欄位
  essential_cols <- c("product_id", "brand", "product_line_id", "rating", "sales")
  essential_cols <- intersect(essential_cols, colnames(data))
  
  # 識別可以篩選的數值屬性欄位（排除基本欄位）
  numeric_cols <- colnames(data)[sapply(data, is.numeric)]
  filterable_cols <- setdiff(numeric_cols, essential_cols)
  
  message("🔧 開始簡單篩選 Position Table...")
  message("  📊 原始資料：", nrow(data), " 行 × ", ncol(data), " 列")
  message("  🔧 可篩選欄位：", length(filterable_cols), " 個")
  message("  🛡️  必保留欄位：", paste(essential_cols, collapse = ", "))
  
  current_data <- data
  original_rows <- nrow(current_data)
  original_cols <- ncol(current_data)
  
  # 步驟 1：移除完全空的行（在所有屬性欄位中都是 NA 的產品）
  if (length(filterable_cols) > 0) {
    current_filterable_cols <- intersect(filterable_cols, colnames(current_data))
    
    if (length(current_filterable_cols) > 0) {
      # 計算每一行在可篩選欄位中的非 NA 個數
      row_non_na_count <- apply(current_data[current_filterable_cols], 1, function(row) {
        sum(!is.na(row))
      })
      
      # 保留至少有一個非 NA 值的行
      non_empty_rows <- row_non_na_count > 0

      # Log which rows are being removed (MP106 - transparency)
      if (any(!non_empty_rows)) {
        removed_indices <- which(!non_empty_rows)
        for (idx in removed_indices) {
          if ("product_id" %in% colnames(current_data) && "brand" %in% colnames(current_data)) {
            message(sprintf("    Removing row %d: product_id='%s', brand='%s' (all attributes NA)",
                          idx,
                          current_data$product_id[idx],
                          current_data$brand[idx]))
          }
        }
      }

      current_data <- current_data[non_empty_rows, ]

      message(sprintf("  ✂️  步驟1 - 移除空行：保留 %d/%d 行",
                     sum(non_empty_rows), original_rows))
    }
  }
  
  # 步驟 2：移除大部分空的列（非 NA 比例低於 threshold 的屬性）
  if (nrow(current_data) > 0 && length(filterable_cols) > 0) {
    current_filterable_cols <- intersect(filterable_cols, colnames(current_data))
    
    if (length(current_filterable_cols) > 0) {
      # 計算每個可篩選欄位的非 NA 比例
      col_valid_ratio <- sapply(current_data[current_filterable_cols], function(col) {
        sum(!is.na(col)) / length(col)
      })
      
      # 保留比例 >= threshold 的欄位
      valid_filterable_cols <- names(col_valid_ratio)[col_valid_ratio >= threshold]
      
      # 保留必要欄位 + 通過篩選的可篩選欄位
      keep_cols <- unique(c(essential_cols, valid_filterable_cols))
      keep_cols <- intersect(keep_cols, colnames(current_data))  # 確保欄位存在
      
      current_data <- current_data %>% dplyr::select(dplyr::all_of(keep_cols))
      
      message(sprintf("  ✂️  步驟2 - 移除空列：保留 %d/%d 屬性欄位（比例 >= %.1f）", 
                     length(valid_filterable_cols), length(current_filterable_cols), threshold))
    }
  }
  
  message(sprintf("🎯 簡單篩選完成：%d 行 × %d 列", 
                 nrow(current_data), ncol(current_data)))
  
  return(current_data)
}



# Filter UI -------------------------------------------------------------------
#' positionTableFilterUI
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param translate Function. Translation function for UI text elements (defaults to identity function).
#'        Should accept a string and return a translated string.
#' @return shiny.tag. A Shiny UI component containing the filter controls for the position table component.
positionTableFilterUI <- function(id, translate = identity) {
  ns <- NS(id)
  
  wellPanel(
    style = "padding:15px;",
    h4(translate("Position Table Filters")),
    
    # Brand filter
    selectizeInput(
      inputId = ns("brand"),
      label = translate("Brand"),
      choices = NULL,
      multiple = TRUE,
      options = list(plugins = list('remove_button', 'drag_drop'))
    ),
    
    # Item ID filter (label will be updated dynamically based on platform)
    selectizeInput(
      inputId = ns("product_id"),
      label = translate("Item ID"),  # Default label, will be updated by server
      choices = NULL,
      multiple = TRUE,
      options = list(plugins = list('remove_button', 'drag_drop'))
    ),
    
    # Display options
    hr(),
    h4(translate("Display Options")),
    
    # Show Ideal value option
    checkboxInput(
      inputId = ns("show_ideal"),
      label = translate("Show ideal values"),
      value = TRUE
    ),
    
    # Reset button only (Apply filter removed for instant filtering)
    actionButton(
      inputId = ns("reset_filters"),
      label = translate("Reset Filters"),
      class = "btn-outline-secondary btn-block mt-3"
    ),
    
    hr(),
    textOutput(ns("component_status"))
  )
}

# Display UI ------------------------------------------------------------------
#' positionTableDisplayUI
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param translate Function. Translation function for UI text elements (defaults to identity function).
#' @return shiny.tag. A Shiny UI component containing the display elements for the position table.
positionTableDisplayUI <- function(id, translate = identity) {
  ns <- NS(id)
  
  tagList(
    div(class = "component-header mb-3 text-center",
        h3(translate("Position Table Analysis")),
        p(translate("Compare and analyze position metrics across brands and products"))),
    div(class = "component-output p-3",
        # 添加 CSS 確保水平滾動條始終可見
        tags$style(HTML(paste0(
          "/* 強制顯示水平滾動條 */",
          "#", ns("position_table"), " .dataTables_scrollBody {",
          "  overflow-x: scroll !important;",  # 強制顯示水平滾動條
          "  overflow-y: auto !important;",    # 垂直滾動條根據需要顯示
          "  -webkit-overflow-scrolling: touch !important;",
          "}",
          "/* 確保滾動容器有固定高度 */",
          "#", ns("position_table"), " .dataTables_scroll {",
          "  overflow-x: scroll !important;",
          "}",
          "/* 確保表格容器寬度 */",
          "#", ns("position_table"), " .dataTables_wrapper {",
          "  width: 100% !important;",
          "  overflow-x: auto !important;",
          "}",
          "/* 針對整個表格容器 */",
          "#", ns("position_table"), " {",
          "  width: 100% !important;",
          "  overflow-x: auto !important;",
          "}",
          "/* 確保滾動條在 macOS 等系統上始終顯示 */",
          "#", ns("position_table"), " .dataTables_scrollBody::-webkit-scrollbar {",
          "  -webkit-appearance: none !important;",  # 防止系統自動隱藏
          "  height: auto !important;",  # 使用預設高度
          "}",
          "#", ns("position_table"), " .dataTables_scrollBody::-webkit-scrollbar:horizontal {",
          "  height: auto !important;",
          "}",
          "#", ns("position_table"), " .dataTables_scrollBody {",
          "  scrollbar-width: auto !important;",  # Firefox
          "  -ms-overflow-style: auto !important;",  # IE/Edge
          "}"
        ))),
        DTOutput(ns("position_table"), width = "100%"))
  )
}

# Server ----------------------------------------------------------------------
#' positionTableServer
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param app_data_connection Database connection object or list. Any connection type supported by tbl2.
#'        Can be a DBI connection, a list with getter functions, a file path, or NULL if no database access is needed.
#' @param config List or reactive expression. Optional configuration settings that can customize behavior.
#'        If reactive, will be re-evaluated when dependencies change.
#' @param session Shiny session object. The current Shiny session (defaults to getDefaultReactiveDomain()).
#' @return list. A list of reactive values providing access to component state and data.
positionTableServer <- function(id, app_data_connection = NULL, config = NULL,
                                session = getDefaultReactiveDomain()) {
  moduleServer(id, function(input, output, session) {
    
    # ------------ Status tracking ----------------------------------
    #' Reactive value for tracking component status
    #' @description Tracks the current status of the component (loading, ready, etc.)
    #' @return Character string. One of "idle", "loading", "ready", "computing", or "error"
    component_status <- reactiveVal("idle")
    
    # ------------ Extract configuration parameters -----------------
    #' Extract platform_id from configuration
    #' 
    #' Parses the provided configuration (reactive or static) to extract platform_id.
    #' Handles both direct platform_id and nested platform_id in filters.
    #' Platform IDs are maintained as character strings for consistency with production environment.
    #' 
    #' @return Character or NULL. The extracted platform_id or NULL if not found.
    platform_id <- reactive({
      tryCatch({
        # Safely handle all config types
        if (is.null(config)) {
          # Handle null config
          return(NULL)
        } else if (is.function(config)) {
          # Only try to call a reactive function if we're in a reactive context
          if (shiny::is.reactivevalues(config) || 
              shiny::is.reactive(config) || 
              "reactive" %in% class(config)) {
            # Get config value safely (in reactive context)
            cfg <- config()
          } else {
            # Non-reactive function
            cfg <- config
          }
        } else {
          # Static config (list or other value)
          cfg <- config
        }
        
        # Extract platform_id from config if available
        if (!is.null(cfg)) {
          # Check for direct platform_id
          if (!is.null(cfg[["platform_id"]])) {
            # Ensure platform_id is a character string
            return(as.character(cfg[["platform_id"]]))
          }
          # Check for nested platform_id in filters
          if (!is.null(cfg[["filters"]]) && !is.null(cfg[["filters"]][["platform_id"]])) {
            # Ensure platform_id is a character string
            return(as.character(cfg[["filters"]][["platform_id"]]))
          }
          # Check for product_line_id_chosen (DM_R058 v2)
          if (!is.null(cfg[["filters"]]) && !is.null(cfg[["filters"]][["product_line_id_chosen"]])) {
            return(NULL)  # We're handling it separately
          }
        }
        
        # Return NULL if no platform_id found
        NULL
      }, error = function(e) {
        warning("Error extracting platform_id from config: ", e$message)
        NULL
      })
    })
    
    #' Extract product_line_id_chosen from configuration (DM_R058 v2)
    #'
    #' Similar to platform_id extraction, but for product_line_id_chosen.
    #'
    #' @return Character or NULL. The extracted product_line_id_chosen or NULL if not found.
    product_line_id <- reactive({
      tryCatch({
        # Safely handle all config types
        if (is.null(config)) {
          # Handle null config
          return("all")
        } else if (is.function(config)) {
          # Only try to call a reactive function if we're in a reactive context
          if (shiny::is.reactivevalues(config) || 
              shiny::is.reactive(config) || 
              "reactive" %in% class(config)) {
            # Get config value safely (in reactive context)
            cfg <- config()
          } else {
            # Non-reactive function
            cfg <- config
          }
        } else {
          # Static config (list or other value)
          cfg <- config
        }
        
        # Extract product_line_id_chosen from config (DM_R058 v2: three-layer naming)
        if (!is.null(cfg)) {
          if (!is.null(cfg[["filters"]]) && !is.null(cfg[["filters"]][["product_line_id_chosen"]])) {
            return(as.character(cfg[["filters"]][["product_line_id_chosen"]]))
          }
        }
        
        # Return "all" if no product_line_id found
        "all"
      }, error = function(e) {
        warning("Error extracting product_line_id: ", e$message)
        "all"
      })
    })
    
    # ------------ Data access (R116) -----------------------------------
    #' Reactive data accessor for position data
    #' 
    #' Retrieves position data from the database or data source using the Enhanced Data Access
    #' Pattern (R116). Applies platform and product line filtering if specified in the configuration.
    #' 
    #' @return data.frame. A reactive data frame containing position metrics.
    position_data <- reactive({
      # Return empty data when product_line is "all" - different product lines cannot be compared
      prod_line <- product_line_id()
      # MP035: Safe comparison to handle potential NA values
      if (is.null(prod_line) || is.na(prod_line) || prod_line == "all") {
        component_status("idle")
        return(data.frame())
      }
      # Update component status to show data loading
      component_status("loading")
      
      # Get product line ID from config (platform filtering disabled for now)
      prod_line <- product_line_id()
      message("DEBUG: position_data reactive called with product_line: ", prod_line)
      
      # Safely access data with proper error handling
      result <- tryCatch({
        # First check if we have a valid connection
        if (is.null(app_data_connection)) {
          warning("No valid database connection available")
          return(data.frame())
        }
        
        # Use complete case function to get position data with type filtering
        filtered_data <- fn_get_position_complete_case(
          app_data_connection = app_data_connection,
          product_line_id = prod_line,
          include_special_rows = TRUE,
          apply_type_filter = TRUE
        )
        
        # Check if product_id column exists, if not try to find platform-specific column
        if (!"product_id" %in% colnames(filtered_data) && nrow2(filtered_data) > 0) {
          platform <- platform_id()

          # Ensure platform is a scalar value for switch statement
          if (is.null(platform) || length(platform) == 0) {
            platform <- "default"
          } else if (length(platform) > 1) {
            warning("platform_id() returned multiple values, using first: ", paste(platform, collapse=", "))
            platform <- as.character(platform[1])
          } else {
            platform <- as.character(platform)
          }

          item_col <- switch(platform,
            "amz" = "asin",  # Amazon
            "eby" = "ebay_item_number",  # eBay
            "product_id"  # Default fallback
          )
          
          if (item_col %in% colnames(filtered_data)) {
            message("DEBUG: Renaming '", item_col, "' to 'product_id' in positionTable")
            filtered_data <- filtered_data %>% dplyr::rename(product_id = !!sym(item_col))
          } else {
            warning("No product identifier column found in position data. Available columns: ", paste(colnames(filtered_data), collapse = ", "))
          }
        }
        
        # Update component status
        component_status("ready")
        
        # Return the filtered data
        return(filtered_data)
      }, error = function(e) {
        warning("Error fetching position data: ", e$message)
        component_status("error")
        data.frame() # Return empty data frame on error
      })
      
      # Return the result
      return(result)
    })
    
    # ------------ Filter Options -----------------------------------------
    # Update brand filter choices when position data changes
    observe({
      # Get the full dataset (already filtered by product_line_id)
      data <- position_data()
      
      # Handle empty data
      if (is.null(data) || nrow2(data) == 0) {
        updateSelectizeInput(session, "brand",
                          choices = c("No brands available" = ""),
                          selected = character(0))
        return()
      }
      
      # Update component status with explanation if product_line is "all"
      prod_line_check <- product_line_id()
      # MP035: Safe comparison to handle potential NA values
      if (is.null(prod_line_check) || is.na(prod_line_check) || prod_line_check == "all") {
        component_status("idle")
        output$component_status <- renderText({
          "Please select a specific product line to view position data"
        })
      }
      
      # Apply product_line_id filter to prevent cross-contamination
      prod_line <- product_line_id()
      # MP035: Safe comparison to handle potential NA values
      if (!is.null(prod_line) && !is.na(prod_line) && prod_line != "all") {
        data <- data %>% dplyr::filter(product_line_id == prod_line)
      }
      
      # Get unique brands within the current product line, excluding "Ideal", "Rating", and "Revenue"
      brands <- data %>%
        dplyr::filter(!brand %in% c("Ideal", "Rating", "Revenue")) %>%
        dplyr::pull(brand) %>%
        as.character() %>%  # Ensure character conversion
        .[!is.na(.)] %>%    # Remove NA values
        unique() %>%
        sort()
      
      # Handle empty brands case
      if (length(brands) == 0) {
        brands <- character(0)
      }
      
      # Maintain current selections if possible (within current product line)
      selected <- intersect(input$brand, brands)
      
      # Update the selectize input
      updateSelectizeInput(session, "brand",
                        choices = brands,
                        selected = selected)
    })
    
    # Update Item ID filter choices when position data changes or brand selection changes
    observe({
      # Get the full dataset (already filtered by product_line_id)
      data <- position_data()
      
      # Handle empty data
      if (is.null(data) || nrow2(data) == 0) {
        updateSelectizeInput(session, "product_id",
                          choices = c("No Item IDs available" = ""),
                          selected = character(0))
        return()
      }
      
      # Apply product_line_id filter to prevent cross-contamination
      prod_line <- product_line_id()
      # MP035: Safe comparison to handle potential NA values
      if (!is.null(prod_line) && !is.na(prod_line) && prod_line != "all") {
        data <- data %>% dplyr::filter(product_line_id == prod_line)
      }
      
      # Filter by selected brands if any (within the current product line)
      if (length(input$brand) > 0) {
        data <- data %>% dplyr::filter(brand %in% input$brand)
      }
      
      # Get unique Product IDs within the current product line, excluding "Ideal", "Rating", and "Revenue"
      item_ids <- data %>%
        dplyr::filter(!product_id %in% c("Ideal", "Rating", "Revenue")) %>%
        dplyr::pull(product_id) %>%
        as.character() %>%  # Ensure character conversion
        .[!is.na(.)] %>%    # Remove NA values
        unique() %>%
        sort()
      
      # Handle empty product_ids case
      if (length(item_ids) == 0) {
        item_ids <- character(0)
      }
      
      # Maintain current selections if possible (within current product line)
      selected <- intersect(input$product_id, item_ids)
      
      # Update the selectize input
      updateSelectizeInput(session, "product_id",
                        choices = item_ids,
                        selected = selected)
    })
    
    # ------------ Filtered Data ----------------------------------------
    # Reactive expression for currently filtered data
    filtered_data <- reactive({
      # Get the base data (already filtered by product_line_id)
      data <- position_data()
      
      # Apply product_line_id filter to prevent cross-contamination
      prod_line <- product_line_id()
      # MP035: Safe comparison to handle potential NA values
      if (!is.null(prod_line) && !is.na(prod_line) && prod_line != "all") {
        data <- data %>% dplyr::filter(product_line_id == prod_line)
      }
      
      # Apply brand filter if selected (within the product line)
      if (length(input$brand) > 0) {
        data <- data %>% dplyr::filter(brand %in% input$brand)
      }
      
      # Apply item ID filter if selected (within the product line)
      if (length(input$product_id) > 0) {
        data <- data %>% dplyr::filter(product_id %in% input$product_id)
      }
      
      # Return the filtered data
      return(data)
    })
    
    # Extract the ideal row for footer display
    ideal_data <- reactive({
      data <- position_data()
      if (is.null(data) || nrow2(data) == 0) return(NULL)
      
      # Get the ideal row
      ideal <- data %>% dplyr::filter(product_id == "Ideal")
      if (nrow2(ideal) == 0) return(NULL)
      
      return(ideal)
    })
    
    # ------------ Filter logic ------------------------------------
    # Update Item ID label based on platform
    observe({
      # Source the helper function if not available
      if (!exists("get_product_id_label")) {
        source_path <- file.path("update_scripts", "global_scripts", "04_utils", "fn_get_product_id_label.R")
        if (file.exists(source_path)) {
          source(source_path)
        }
      }
      
      # Get current platform
      current_platform <- platform_id()
      
      # Update the label
      if (exists("get_product_id_label")) {
        label <- get_product_id_label(current_platform, translate = translate)
        updateSelectizeInput(session, "product_id", label = label)
      }
    })
    
    # Reset filters when reset button is clicked
    observeEvent(input$reset_filters, {
      # Clear filter inputs
      updateSelectizeInput(session, "brand", selected = character(0))
      updateSelectizeInput(session, "product_id", selected = character(0))
      updateCheckboxInput(session, "show_ideal", value = TRUE)
      
      # Log reset action
      message("Filters reset - all brands will be displayed")
    })
    
    # ------------ Table Rendering --------------------------------------
    # Render the position table
    output$position_table <- renderDT({
      # Get the base data
      data <- position_data()
      if (is.null(data) || nrow2(data) == 0) {
        return(data.frame(Message = "No data available"))
      }
      
      # Apply product_line_id filter in table display to prevent cross-contamination
      prod_line <- product_line_id()
      # MP035: Safe comparison to handle potential NA values
      if (!is.null(prod_line) && !is.na(prod_line) && prod_line != "all") {
        message("Applying product_line filter: ", prod_line)
        data <- data %>% dplyr::filter(product_line_id == prod_line)
        message("After product_line filter: ", nrow(data), " rows")
      }
      
      # Apply brand filter directly from input if selected
      if (length(input$brand) > 0) {
        message("Applying brand filter: ", paste(input$brand, collapse = ", "))
        data <- data %>% dplyr::filter(brand %in% input$brand)
        message("After brand filter: ", nrow(data), " rows")
      }
      
      # Apply Item ID filter directly from input if selected
      if (length(input$product_id) > 0) {
        message("Applying Item ID filter: ", paste(input$product_id, collapse = ", "))
        data <- data %>% dplyr::filter(product_id %in% input$product_id)
        message("After Item ID filter: ", nrow(data), " rows")
      }
      
      # First check that we have brand column in the data
      message("Column names before special rows filter: ", paste(colnames(data), collapse = ", "))
      
      # Filter out special rows if needed
      data <- data %>% 
        dplyr::filter(!product_id %in% c("Rating", "Revenue"))
        
      # Use conditional logic rather than vectorized && operation
      # Show or hide ideal rows based on checkbox input
      if (!input$show_ideal) {
        data <- data %>% dplyr::filter(product_id != "Ideal")
      }
      
      # Format numeric columns
      data <- data %>%
        dplyr::mutate_if(is.numeric, round, digits = 1)
      
      # Store the ideal row for later processing after NA filtering
      ideal_row_data <- NULL
      if (input$show_ideal) {
        # Get the ideal row from the original dataset, not the filtered data
        # This ensures the ideal values are always available regardless of filters
        original_data <- position_data()
        ideal_row_data <- original_data %>% dplyr::filter(product_id == "Ideal")
        
        if (nrow2(ideal_row_data) > 0) {
          message("Found Ideal row in original data")
          
          # Remove ideal from main data to avoid duplication
          data <- data %>% dplyr::filter(product_id != "Ideal")
        } else {
          message("No Ideal row found in original data")
          ideal_row_data <- NULL
        }
      }
      
      # 簡單篩選邏輯：先移除空行，再移除空列
      data <- simple_filter_position_table(data)
      
      # Apply column renaming through the translate function
      data <- data %>%
        dplyr::rename_with(translate, .cols = everything())
      
      # Remove product_line_id from display (if still present after filtering)
      if ("product_line_id" %in% colnames(data)) {
        data <- data %>% dplyr::select(-product_line_id)
      }
      
      # Move key columns to front and arrange by brand name as requested
      brand_name <- input$brand[1]  # Use first selected brand as primary brand
      
      # Safe relocation - ensure product_id and brand are in the data
      key_cols <- intersect(c("product_id", "brand"), colnames(data))
      if (length(key_cols) > 0) {
        data <- data %>% dplyr::relocate(dplyr::all_of(key_cols), .before = everything())
      }
      
      # Safe relocation of rating, sales, and Ideal-related columns (if they exist)
      # First identify columns related to Ideal points
      ideal_cols <- grep("^Ideal|^ideal|理想|最佳", colnames(data), value = TRUE)
      
      # Combine with rating and sales
      end_cols <- unique(c(intersect(c("rating", "sales"), colnames(data)), ideal_cols))
      
      if (length(end_cols) > 0) {
        message("Relocating columns to the end: ", paste(end_cols, collapse = ", "))
        data <- data %>% dplyr::relocate(dplyr::all_of(end_cols), .after = dplyr::last_col())
      }
      
      # Don't apply input filters directly, as we're using the current_filters reactive value
      # The filters are already applied above based on the current_filters state
        
      # Apply brand-based sorting if brand exists in the data
      if (!is.null(brand_name) && nchar(brand_name) > 0 && "brand" %in% colnames(data)) {
        message("Sorting by brand: ", brand_name)
        # Just move the specified brand to the top, don't filter out other brands
        data <- data %>% dplyr::arrange(dplyr::desc(brand == brand_name))
      } else {
        message("No brand-based sorting applied. Brand name: ", brand_name, 
                ", brand in columns: ", "brand" %in% colnames(data))
      }
      
      # 簡單篩選完成後的日誌輸出已包含在 simple_filter_position_table 函數中
      
      # Create DT options matching the requested settings
      options <- list(
        dom = 'Bfrtip',  # 顯示下載按鈕
        buttons = list(
          list(
            extend = 'excel',  # 設定按鈕類型為 Excel
            text = translate('品牌定位資料'),  # 設定按鈕上顯示的文字
            filename = translate('品牌定位資料')  # 設定下載的檔案名稱
          )
        ),
        pageLength = -1,  # 展示所有行
        searching = FALSE,  # 禁用搜尋框
        lengthChange = FALSE,  # 不顯示「Show [number] entries」選項
        info = FALSE,  # 不顯示「Showing 1 to X of X entries」信息
        paging = FALSE,  # 禁用分頁
        scrollX = TRUE,
        scrollY = FALSE,  # 不限制垂直高度
        fixedColumns = list(leftColumns = 2),  # 固定前兩欄（product_id, brand）
        autoWidth = FALSE,  # 防止自動調整欄位寬度
        columnDefs = list(
          list(width = '100px', targets = 0),  # Item ID 欄位寬度
          list(width = '120px', targets = 1),  # Brand 欄位寬度
          list(width = '80px', targets = '_all')  # 其他欄位預設寬度
        ),
        initComplete = JS("function(settings, json) {
          // 強制水平滾動條始終可見
          var wrapper = $(this.api().table().container());
          var scrollBody = wrapper.find('.dataTables_scrollBody');
          var scrollHead = wrapper.find('.dataTables_scrollHead');
          
          // 設置滾動容器樣式
          scrollBody.css({
            'overflow-x': 'scroll',  // 水平滾動條始終顯示
            'overflow-y': 'auto',    // 垂直滾動條自動顯示
            '-webkit-overflow-scrolling': 'touch'
          });
          
          // 確保表格寬度適中，不要太寬
          var table = scrollBody.find('table');
          var containerWidth = scrollBody.width();
          var tableWidth = table.width();
          
          // 只有當表格寬度小於容器寬度時，才稍微增加寬度以顯示滾動條
          if (tableWidth <= containerWidth) {
            // 設置較小的額外寬度，僅足以顯示滾動條
            table.css('min-width', (containerWidth + 50) + 'px');
          }
          
          // 強制重新計算滾動區域
          this.api().columns.adjust();
        }")
      )
      
      # Create the DT table with container and settings matching the request
      # Check if we need to create a footer with Ideal row
      container <- NULL
      
      # Get the column names from the data after NA filtering
      # MP035: Use colnames() instead of names() for better compatibility
      displayed_cols <- colnames(data)
      original_data <- position_data()
      
      # Process Ideal row if needed - now using the stored ideal_row_data
      if (!is.null(ideal_row_data) && input$show_ideal && nrow2(ideal_row_data) > 0) {
        # Build footer that exactly matches displayed columns
        aligned_ideal_footer <- character(length(displayed_cols))
        
        # Process each displayed column
        for (i in seq_along(displayed_cols)) {
          col <- displayed_cols[i]
          
          if (col == "product_id") {
            aligned_ideal_footer[i] <- ""  # Empty for product_id column
          } else if (col == "brand") {
            aligned_ideal_footer[i] <- "Ideal"  # "Ideal" for brand column
          } else if (col %in% colnames(ideal_row_data)) {
            # Get value from ideal row
            val <- ideal_row_data[[col]][1]
            if (is.numeric(val) && !is.na(val)) {
              aligned_ideal_footer[i] <- as.character(round(val, digits = 1))
            } else if (!is.na(val)) {
              aligned_ideal_footer[i] <- as.character(val)
            } else {
              aligned_ideal_footer[i] <- ""
            }
          } else {
            aligned_ideal_footer[i] <- ""  # Empty for missing columns
          }
        }
        
        # Verify footer length matches column count
        if (length(aligned_ideal_footer) == length(displayed_cols)) {
          # Create container with footer
          container <- htmltools::tags$table(
            tableHeader(displayed_cols),
            tableFooter(aligned_ideal_footer)
          )
          
          message("Added Ideal footer with ", length(aligned_ideal_footer), " elements for ", 
                  length(displayed_cols), " columns")
        } else {
          message("ERROR: Footer length mismatch - ", length(aligned_ideal_footer), 
                  " footer elements for ", length(displayed_cols), " columns")
          container <- NULL
        }
      }
      
      # Create table with or without footer
      if (!is.null(container)) {
        # Return the formatted table with container
        datatable(data, 
                  options = options,
                  container = container,
                  extensions = c("Buttons", "FixedHeader", "FixedColumns"),
                  rownames = FALSE)
      } else {
        # Create table without footer
        datatable(data, 
                  options = options,
                  extensions = c("Buttons", "FixedHeader", "FixedColumns"),
                  rownames = FALSE)
      }
    })
    
    # Display component status
    output$component_status <- renderText({
      # Special message when product_line is "all"
      if (product_line_id() == "all") {
        return("Please select a specific product line to view position data. Different product lines cannot be compared directly.")
      }

      # MP031: Defensive programming - check for NULL/empty values before switch
      # R113: Error handling for reactive expressions
      status_val <- tryCatch({
        component_status()
      }, error = function(e) {
        warning("Error getting component status: ", e$message)
        "idle"
      })

      # MP099: Defensive check for NULL or empty status
      if (is.null(status_val) || length(status_val) == 0 || status_val == "") {
        return("Ready to filter data")
      }

      # Ensure status_val is character and length 1 for switch
      status_val <- as.character(status_val)[1]

      # Standard status messages
      switch(status_val,
             idle = "Ready to filter data",
             loading = "Loading data...",
             ready = paste0("Data loaded with ", nrow2(position_data()), " records"),
             computing = "Computing results...",
             error = "Error loading data",
             status_val)  # Default: return the status value itself
    })
    
    # Return reactive values for external use
    return(list(
      position_data = position_data,
      filtered_data = filtered_data,
      component_status = component_status
    ))
  })
}

# Component wrapper -----------------------------------------------------------
#' positionTableComponent
#' 
#' Implements a visualization component for position table analysis
#' following the Connected Component principle.
#' 
#' @param id Character string. The module ID used for namespacing inputs and outputs.
#' @param app_data_connection Database connection object or list. The data connection supporting Enhanced Data Access pattern (R116).
#'        Can be a DBI connection, a list with getter functions, a file path, or NULL if no database access is needed.
#' @param config List or reactive expression. Configuration parameters for customizing component behavior (optional).
#'        If reactive, will be re-evaluated when dependencies change.
#' @param translate Function. Translation function for UI text elements (defaults to identity function).
#'        Should accept a string and return a translated string.
#' @return A list containing UI and server functions structured according to the Connected Component Principle (MP56).
#'         The UI element contains 'filter' and 'display' components, and the server function initializes component functionality.
#' @examples
#' # Basic usage with default settings
#' positionComp <- positionTableComponent("position_table")
#' 
#' # Usage with database connection
#' positionComp <- positionTableComponent(
#'   id = "position_table",
#'   app_data_connection = app_conn, 
#'   config = list(platform_id = "amz")  # Using character ID for Amazon platform
#' )
#'
#' # Usage with reactive configuration
#' positionComp <- positionTableComponent(
#'   id = "position_table",
#'   app_data_connection = app_conn,
#'   config = reactive({ list(filters = list(platform_id = input$platform)) })
#' )
#' @export
positionTableComponent <- function(id, app_data_connection = NULL, config = NULL, translate = identity) {
  list(
    ui = list(filter = positionTableFilterUI(id, translate),
              display = positionTableDisplayUI(id, translate)),
    server = function(input, output, session) {
      positionTableServer(id, app_data_connection, config, session)
    }
  )
}
