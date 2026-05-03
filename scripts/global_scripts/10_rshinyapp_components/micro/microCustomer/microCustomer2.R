#' @principle P15 Debug Efficiency Exception
#' @principle P76 Error Handling Patterns
#' @principle P77 Performance Optimization
#' @principle P80 Integer ID Consistency
#' @principle P81 Tidyverse-Shiny Terminology Alignment
#' @principle R76 Module Data Connection Rule
#' @principle R88 Shiny Module ID Handling
#' @principle R89 Integer ID Type Conversion
#' @principle R90 ID Relationship Validation
#' @principle MP52 Unidirectional Data Flow
#' @principle MP53 Feedback Loop
#' @principle MP54 UI-Server Correspondence
#' @principle R116 Enhanced Data Access with tbl2
#' @r21_exception This file contains the UI-server-defaults triple for microCustomer module with Enhanced Data Access (tbl2)
#' @justification These components are frequently debugged together during UI development and data integration
#' @refactor_plan To be refactored back into separate files once micro-level component design is finalized (est. Q3 2025)

# Define the string concatenation operator if it doesn't exist
if (!exists("%+%")) {
  `%+%` <- function(x, y) paste0(x, y)
}

# Define NULL coalescing operator if it doesn't exist
if (!exists("%||%")) {
  `%||%` <- function(x, y) if (is.null(x)) y else x
}

# NSQL Data Flow Documentation
# DATA_FLOW(component: customer_filter) {
#   SOURCE: app_data_connection
#   INITIALIZE: {
#     EXTRACT(app_data_connection → GET dna_data → dna_data)
#     EXTRACT(app_data_connection → GET customer_profiles → profiles)
#     EXTRACT(dna_data → DISTINCT customer_id → valid_ids)
#     FILTER(profiles → WHERE customer_id IN valid_ids → dropdown_options)
#   }
#   ON_SELECT: {
#     value = customer_filter.selected
#     FILTER(profiles → WHERE customer_id = as.integer(value) → customer_detail)
#     FILTER(dna_data → WHERE customer_id = as.integer(value) → customer_metrics)
#     JOIN(customer_detail → WITH customer_metrics ON customer_id → display_data)
#   }
# }

# Safe nrow: returns 0 on error, NULL, or NA
nrow2 <- function(x) {
  n <- tryCatch(NROW(x), error = function(e) NA)
  if (is.null(n) || length(n) == 0 || is.na(n)) 0L else as.integer(n)
}

####microCustomerDefaults####

#' Default Values for Micro Customer Component
#'
#' This function provides standard default values for the micro customer component.
#' These defaults ensure that all UI outputs have appropriate values even when
#' data is unavailable or invalid, implementing the UI-Server Pairing Rule.
#'
#' @return Named list of output IDs and their default values
#' @export
microCustomerDefaults <- function() {
  list(
    # Customer history metrics
    dna_time_first = "N/A",
    dna_time_first_to_now = "N/A",
    
    # RFM metrics
    dna_r_label = "N/A",
    dna_r_value = "N/A",
    dna_f_label = "N/A",
    dna_f_value = "N/A",
    dna_m_label = "N/A",
    dna_m_value = "N/A",
    
    # Customer activity metrics
    dna_cai_label = "N/A",
    dna_cai = "N/A",
    dna_ipt_mean = "N/A",
    
    # Value metrics
    dna_pcv = "N/A",
    dna_clv = "N/A",
    dna_cri = "N/A",
    
    # Prediction metrics
    dna_nrec = "N/A",
    dna_nrec_prob = "N/A",
    
    # Status metrics
    dna_nes_status = "N/A",
    
    # Transaction metrics
    dna_nt = "N/A",
    dna_e0t = "N/A"
  )
}


####microCustomerFilterUI####

#' Micro Customer Filter UI Component
#'
#' This component provides a filter interface for selecting customers
#' to be displayed in the micro-level customer analysis view.
#' Follows P75: Search Input Optimization pattern and P81: Tidyverse-Shiny Terminology Alignment.
#'
#' @param id The module ID
#' @param translate 翻譯函數，默認為identity函數
#'
#' @return A UI component for filtering customers
#' @export
microCustomerFilterUI <- function(id, translate = function(x) x) {
  ns <- NS(id)
  
  tagList(
    tags$head(
      tags$style(HTML("
          .selectize-input, .selectize-input input {
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
            padding-right: 20px !important;
          }

      "))
    ),
    div(
      class = "customer-filter-container",
      style = "padding: 15px; background-color: #f8f9fa; border-radius: 5px; margin-bottom: 20px;",
      fluidRow(
        column(
          width = 12,
          #h4(translate("Customer DNA"), style = "margin-bottom: 15px;"),
          # 改用不帶 server=TRUE 的 selectizeInput，但使用 NULL choices 為後續更新做準備
          shiny::selectizeInput(
            inputId = ns("customer_filter"), # P81: filter aligns with dplyr::filter
            label = translate("Select Customer:"),
            choices = NULL, # 初始為空，將由服務器填充
            selected = NULL,
            options = list(
              placeholder = translate("開始輸入以搜尋客戶..."),
              onInitialize = I('function() { this.setValue(""); }'),
              maxOptions = 50 # 限制顯示結果數量
            )
          ),
          div(
            style = "margin-top: 10px; width: 200px; margin-left: 10px;",
            actionButton(
              inputId = ns("clear_filter"),
              label = translate("清除篩選"),
              icon = icon("times"),
              class = "btn-outline-secondary",
              width = "100%"
            )
          )
        )
      )
    )
  )
}


####microCustomerDisplayUI####

#' Micro Customer Display UI Component
#'
#' This component provides the UI elements for displaying detailed customer analytics
#' in the micro-level view of the application.
#'
#' IMPORTANT: According to the UI-Server Pairing Rule, this UI component MUST be used with
#' its corresponding server component microCustomerServer(). All outputs defined here
#' must be fulfilled by the server component to avoid broken displays.
#'
#' @param id The module ID
#'
#' @return A UI component
#' @export
microCustomerDisplayUI <- function(id) {
  ns <- NS(id)
  
  bslib::nav_panel(
    title = "micro",
    # 使用基本的shiny元素替代bs4Dash專有元素
    div(
      class = "card",
      style = "width: 100%; margin-bottom: 20px;",
      div(
        class = "card-body",
        # Header: Customer Name and Email
        fluidRow(
          column(12,
                 div(
                   class = "customer-profile-header",
                   style = "margin-bottom: 15px; padding-bottom: 15px; border-bottom: 1px solid #eee; text-align: center;",
                   div(
                     style = "font-size: 1.4rem; font-weight: 600;",
                     htmlOutput(ns("customer_name"))
                   ),
                   div(
                     style = "font-size: 1rem; color: #666;",
                     htmlOutput(ns("customer_email"))
                   )
                 )
          )
        ),
        # Row 1: 顧客資歷、最近購買日(R)、購買頻率(F)
        fluidRow(
          column(4, bs4Dash::valueBoxOutput(ns("dna_time_first"), width = 12)),
          column(4, bs4Dash::valueBoxOutput(ns("dna_recency"), width = 12)),
          column(4, bs4Dash::valueBoxOutput(ns("dna_frequency"), width = 12))
        ),
        # Row 2: 購買金額(M)、顧客活躍度(CAI)、顧客平均購買週期(IPT)
        fluidRow(
          column(4, bs4Dash::valueBoxOutput(ns("dna_monetary"), width = 12)),
          column(4, bs4Dash::valueBoxOutput(ns("dna_cai"), width = 12)),
          column(4, bs4Dash::valueBoxOutput(ns("dna_ipt"), width = 12))
        ),
        # Row 3: 過去價值(PCV)、顧客終身價值(CLV)、顧客交易穩定度 (CRI)
        fluidRow(
          column(4, bs4Dash::valueBoxOutput(ns("dna_pcv"), width = 12)),
          column(4, bs4Dash::valueBoxOutput(ns("dna_clv"), width = 12)),
          column(4, bs4Dash::valueBoxOutput(ns("dna_cri"), width = 12))
        ),
        # Row 4: 顧客狀態(NES)、新客單價、主力客單價
        fluidRow(
          column(4, bs4Dash::valueBoxOutput(ns("dna_nes"), width = 12)),
          column(4, bs4Dash::valueBoxOutput(ns("dna_nt"), width = 12)),
          column(4, bs4Dash::valueBoxOutput(ns("dna_e0t"), width = 12))
        )
      )
    )
  )
}

####microCustomerServer####

#' Unified Micro Customer Server Component with Enhanced Data Access
#'
#' This component integrates both filtering and display logic for the micro customer
#' analytics view. It handles customer search, filtering, and data visualization.
#' Implements R116 Enhanced Data Access with tbl2 for compatibility with any connection type.
#'
#' @param id The module ID
#' @param app_data_connection App data connection object providing access to data sources.
#'        This can be any of: DBI connection, list with data frames, list with functions,
#'        reactive expression, or direct data frame.
#' @param session 當前Shiny會話，用於registerDataObj
#'
#' @return Reactive expression that returns filtered data
#' @export
#' @implements R116 Enhanced Data Access with tbl2
microCustomerServer <- function(id, 
                                app_data_connection = NULL,
                                session = getDefaultReactiveDomain(),
                                config = NULL) {
  # 創建整合的模組伺服器
  moduleServer(id, function(input, output, session) {
    # Log module initialization
    cat("INFO: Initializing microCustomer module with ID:", id, "\n")
    
    # == 定義常量 ==
    textRlabel <- c("極近", "近期", "一般", "久遠", "非常久遠")
    textFlabel <- c("極低", "低", "一般", "高", "非常高")
    textMlabel <- c("極低", "低", "一般", "高", "非常高")
    textCAIlabel <- c("不活躍", "低度活躍", "一般活躍", "活躍", "非常活躍")
    
    # == 默認值設置 ==
    defaults <- microCustomerDefaults()
    
    # == 反應式資料來源 - 使用R91 Universal Data Access Pattern ==
    
    # Extract platform_id from config or input - safe implementation
    platform_id <- reactive({
      tryCatch({
        # First, try to get from config safely
        local_config <- NULL
        
        # Handle different config types properly
        if (is.function(config)) {
          # Only try to access reactives in reactive contexts
          if (shiny::is.reactivevalues(config) || 
              shiny::is.reactive(config) || 
              "reactive" %in% class(config)) {
            # Safe reactive access
            req(config())              # 若 config() 尚未就緒會自動阻斷
            local_config <- config()
          } else {
            # Function but not reactive
            local_config <- config()
          }
        } else {
          # Static config
          local_config <- config
        }
        
        # Extract value from config
        pid <- NULL
        if (!is.null(local_config) && is.list(local_config)) {
          if (!is.null(local_config[["filters"]]) && 
              !is.null(local_config[["filters"]][["platform_id"]])) {
            pid <- local_config[["filters"]][["platform_id"]]
          } else if (!is.null(local_config[["platform_id"]])) {
            pid <- local_config[["platform_id"]]
          }
        }
        
        # If not in config, try input
        if (is.null(pid) && !is.null(input[["platform"]])) {
          pid <- input[["platform"]]
        }
        
        # Return platform ID as character string - do not convert to integer
        # Platform IDs are now character strings like "amz", "eby", "all"
        pid %||% "all"
      }, error = function(e) {
        warning("Error extracting platform_id: ", e$message)
        # Default to "all" on any error (was previously 0L)
        "all"
      })
    })

    product_line_id_sliced <- reactive({
      tryCatch({
        local_config <- NULL

        if (is.function(config)) {
          if (shiny::is.reactivevalues(config) ||
              shiny::is.reactive(config) ||
              "reactive" %in% class(config)) {
            req(config())
            local_config <- config()
          } else {
            local_config <- config()
          }
        } else {
          local_config <- config
        }

        plid <- NULL
        if (!is.null(local_config) && is.list(local_config)) {
          if (!is.null(local_config[["filters"]]) &&
              !is.null(local_config[["filters"]][["product_line_id_sliced"]]) &&
              nzchar(as.character(local_config[["filters"]][["product_line_id_sliced"]])[1])) {
            plid <- as.character(local_config[["filters"]][["product_line_id_sliced"]])[1]
          } else if (!is.null(local_config[["filters"]]) &&
                     !is.null(local_config[["filters"]][["product_line_id_chosen"]]) &&
                     nzchar(as.character(local_config[["filters"]][["product_line_id_chosen"]])[1])) {
            # DM_R058 v2: fallback to product_line_id_chosen (UI selection)
            plid <- as.character(local_config[["filters"]][["product_line_id_chosen"]])[1]
          }
        }

        as.character(plid %||% "all")
      }, error = function(e) {
        warning("Error extracting product_line_id_sliced: ", e$message)
        "all"
      })
    })
    
    active_tab <- reactive({
      cfg <- if (is.function(config)) config() else config
      cfg$active_tab %||% "microCustomer"
    })
    
    # Ensure cached access helpers are available
    if (!exists("load_customer_dropdown_options_cached", mode = "function")) {
      source("scripts/global_scripts/04_utils/fn_cached_data_access.R")
    }

    # Define columns needed for this component
    dna_columns <- c(
      "customer_id", "platform_id",
      "time_first", "time_first_to_now",
      "r_label", "r_value", "f_label", "f_value", "m_label", "m_value",
      "cai_label", "cai", "ipt_mean", "pcv", "clv", "cri",
      "nes_status", "nt", "e0t",
      "product_line_id_filter"
    )
    prof_columns <- c("customer_id", "platform_id", "buyer_name", "email")

    # Load customer options with caching (limited columns)
    customer_options <- reactive({
      req(app_data_connection)

      if (!is.null(active_tab()) && active_tab() != "microCustomer") {
        return(tibble::tibble(customer_id = integer(), buyer_name = character(), email = character()))
      }

      plat <- platform_id()
      pl_filtered <- product_line_id_sliced()
      if (exists("load_customer_dropdown_options_cached", mode = "function")) {
        load_customer_dropdown_options_cached(
          app_data_connection,
          plat,
          pl_filtered,
          limit = 500
        )
      } else {
        tbl <- tbl2(app_data_connection, "df_profile_by_customer")
        dna_tbl <- tbl2(app_data_connection, "df_dna_by_customer") %>%
          dplyr::select(dplyr::any_of(c("customer_id", "platform_id", "product_line_id_filter")))

        if (!is.null(plat) && !is.na(plat) && plat != "all") {
          tbl <- dplyr::filter(tbl, platform_id == !!plat)
          dna_tbl <- dplyr::filter(dna_tbl, platform_id == !!plat)
        }
        if (!is.null(pl_filtered) && !is.na(pl_filtered) && pl_filtered != "all" &&
            "product_line_id_filter" %in% colnames(dna_tbl)) {
          dna_tbl <- dplyr::filter(dna_tbl, product_line_id_filter == !!pl_filtered)
        }
        # Use head() instead of slice_head() for PostgreSQL/Supabase compatibility
        tbl %>%
          dplyr::semi_join(dna_tbl, by = c("customer_id", "platform_id")) %>%
          dplyr::select(dplyr::any_of(prof_columns)) %>%
          head(n = 500) %>%
          dplyr::collect()
      }
    })

    # Keep df_prof for compatibility (limited columns)
    df_prof <- reactive({
      customer_options()
    })

    # Valid customer IDs from available options
    valid_ids <- reactive({ unique(as.integer(df_prof()[["customer_id"]])) })

    # Load single-customer detail data on demand (SQL-level filter)
    fetch_customer_detail <- function(sel_id) {
      req(app_data_connection)
      if (is.null(sel_id) || is.na(sel_id) || sel_id <= 0) return(NULL)

      plat <- platform_id()
      pl_filtered <- product_line_id_sliced()
      has_product_line_filter <- exists("table_has_column", mode = "function") &&
        table_has_column(app_data_connection, "df_dna_by_customer", "product_line_id_filter")

      dna_tbl <- tbl2(app_data_connection, "df_dna_by_customer") %>%
        dplyr::filter(customer_id == !!sel_id) %>%
        dplyr::select(dplyr::any_of(dna_columns))

      prof_tbl <- tbl2(app_data_connection, "df_profile_by_customer") %>%
        dplyr::filter(customer_id == !!sel_id) %>%
        dplyr::select(dplyr::any_of(prof_columns))

      if (!is.null(plat) && !is.na(plat) && plat != "all") {
        dna_tbl <- dplyr::filter(dna_tbl, platform_id == !!plat)
        prof_tbl <- dplyr::filter(prof_tbl, platform_id == !!plat)
      }

      if (has_product_line_filter &&
          !is.null(pl_filtered) &&
          !is.na(pl_filtered) &&
          pl_filtered != "all") {
        local_pl <- as.character(pl_filtered)
        dna_tbl <- dplyr::filter(dna_tbl, product_line_id_filter == !!local_pl)
      }

      dna <- dplyr::collect(dna_tbl)
      prof <- dplyr::collect(prof_tbl)

      if (nrow2(dna) == 0 && nrow2(prof) == 0) return(NULL)
      if (nrow2(dna) == 0) return(prof)
      if (nrow2(prof) == 0) return(dna)

      dplyr::left_join(dna, prof, by = "customer_id", suffix = c("", "_prof"))
    }
    
    # Make df_prof available to external components like microDNADistribution
    # This helps fix "could not find function df_prof" error
    exportTestValues(df_prof = df_prof)
    
    # Customer filter selection state tracking
    current_customer <- reactiveVal(NULL)
    
    # Populate selectize choices whenever profiles update
    observeEvent(
      {
        list(df_prof(), valid_ids())    # <- 依賴二者
      },
      {
        prof <- df_prof()
        ids  <- valid_ids()
        req(nrow2(prof) > 0, length(ids) > 0)   # 沒資料時暫停，等下一輪
        
        # --- 下面維持原本邏輯 ---
        # Note: prof is already collected, so head() works fine here
        choices <- prof %>%
          dplyr::filter(customer_id %in% ids) %>%
          head(n = 100) %>%
          dplyr::mutate(label = paste0(buyer_name, " (", email, ")")) %>%
          dplyr::select(customer_id, label)
        
        choice_list <- setNames(as.character(choices$customer_id), choices$label)

        # Auto-select first customer so TagPilot shows data immediately (#20)
        auto_selected <- if (length(choice_list) > 0) choice_list[[1]] else ""

        updateSelectizeInput(
          session, "customer_filter",
          choices  = c("Select a customer..." = "", choice_list),
          selected = auto_selected,
          server   = TRUE
        )
      },
      ignoreInit = FALSE   # 讓第一次也跑，但 req() 會擋掉空資料
    )
    
    # Debug observer for customer filter changes
    observeEvent(input[["customer_filter"]], {
      sel_val <- input[["customer_filter"]]
      cat("DEBUG: customer_filter changed to:", sel_val, "\n")
      
      # Update reactive customer tracking
      current_customer(sel_val)
      
      # Force reactivity by invalidating
      if (!is.null(sel_val) && nchar(sel_val) > 0) {
        sel_id <- as.integer(sel_val)
        cat("DEBUG: Looking for customer ID:", sel_id, "\n")
      }
    }, ignoreNULL = FALSE)
    
    # Clear filter handler with improved force-update logic
    observeEvent(input[["clear_filter"]], {
      timestamp <- format(Sys.time(), "%H:%M:%S.%OS")
      cat("DEBUG [", timestamp, "]: Clear filter button clicked\n")
      
      # Reset customer selection
      updateSelectizeInput(session, "customer_filter", selected = "")
      current_customer(NULL)
      
      # Additional debugging to verify the clear worked
      cat("DEBUG: After clear, customer_filter =", input[["customer_filter"]], 
          ", current_customer =", current_customer(), "\n")
      
      # Mark any cached data as out of date
      session$userData$cache_invalidated <- TRUE
      
      # Force all reactive expressions to update immediately
      # Use multiple methods to ensure reactivity propagates
      invalidateLater(100, session)
      
      # Directly trigger reactivity in box_data by sending a dummy value
      # This helps break dependencies that might be stuck
      session$sendCustomMessage("force_update", list(timestamp = timestamp))
    })
    
    # Reactive selected customer data with improved debugging and reactivity 
    selected_data <- reactive({
      # Force reactivity from multiple sources
      sel <- input[["customer_filter"]]
      cust <- current_customer()  # Add dependency on the reactive tracking

      # CRITICAL: This section has been heavily updated
      # Use the input value if available, otherwise use tracked value
      selection <- sel %||% cust
      cat("DEBUG: selected_data reactive running with customer:", selection, "\n")
      
      # Convert to integer and validate
      sel_id <- tryCatch(as.integer(selection), error = function(e) NA)
      if (is.na(sel_id) || length(sel_id) == 0 || sel_id <= 0) {
        cat("DEBUG: Invalid customer ID:", selection, "\n")
        return(NULL)
      }
      
      # Load data on demand for the selected customer
      cat("DEBUG: Fetching data for customer ID:", sel_id, "\n")
      tryCatch({
        result <- fetch_customer_detail(sel_id)
        
        # Verify we have a result
        if (is.null(result) || nrow2(result) == 0) {
          cat("DEBUG: No data returned for customer ID:", sel_id, "\n")
          return(NULL)
        }
        
        # Print columns for debugging
        cat("DEBUG: Result data has", nrow2(result), "rows with columns:", 
            paste0(names(result), collapse = ", "), "\n")
            
        # Manually ensure key columns have the expected simple names
        # This ensures the valueBox code can find them regardless of the join
        expected_columns <- c(
          "time_first", "time_first_to_now", 
          "r_label", "r_value", "f_label", "f_value", "m_label", "m_value",
          "cai_label", "cai", "ipt_mean", "pcv", "clv", "cri", 
          "nes_status", "nt", "e0t"
        )
        
        cat("DEBUG: Checking for expected columns\n")
        for (col in expected_columns) {
          # Try variations of the column name
          candidates <- c(
            col,
            paste0(col, "_dna"),
            paste0(col, "_prof")
          )
          
          # Find the first match
          found <- FALSE
          for (candidate in candidates) {
            if (candidate %in% names(result)) {
              # Only rename if we need to
              if (candidate != col) {
                cat("DEBUG: Remapping", candidate, "to", col, "\n")
                result[[col]] <- result[[candidate]]
              }
              found <- TRUE
              break
            }
          }
          
          # If column wasn't found at all, note it
          if (!found) {
            cat("DEBUG: Warning - Expected column", col, "not found in any variation\n")
          }
        }
        
        return(result)
      }, error = function(e) {
        cat("ERROR joining customer data:", e$message, "\n")
        NULL
      })
    })
    
    # == 顯示功能 - 準備資料以供UI顯示 ==
    
    # 選定的客戶資料 - 添加錯誤處理
    selected_customer_data <- reactive({
      data <- selected_data()
      if (is.null(data) || !is.data.frame(data) || nrow2(data) == 0) {
        return(NULL)
      }
      return(data)
    })
    
    # 安全值存取輔助函數 - 提高強健性
    safeValue <- function(data, field, default = NA) {
      # P76: 錯誤處理 - 更強健的值獲取函數
      if (is.null(data) || !is.data.frame(data) || nrow2(data) == 0 || !field %in% names(data)) {
        return(default)
      }
      value <- data[[field]][1]
      if (is.null(value) || is.na(value)) {
        return(default)
      }
      return(value)
    }
    
    # == UI輸出渲染 - 將資料呈現在UI中 ==
    
    # Check if filter resulted in empty data (platform or product line)
    is_empty_filter_result <- reactive({
      # Try to safely get valid customer IDs
      valid_ids <- tryCatch({
        valid_ids()
      }, error = function(e) {
        cat("ERROR in is_empty_filter_result:", e$message, "\n")
        integer(0)  # Return empty vector on error
      })

      # Get profiles data
      profiles <- tryCatch({
        df_prof()
      }, error = function(e) {
        NULL
      })

      # Get current filters
      plat <- platform_id()
      pl <- product_line_id_sliced()

      # Add more detailed logging
      cat("DEBUG: is_empty_filter_result check - platform_id:",
          if(is.null(plat)) "NULL" else plat,
          "- product_line:", if(is.null(pl)) "NULL" else pl,
          "- valid_ids count:", length(valid_ids),
          "- profiles:", if(is.null(profiles)) "NULL" else paste(nrow2(profiles), "rows"),
          "\n")

      # Check for empty data conditions
      no_data <- is.null(profiles) || length(valid_ids) == 0 || nrow2(profiles) == 0

      if (no_data) {
        # Empty due to product line filter (#20)
        if (!is.null(pl) && !is.na(pl) && pl != "all") {
          cat("NOTICE: No customers found for product line:", pl, "\n")
          return(TRUE)
        }
        # Empty due to platform filter
        if (!is.null(plat) && plat != "all") {
          cat("NOTICE: No customers found for platform ID:", plat, "\n")
          return(TRUE)
        }
      }

      # Default case - not empty
      return(FALSE)
    })
    
    # 顯示客戶名稱和電子郵件
    output$customer_name <- renderUI({
      # First check if we have empty results due to filtering
      if (is_empty_filter_result()) {
        plat <- platform_id()
        pl <- product_line_id_sliced()

        # Determine which filter caused the empty result (#20)
        if (!is.null(pl) && !is.na(pl) && pl != "all") {
          return(HTML(paste0(
            "<span style='font-size: 1.4rem; color: #666;'>",
            "No customer data for product line: ", pl,
            "</span>"
          )))
        }

        platform_name <- switch(plat,
                             "amz" = "Amazon",
                             "eby" = "eBay",
                             "ofw" = "Official Website",
                             "sho" = "Shopify",
                             "cyb" = "Cyberbiz",
                             paste("Platform", plat))
        return(HTML(paste0(
          "<span style='font-size: 1.4rem; color: #666;'>",
          "No customers found for ", platform_name, " platform",
          "</span>"
        )))
      }
      
      # MP54: UI-Server對應 - 確保UI元素有伺服器功能
      customer <- selected_customer_data()
      if (is.null(customer)) return(HTML("<span>未選擇客戶</span>"))
      
      # Find the name field
      name_field <- NULL
      if ("buyer_name" %in% colnames(customer)) {
        name_field <- "buyer_name"
      } else if ("name" %in% colnames(customer)) {
        name_field <- "name"
      } else if ("customer_name" %in% colnames(customer)) {
        name_field <- "customer_name"
      }
      
      if (is.null(name_field)) {
        return(HTML("<span>未知客戶</span>"))
      }
      
      name <- safeValue(customer, name_field, default = "未知客戶")
      HTML(paste0("<span>", name, "</span>"))
    })
    
    output$customer_email <- renderUI({
      # First check if we have empty results due to filtering
      if (is_empty_filter_result()) {
        return(HTML(
          "<span style='color: #666;'>No customer data available for this platform. Please try selecting a different platform.</span>"
        ))
      }
      
      # MP54: UI-Server對應 - 確保UI元素有伺服器功能
      customer <- selected_customer_data()
      if (is.null(customer)) return(HTML("<span>無電子郵件</span>"))
      
      # Find the email field
      email_field <- NULL
      if ("email" %in% colnames(customer)) {
        email_field <- "email"
      } else if ("customer_email" %in% colnames(customer)) {
        email_field <- "customer_email"
      }
      
      if (is.null(email_field)) {
        return(HTML("<span>無電子郵件</span>"))
      }
      
      email <- safeValue(customer, email_field, default = "無電子郵件")
      HTML(paste0("<span>", email, "</span>"))
    })
    
    # 渲染所有valueBoxes - 使用相同的模式
    output$dna_time_first <- bs4Dash::renderValueBox({
      customer <- selected_customer_data()
      
      if (is.null(customer)) {
        value <- defaults$dna_time_first
        subtitle <- "客戶資歷"
      } else {
        time_first <- safeValue(customer, "time_first", default = NA)
        time_first_to_now <- safeValue(customer, "time_first_to_now", default = NA)
        
        if (is.na(time_first)) {
          value <- defaults$dna_time_first
        } else {
          value <- format(time_first, "%Y-%m-%d")
        }
        
        if (is.na(time_first_to_now)) {
          subtitle <- paste("客戶資歷:", defaults$dna_time_first_to_now, "天")
        } else {
          subtitle <- paste("客戶資歷:", time_first_to_now, "天")
        }
      }
      
      bs4Dash::valueBox(
        value = value,
        subtitle = subtitle,
        icon = icon("calendar"),
        color = "primary"
      )
    })
    
    output$dna_recency <- bs4Dash::renderValueBox({
      customer <- selected_customer_data()
      
      if (is.null(customer)) {
        label <- defaults$dna_r_label
        value <- defaults$dna_r_value
      } else {
        rlabel <- safeValue(customer, "r_label", default = NA)
        rvalue <- safeValue(customer, "r_value", default = NA)
        
        if (is.na(rlabel) || (!is.factor(rlabel) && !is.character(rlabel))) {
          label <- defaults$dna_r_label
        } else {
          label <- as.character(rlabel)
        }
        
        if (is.na(rvalue)) {
          value <- defaults$dna_r_value
        } else {
          value <- rvalue
        }
      }
      
      # Safely convert value to numeric with error handling
      r_display_value <- tryCatch({
        if (is.character(value)) {
          # Try to convert only if it's a numeric string
          if (grepl("^[0-9.]+$", value)) {
            round(as.numeric(value), 2)
          } else {
            "N/A"  # Non-numeric string
          }
        } else if (is.numeric(value)) {
          round(value, 2)
        } else if (is.factor(value)) {
          # Handle factor values
          val_str <- as.character(value)
          if (grepl("^[0-9.]+$", val_str)) {
            round(as.numeric(val_str), 2)
          } else {
            val_str  # Use the factor string representation
          }
        } else {
          "N/A"  # Other non-convertible value
        }
      }, error = function(e) {
        cat("DEBUG: Error converting r_value to numeric:", e$message, "\n")
        "N/A"  # Return N/A if conversion fails
      })
      
      bs4Dash::valueBox(
        value = label,
        subtitle = paste("最近購買日(R):", r_display_value, "天"),
        icon = icon("clock"),
        color = "danger"
      )
    })
    
    output$dna_frequency <- bs4Dash::renderValueBox({
      customer <- selected_customer_data()
      
      if (is.null(customer)) {
        label <- defaults$dna_f_label
        value <- defaults$dna_f_value
      } else {
        flabel <- safeValue(customer, "f_label", default = NA)
        fvalue <- safeValue(customer, "f_value", default = NA)
        
        if (is.na(flabel) || (!is.factor(flabel) && !is.character(flabel))) {
          label <- defaults$dna_f_label
        } else {
          label <- as.character(flabel)
        }
        
        if (is.na(fvalue)) {
          value <- defaults$dna_f_value
        } else {
          value <- fvalue
        }
      }
      
      bs4Dash::valueBox(
        value = label,
        subtitle = paste("購買頻率(F):", value, "次"),
        icon = icon("chart-line"),
        color = "warning"
      )
    })
    
    output$dna_monetary <- bs4Dash::renderValueBox({
      customer <- selected_customer_data()
      
      if (is.null(customer)) {
        label <- defaults$dna_m_label
        value <- defaults$dna_m_value
      } else {
        mlabel <- safeValue(customer, "m_label", default = NA)
        mvalue <- safeValue(customer, "m_value", default = NA)
        
        if (is.na(mlabel) || (!is.factor(mlabel) && !is.character(mlabel))) {
          label <- defaults$dna_m_label
        } else {
          label <- as.character(mlabel)
        }
        
        if (is.na(mvalue)) {
          value <- defaults$dna_m_value
        } else {
          value <- round(mvalue, 2)
        }
      }
      
      bs4Dash::valueBox(
        value = label,
        subtitle = paste("購買金額(M):", value),
        icon = icon("dollar-sign"),
        color = "success"
      )
    })
    
    output$dna_cai <- bs4Dash::renderValueBox({
      customer <- selected_customer_data()
      
      if (is.null(customer)) {
        label <- defaults$dna_cai_label
        value <- defaults$dna_cai
      } else {
        cailabel <- safeValue(customer, "cai_label", default = NA)
        cai <- safeValue(customer, "cai", default = NA)
        
        if (is.na(cailabel) || (!is.factor(cailabel) && !is.character(cailabel))) {
          label <- defaults$dna_cai_label
        } else {
          label <- as.character(cailabel)
        }
        
        if (is.na(cai)) {
          value <- defaults$dna_cai
        } else {
          value <- round(cai, 2)
        }
      }
      
      bs4Dash::valueBox(
        value = label,
        subtitle = paste("顧客活躍度(CAI):", value),
        icon = icon("user-check"),
        color = "info"
      )
    })
    
    output$dna_ipt <- bs4Dash::renderValueBox({
      customer <- selected_customer_data()
      
      if (is.null(customer)) {
        value <- defaults$dna_ipt_mean
      } else {
        ipt_mean <- safeValue(customer, "ipt_mean", default = NA)
        
        if (is.na(ipt_mean)) {
          value <- defaults$dna_ipt_mean
        } else {
          value <- round(ipt_mean, 1)
        }
      }
      
      bs4Dash::valueBox(
        value = value,
        subtitle = "顧客平均購買週期(IPT)",
        icon = icon("calendar-check"),
        color = "secondary"
      )
    })
    
    output$dna_pcv <- bs4Dash::renderValueBox({
      customer <- selected_customer_data()
      
      if (is.null(customer)) {
        value <- defaults$dna_pcv
      } else {
        pcv <- safeValue(customer, "pcv", default = NA)
        
        if (is.na(pcv)) {
          value <- defaults$dna_pcv
        } else {
          value <- round(pcv, 2)
        }
      }
      
      bs4Dash::valueBox(
        value = value,
        subtitle = "過去價值(PCV)",
        icon = icon("history"),
        color = "primary"
      )
    })
    
    output$dna_clv <- bs4Dash::renderValueBox({
      customer <- selected_customer_data()
      
      if (is.null(customer)) {
        value <- defaults$dna_clv
      } else {
        clv <- safeValue(customer, "clv", default = NA)
        
        if (is.na(clv)) {
          value <- defaults$dna_clv
        } else {
          value <- round(clv, 2)
        }
      }
      
      bs4Dash::valueBox(
        value = value,
        subtitle = "顧客終身價值(CLV)",
        icon = icon("gem"),
        color = "success"
      )
    })
    
    output$dna_cri <- bs4Dash::renderValueBox({
      customer <- selected_customer_data()
      
      if (is.null(customer)) {
        value <- defaults$dna_cri
      } else {
        cri <- safeValue(customer, "cri", default = NA)
        
        if (is.na(cri)) {
          value <- defaults$dna_cri
        } else {
          value <- round(cri, 2)
        }
      }
      
      bs4Dash::valueBox(
        value = value,
        subtitle = "顧客交易穩定度(CRI)",
        icon = icon("balance-scale"),
        color = "warning"
      )
    })
    
    output$dna_nes <- bs4Dash::renderValueBox({
      customer <- selected_customer_data()
      
      if (is.null(customer)) {
        value <- defaults$dna_nes_status
      } else {
        nesstatus <- safeValue(customer, "nes_status", default = NA)
        
        if (is.na(nesstatus)) {
          value <- defaults$dna_nes_status
        } else {
          value <- as.character(nesstatus)
        }
      }
      
      bs4Dash::valueBox(
        value = value,
        subtitle = "顧客狀態(NES)",
        icon = icon("user-tag"),
        color = "danger"
      )
    })
    
    output$dna_nt <- bs4Dash::renderValueBox({
      customer <- selected_customer_data()
      
      if (is.null(customer)) {
        value <- defaults$dna_nt
      } else {
        nt <- safeValue(customer, "nt", default = NA)
        
        if (is.na(nt)) {
          value <- defaults$dna_nt
        } else {
          value <- round(nt, 2)
        }
      }
      
      bs4Dash::valueBox(
        value = value,
        subtitle = "新客單價",
        icon = icon("user-plus"),
        color = "info"
      )
    })
    
    output$dna_e0t <- bs4Dash::renderValueBox({
      customer <- selected_customer_data()
      
      if (is.null(customer)) {
        value <- defaults$dna_e0t
      } else {
        e0t <- safeValue(customer, "e0t", default = NA)
        
        if (is.na(e0t)) {
          value <- defaults$dna_e0t
        } else {
          value <- round(e0t, 2)
        }
      }
      
      bs4Dash::valueBox(
        value = value,
        subtitle = "主力客單價",
        icon = icon("star"),
        color = "secondary"
      )
    })
    
    
    # 返回篩選後的資料
    # Note: df_dna removed as it's no longer used in the on-demand loading design
    return(list(
      selected_data = selected_data,
      df_prof = df_prof
    ))
  })
}

####microCustomerComponent####

#' Create a Micro Customer component with Enhanced Data Access
#'
#' Integrates the filter and display components following R116 Enhanced Data Access Pattern.
#' This function returns a component that can be used as a whole or by individual parts 
#' within union components.
#'
#' @param id Module ID
#' @param app_data_connection App data connection (any supported connection type)
#' @param config Optional configuration parameters
#' @param translate Translation function, defaults to identity function
#'
#' @return A list containing UI elements and server function
#' @export
#' @principle MP56 Connected Component Principle

microCustomerComponent <- function(id, app_data_connection = NULL, config = NULL, translate = function(x) x) {
  # R88: 不使用自定义namespace后缀，防止双重命名空间问题
  # 使用Shiny内置的命名空间机制
  
  # Return the components as a structured list for flexible component access
  list(
    ui = list(
      filter = microCustomerFilterUI(id, translate),
      display = microCustomerDisplayUI(id)
    ),
    server = function(input, output, session) {
      # Pass the config parameter correctly - the module will handle both reactive and non-reactive cases
      filtered_data <- microCustomerServer(
        id, 
        app_data_connection,
        session = session,
        config = config  # This might be a reactive or a regular value
      )
      
      # Return the filtered data for potential outside use
      return(filtered_data)
    }
  )
}

#' @rdname microCustomerComponent
#' @export
#' @principle R12 Minimal Modification
#' @deprecated Please use microCustomerComponent instead


####microCustomerInitialize####

microCustomerInitialize <- function(id, app_data_connection = NULL, config = NULL, translate = function(x) x) {
  # Create alias for backward compatibility
  # Pass parameters directly to ensure reactive expressions are preserved
  microCustomerComponent(id, app_data_connection, config, translate)
}

####initServerSideSelectize####

# 新增的服務器端函數，負責處理 selectize 的選項
initServerSideSelectize <- function(session, inputId, df, labelFields, valueField, maxItems = 1000) {
  # 設置服務器端 selectize 搜索處理函數
  session$sendCustomMessage("updateSelectizeInput", list(
    inputId = session$ns(inputId),
    settings = list(
      load = I(paste0(
        "(query, callback) => {
          if (!query.length) return callback();
          Shiny.setInputValue('", 
        session$ns(paste0(inputId, "_search")),
        "', {query: query, item_count: ", maxItems, "}, {priority: 'event'});
        }"
      ))
    )
  ))
  
  return(reactive({
    if (is.null(input[[paste0(inputId, "_search")]])) {
      return(NULL)
    }
    
    query <- input[[paste0(inputId, "_search")]]$query
    maxResults <- input[[paste0(inputId, "_search")]]$item_count
    
    if (is.null(query) || query == "") {
      return(NULL)
    }
    
    # 使用簡單的模糊匹配算法
    # 在真實環境中可能需要更複雜的搜索邏輯
    filteredData <- df
    for (field in labelFields) {
      # 只處理包含查詢內容的行
      if (nrow2(filteredData) > 0) {
        filteredData <- filteredData[grepl(query, filteredData[[field]], ignore.case = TRUE), ]
      }
    }
    
    # 將結果限制為指定數量
    if (nrow2(filteredData) > maxResults) {
      filteredData <- filteredData[1:maxResults, ]
    }
    
    # 格式化結果
    results <- list()
    if (nrow2(filteredData) > 0) {
      # 創建標籤 - 根據可用欄位組合
      labels <- if (length(labelFields) > 1) {
        apply(filteredData[, labelFields, drop = FALSE], 1, function(row) {
          paste(na.omit(row), collapse = " - ")
        })
      } else {
        filteredData[[labelFields[1]]]
      }
      
      # 建立選項列表
      results <- mapply(
        function(label, value) {
          list(label = label, value = value)
        },
        labels, filteredData[[valueField]],
        SIMPLIFY = FALSE
      )
    }
    
    return(results)
  }))
}
