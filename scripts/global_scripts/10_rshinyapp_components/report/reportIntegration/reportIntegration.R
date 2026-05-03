#' @title Report Integration Module - Auto-Loading Version
#' @description Enhanced report generation with automatic data loading from all modules
#' @principle MP56 Connected Component Principle
#' @principle R091 Universal Data Access Pattern
#' @principle MP81 Explicit Parameter Specification
#' @principle MP099 Real-time progress reporting and monitoring
#' @principle MP106 Console Output Transparency
#' @principle MP052 Unidirectional Data Flow
#' @principle MP064 ETL-Derivation Separation Principle
#' @principle R116 Enhanced Data Access with tbl2

# Helper functions ------------------------------------------------------------
`%||%` <- function(x, y) if (is.null(x)) y else x

#' Extract reactive value from module results - Enhanced version
#' @description Safely extracts values with better debugging output
extract_reactive_value_debug <- function(obj, field = NULL, debug_name = "unknown") {
  tryCatch({
    # MP106: Console Output Transparency
    cat(sprintf("[DEBUG] Extracting %s...\n", debug_name))

    # If field is specified, try to access it first
    if (!is.null(field) && is.list(obj)) {
      obj <- obj[[field]]
    }

    # Now extract the value based on the type
    if (is.null(obj)) {
      cat(sprintf("[DEBUG] %s is NULL\n", debug_name))
      return(NULL)
    } else if (is.function(obj)) {
      # It's a reactive or reactiveVal - call it
      result <- obj()
      # If result is still a function (nested reactive), call it again
      if (is.function(result)) {
        result <- result()
      }
      cat(sprintf("[DEBUG] %s extracted from reactive: %s\n", debug_name, class(result)[1]))
      return(result)
    } else if (is.list(obj) && "ai_analysis_result" %in% names(obj)) {
      cat(sprintf("[DEBUG] %s found in ai_analysis_result field\n", debug_name))
      return(extract_reactive_value_debug(obj$ai_analysis_result, debug_name = paste0(debug_name, ".ai_analysis_result")))
    } else if (is.list(obj) && "result" %in% names(obj)) {
      cat(sprintf("[DEBUG] %s found in result field\n", debug_name))
      return(extract_reactive_value_debug(obj$result, debug_name = paste0(debug_name, ".result")))
    } else if (is.list(obj) && "value" %in% names(obj)) {
      cat(sprintf("[DEBUG] %s found in value field\n", debug_name))
      return(extract_reactive_value_debug(obj$value, debug_name = paste0(debug_name, ".value")))
    } else {
      # Assume it's already a value
      cat(sprintf("[DEBUG] %s is direct value: %s\n", debug_name, class(obj)[1]))
      return(obj)
    }
  }, error = function(e) {
    cat(sprintf("[ERROR] Failed to extract %s: %s\n", debug_name, e$message))
    return(NULL)
  })
}

#' Report Integration UI - Enhanced
#' @param id Module ID
#' @param translate Translation function
reportIntegrationUI <- function(id, translate = function(x) x) {
  ns <- NS(id)

  tagList(
    # Include shinyjs for show/hide functionality
    shinyjs::useShinyjs(),
    # MP106: Console Output Transparency - Remove debug panel from UI
    # Debug messages now go to console only

    # Progress indicator will be shown via withProgress in server

    # Initial guidance message - MP088: Immediate Feedback
    div(
      id = ns("initial_guidance"),
      class = "alert alert-info",
      style = "padding: 20px; margin: 20px 0; text-align: center;",
      h4(icon("info-circle"), " ", translate("Integrated Report Generation Guide")),
      p(translate("Click the Generate Integrated Report button in the left sidebar to automatically combine data from all analysis modules.")),
      p(style = "margin-top: 15px; font-size: 14px; color: #666;",
        icon("magic"), " ", translate("The system will automatically integrate the following modules:"),
        br(),
        paste0("• ", translate("Marketing Vital-Signs"), " (", translate("Market Indicators"), ")"),
        br(),
        paste0("• ", translate("TagPilot"), " (", translate("Customer DNA Analysis"), ")"),
        br(),
        paste0("• ", translate("BrandEdge"), " (", translate("Brand Positioning"), ")"),
        br(),
        paste0("• ", translate("InsightForge 360"), " (", translate("Market Insights"), ")")
      )
    ),

    # Progress and Preview
    uiOutput(ns("generation_progress")),

    # Report Preview - Initially hidden
    shinyjs::hidden(
      div(
        id = ns("report_preview_section"),
        h4(translate("Report Preview")),
        uiOutput(ns("report_preview")),
        br(),
        downloadButton(
          ns("download_report"),
          translate("Download Report"),
          class = "btn-success"
        )
      )
    )
  )
}

#' Report Integration Server - Enhanced with debugging
#' @param id Module ID
#' @param app_data_connection Data connection object
#' @param module_results Reactive containing analysis results from all modules
reportIntegrationServer <- function(id, app_data_connection = NULL, module_results = NULL, translate = function(x) x) {
  moduleServer(id, function(input, output, session) {

    # Debug output reactive
    debug_messages <- reactiveVal("")

    # MP106: Console Output Transparency - Output to console only
    add_debug <- function(msg) {
      timestamp <- format(Sys.time(), "%H:%M:%S")
      message(paste0("[REPORT ", timestamp, "] ", msg))  # Use message() for console output
    }

    # Get OpenAI API key
    gpt_key <- Sys.getenv("OPENAI_API_KEY", "")

    # Resolve UI language once so report fallback text and AI output stay aligned
    ui_lang_input <- tryCatch({
      if (exists("get_language_scope", mode = "function")) {
        get_language_scope("ui_text")
      } else if (exists("app_configs") && !is.null(app_configs$language)) {
        lang_val <- app_configs$language
        if (is.list(lang_val)) {
          lang_val$default %||% "en"
        } else {
          lang_val
        }
      } else {
        "en"
      }
    }, error = function(e) {
      "en"
    })
    is_zh_ui <- grepl("^zh", tolower(as.character(ui_lang_input)[1]))
    report_text <- function(en, zh = en) {
      translated <- translate(en)
      if (is_zh_ui && identical(translated, en)) {
        return(zh)
      }
      translated
    }

    # Check and load chat_api if not available
    if (!exists("chat_api")) {
      add_debug("chat_api not found, attempting to load...")
      chat_api_path <- "scripts/global_scripts/08_ai/fn_chat_api.R"
      if (file.exists(chat_api_path)) {
        source(chat_api_path)
        add_debug("chat_api loaded successfully")
      } else {
        add_debug("WARNING: fn_chat_api.R not found!")
      }
    }

    # Reactive values for report content
    report_content <- reactiveVal(NULL)
    report_html <- reactiveVal(NULL)
    module_loading_status <- reactiveVal(list())
    data_loaded <- reactiveVal(FALSE)

    # MP099: Track generation status
    generation_in_progress <- reactiveVal(FALSE)
    generation_message <- reactiveVal(NULL)

    # MP106: Debug output removed from UI - now console only

    # Helper function to trigger module data loading
    trigger_module_data <- function(module_result) {
      # MP052: Unidirectional Data Flow - Trigger data loading by accessing reactive
      if (!is.null(module_result)) {
        tryCatch({
          # For modules that return reactive lists
          if (is.list(module_result)) {
            # Try to access key data reactives to trigger loading
            if (!is.null(module_result$data)) {
              if (is.function(module_result$data)) module_result$data()
            }
            if (!is.null(module_result$position_data)) {
              if (is.function(module_result$position_data)) module_result$position_data()
            }
            if (!is.null(module_result$kpi_data)) {
              if (is.function(module_result$kpi_data)) module_result$kpi_data()
            }
            if (!is.null(module_result$result)) {
              if (is.function(module_result$result)) module_result$result()
            }
          }
        }, error = function(e) {
          add_debug(sprintf("Error triggering module data: %s", e$message))
        })
      }
    }

    # Generate integrated report with auto-loading
    observeEvent(input$generate_report, {
      add_debug("===================================================================")
      add_debug("=== BUTTON CLICKED: generate_report triggered successfully! ===")
      add_debug("===================================================================")

      # Prevent multiple simultaneous generations
      if (generation_in_progress()) {
        add_debug("Report generation already in progress, skipping...")
        showNotification(
          translate("A report is already being generated. Please do not click again."),
          type = "warning",
          duration = 3
        )
        return()
      }

      generation_in_progress(TRUE)
      generation_message(translate("Generating..."))  # Set initial generating message

      # Hide initial guidance when report generation starts - MP088: Immediate Feedback
      # CRITICAL FIX: In moduleServer context, IDs are already namespaced
      shinyjs::hide("initial_guidance")

      add_debug("=== Starting Report Generation with Auto-Loading ===")
      add_debug(sprintf("OpenAI API Key: %s", ifelse(nzchar(gpt_key), "✓ Available (sk-...)", "✗ Missing")))
      add_debug(sprintf("chat_api function: %s", ifelse(exists("chat_api"), "✓ Available", "✗ Missing")))
      add_debug(sprintf("Module ID: %s", id))
      add_debug(sprintf("Button namespace: %s", session$ns("generate_report")))

      # MP064: ETL-Derivation Separation - Fetch data directly from database
      # R76: Module Data Connection - Use connection, not pre-filtered data
      add_debug("Fetching data directly from database for self-contained generation...")

      # MP099: Real-time progress reporting using withProgress

      # Initialize module loading status
      loading_status <- list(
        vital_signs = "Loading...",
        tagpilot = "Loading...",
        brandedge = "Loading...",
        insightforge = "Loading..."
      )
      module_loading_status(loading_status)

      # Self-contained data fetching variables
      db_data <- list()

      # MP099: Real-time progress reporting and monitoring
      withProgress(
        message = translate("Automatically loading module data and generating the report..."),
        value = 0,
        detail = translate("Initializing..."),
        {

        incProgress(0.05, detail = translate("Starting automatic module data loading..."))
        add_debug("Starting automatic module data loading...")

        # SELF-CONTAINED DATA FETCHING - Following R76 and MP064
        # Fetch data directly from database instead of relying on other modules
        tryCatch({
          add_debug("Attempting direct database connection for self-contained data fetching...")

          # Get database connection from app_data_connection or create new one
          if (!is.null(app_data_connection)) {
            if (is.reactive(app_data_connection)) {
              con <- app_data_connection()
            } else {
              con <- app_data_connection
            }
            add_debug("Using provided database connection")
          } else {
            # Fallback to direct connection - check multiple possible paths
            add_debug("Creating new database connection...")
            db_paths <- c(
              "data/data.duckdb",
              "data/app_data/app_data.duckdb",
              "data/database/mamba.duckdb",
              "scripts/global_scripts/30_global_data/mock_data.duckdb"
            )

            con <- NULL
            for (db_path in db_paths) {
              if (file.exists(db_path)) {
                add_debug(sprintf("Trying to connect to: %s", db_path))
                tryCatch({
                  con <- DBI::dbConnect(duckdb::duckdb(), db_path, read_only = TRUE)
                  add_debug(sprintf("Successfully connected to: %s", db_path))
                  break
                }, error = function(e) {
                  add_debug(sprintf("Failed to connect to %s: %s", db_path, e$message))
                })
              }
            }

            if (is.null(con)) {
              add_debug("WARNING: No database file found or connection failed")
            }
          }

          if (!is.null(con)) {
            # List available tables for debugging
            available_tables <- DBI::dbListTables(con)
            add_debug(sprintf("Available tables: %d", length(available_tables)))

            # #376: Migrated from raw DBI::dbGetQuery() to tbl2() per DM_R023 v1.2

            # Fetch customer DNA data - using actual table names
            if (DBI::dbExistsTable(con, "df_profile_by_customer")) {
              db_data$customers <- tbl2(con, "df_profile_by_customer") %>%
                head(100) %>%
                dplyr::collect()
              add_debug(sprintf("Fetched %d customer profile records", nrow(db_data$customers)))
            } else if (DBI::dbExistsTable(con, "df_dna_by_customer")) {
              db_data$customers <- tbl2(con, "df_dna_by_customer") %>%
                head(100) %>%
                dplyr::collect()
              add_debug(sprintf("Fetched %d DNA customer records", nrow(db_data$customers)))
            } else {
              add_debug("WARNING: No customer table found")
            }

            # Fetch position data - using actual table name
            if (DBI::dbExistsTable(con, "df_position")) {
              db_data$position <- tbl2(con, "df_position") %>%
                head(100) %>%
                dplyr::collect()
              add_debug(sprintf("Fetched %d position records", nrow(db_data$position)))
            } else {
              add_debug("WARNING: Position table not found")
            }

            # Fetch poisson metrics - using actual table name
            if (DBI::dbExistsTable(con, "df_cbz_poisson_analysis_all")) {
              db_data$poisson <- tbl2(con, "df_cbz_poisson_analysis_all") %>%
                head(100) %>%
                dplyr::collect()
              add_debug(sprintf("Fetched %d poisson records", nrow(db_data$poisson)))
            } else if (DBI::dbExistsTable(con, "df_eby_poisson_analysis_all")) {
              db_data$poisson <- tbl2(con, "df_eby_poisson_analysis_all") %>%
                head(100) %>%
                dplyr::collect()
              add_debug(sprintf("Fetched %d poisson records from eby", nrow(db_data$poisson)))
            } else {
              add_debug("WARNING: Poisson analysis table not found")
            }

            # Close connection if we created it
            if (is.null(app_data_connection)) {
              DBI::dbDisconnect(con)
              add_debug("Database connection closed")
            }
          }
        }, error = function(e) {
          add_debug(sprintf("ERROR in self-contained data fetching: %s", e$message))
        })

        # MP064: ETL-Derivation Separation - Trigger data loading for all modules
        if (!is.null(module_results)) {
          if (is.reactive(module_results)) {
            mod_res <- module_results()

            # Trigger Marketing Vital Signs data loading
            incProgress(0.1, detail = translate("Loading Marketing Vital Signs data..."))
            add_debug("Loading Marketing Vital Signs data...")
            if (!is.null(mod_res$vital_signs)) {
              trigger_module_data(mod_res$vital_signs$micro_macro_kpi)
              trigger_module_data(mod_res$vital_signs$dna_distribution)
              loading_status$vital_signs <- "✓ 已載入"
              module_loading_status(loading_status)
            }

            # Trigger TagPilot data loading
            incProgress(0.15, detail = translate("Loading TagPilot data..."))
            add_debug("Loading TagPilot data...")
            if (!is.null(mod_res$tagpilot)) {
              trigger_module_data(mod_res$tagpilot$customer_dna)
              loading_status$tagpilot <- "✓ 已載入"
              module_loading_status(loading_status)
            }

            # Trigger BrandEdge data loading
            incProgress(0.2, detail = translate("Loading BrandEdge data..."))
            add_debug("Loading BrandEdge data...")
            if (!is.null(mod_res$brandedge)) {
              trigger_module_data(mod_res$brandedge$position_table)
              trigger_module_data(mod_res$brandedge$position_dna)
              trigger_module_data(mod_res$brandedge$position_ms)
              trigger_module_data(mod_res$brandedge$position_kfe)
              trigger_module_data(mod_res$brandedge$position_ideal)
              trigger_module_data(mod_res$brandedge$position_strategy)
              loading_status$brandedge <- "✓ 已載入"
              module_loading_status(loading_status)
            }

            # Trigger InsightForge data loading
            incProgress(0.25, detail = translate("Loading InsightForge data..."))
            add_debug("Loading InsightForge data...")
            if (!is.null(mod_res$insightforge)) {
              trigger_module_data(mod_res$insightforge$poisson_comment)
              trigger_module_data(mod_res$insightforge$poisson_time)
              trigger_module_data(mod_res$insightforge$poisson_feature)
              loading_status$insightforge <- "✓ 已載入"
              module_loading_status(loading_status)
            }

            # Allow time for reactive updates to propagate
            Sys.sleep(0.5)

            add_debug("All module data loading triggered successfully")
            data_loaded(TRUE)
          }
        }

        incProgress(0.3, detail = translate("Collecting all analysis results..."))
        add_debug("Collecting analysis results...")

        # Automatically include all modules
        selected_modules <- c(
          "macro_kpi", "dna_dist",           # Marketing Vital-Signs
          "customer_dna",                     # TagPilot
          "position_strategy",                # BrandEdge
          "market_segment", "key_factors",    # BrandEdge
          "market_track",                     # InsightForge 360
          "time_analysis", "precision"        # InsightForge 360
        )

        incProgress(0.4, detail = translate("Integrating AI analysis content..."))
        add_debug("Integrating AI analysis content...")

        # Debug: Check module_results structure
        if (!is.null(module_results)) {
          if (is.reactive(module_results)) {
            mod_res <- module_results()
            add_debug(sprintf("Module results type: %s", class(mod_res)[1]))
            if (is.list(mod_res)) {
              add_debug(sprintf("Module results names: %s", paste(names(mod_res), collapse = ", ")))
            }
          } else {
            add_debug("Module results is not reactive")
          }
        } else {
          add_debug("WARNING: module_results is NULL!")
        }

        # Build report structure
        report_sections <- list()

        # Report header
        report_sections$title <- paste0("# ", translate("MAMBA Integrated Analysis Report"), "\n\n")
        report_sections$date <- paste0("**", translate("Report Date"), ":** ", Sys.Date(), "\n")
        report_sections$time <- paste0("**", translate("Generated Time"), ":** ", format(Sys.time(), "%H:%M:%S"), "\n\n")

        incProgress(0.5, detail = translate("Generating report content..."))
        add_debug("Generating report sections...")

        # Generate each section with error handling
        tryCatch({
          # Section 1: Marketing Vital Signs - Use self-contained data first
          if ("macro_kpi" %in% selected_modules) {
            add_debug("Processing macro_kpi section...")

            # Try self-contained data first
            if (!is.null(db_data$customers) && nrow(db_data$customers) > 0) {
              # Generate insights from self-contained data
              total_customers <- nrow(db_data$customers)
              # Use actual column names from the data
              unique_products <- length(unique(db_data$customers[[1]]))  # First column

              report_sections$macro <- paste0(
                "## 1. ", report_text("Macro Market Indicators", "宏觀市場指標"), "\n\n",
                "### ", report_text("Key Performance Indicators", "關鍵績效指標"), "\n",
                "- ✓ ", report_text("Customer data analysis", "客戶資料分析"), ": ", total_customers, " ", report_text("records", "筆記錄"), "\n",
                "- ", report_text("Unique product count", "獨立產品數"), ": ", unique_products, "\n",
                "- ", report_text("Data source", "資料來源"), ": ", report_text("Customer Profile Database", "客戶檔案資料庫"), "\n",
                "- ", report_text("Data coverage date", "資料涵蓋時間"), ": ", Sys.Date(), "\n\n"
              )
              add_debug("KPI section generated from self-contained data")
            } else if (!is.null(module_results) && is.reactive(module_results)) {
              # Fallback to module results if available
              mod_res <- module_results()
              kpi_data <- extract_reactive_value_debug(
                mod_res$vital_signs$micro_macro_kpi,
                "kpi_data",
                "KPI_Data"
              )

              if (!is.null(kpi_data)) {
                report_sections$macro <- paste0(
                  "## 1. ", report_text("Macro Market Indicators", "宏觀市場指標"), "\n\n",
                  "### ", report_text("Key Performance Indicators", "關鍵績效指標"), "\n",
                  "- ✓ ", report_text("KPI data loaded", "KPI 資料已載入"), "\n",
                  "- ", report_text("Data coverage date", "資料涵蓋時間"), ": ", Sys.Date(), "\n\n"
                )
                add_debug("KPI section generated from module data")
              } else {
                report_sections$macro <- paste0(
                  "## 1. ", report_text("Macro Market Indicators", "宏觀市場指標"), "\n\n",
                  "*", report_text("Waiting for KPI module data...", "等待 KPI 模組資料載入..."), "*\n\n"
                )
                add_debug("KPI data not available")
              }
            } else {
              report_sections$macro <- paste0(
                "## 1. ", report_text("Macro Market Indicators", "宏觀市場指標"), "\n\n",
                "*", report_text("Data is loading, please try again later...", "資料載入中，請稍後重試..."), "*\n\n"
              )
              add_debug("No data available for KPI section")
            }
          }

          # Section 2: Brand Positioning Strategy - Use self-contained data first
          if ("position_strategy" %in% selected_modules) {
            add_debug("Processing position_strategy section...")

            # Try self-contained data first
            if (!is.null(db_data$position) && nrow(db_data$position) > 0) {
              # Generate insights from self-contained position data
              # Use actual columns available in the data
              total_records <- nrow(db_data$position)
              col_names <- names(db_data$position)
              num_columns <- length(col_names)

              report_sections$strategy <- paste0(
                "## 2. ", report_text("Brand Positioning Strategy Analysis", "品牌定位策略分析"), "\n\n",
                "### ", report_text("Strategic Positioning Analysis", "策略定位分析"), "\n",
                "- ", report_text("Position data records", "定位資料記錄"), ": ", total_records, "\n",
                "- ", report_text("Data dimensions", "資料維度"), ": ", num_columns, " ", report_text("analysis indicators", "個分析指標"), "\n",
                "- ", report_text("Data source", "資料來源"), ": ", report_text("Position Analysis Database", "品牌定位分析資料庫"), "\n",
                "- ", report_text("Analysis date", "分析時間"), ": ", Sys.Date(), "\n\n",
                "**", report_text("Strategic Recommendations", "策略建議"), "**\n",
                report_text(
                  "Based on the positioning data, deeper analysis of brand positioning and competitive advantages is recommended.",
                  "基於定位資料，建議深入分析品牌定位與競爭優勢。"
                ), "\n\n"
              )
              add_debug("Position strategy section generated from self-contained data")
            } else if (!is.null(module_results) && is.reactive(module_results)) {
              mod_res <- module_results()

              # Try multiple paths to find the AI analysis
              ai_text <- NULL

              # Path 1: Direct position_strategy
              if (!is.null(mod_res$brandedge$position_strategy)) {
                ai_text <- extract_reactive_value_debug(
                  mod_res$brandedge$position_strategy,
                  "ai_analysis_result",
                  "Position_Strategy_AI"
                )
              }

              # Path 2: Try position module
              if (is.null(ai_text) && !is.null(mod_res$position)) {
                ai_text <- extract_reactive_value_debug(
                  mod_res$position,
                  "ai_analysis",
                  "Position_AI_Alternative"
                )
              }

              # Handle the extracted text
              if (!is.null(ai_text) && length(ai_text) > 0) {
                # Fix for vector case
                if (is.character(ai_text) && all(nzchar(ai_text))) {
                  if (length(ai_text) > 1) {
                    ai_text <- paste(ai_text, collapse = "\n")
                  }
                  report_sections$strategy <- paste0(
                    "## 2. 品牌定位策略分析\n\n",
                    ai_text, "\n\n"
                  )
                  add_debug("Position strategy AI text included")
                } else {
                  report_sections$strategy <- paste0(
                    "## 2. ", report_text("Brand Positioning Strategy Analysis", "品牌定位策略分析"), "\n\n",
                    "### ", report_text("Strategic Positioning Analysis", "策略定位分析"), "\n",
                    "- ", report_text("Four-quadrant strategy analysis in progress", "四象限策略分析進行中"), "\n",
                    "- ", report_text("Brand positioning recommendations are being generated", "品牌定位建議生成中"), "\n\n"
                  )
                  add_debug("Position strategy data processing")
                }
              } else {
                report_sections$strategy <- paste0(
                  "## 2. ", report_text("Brand Positioning Strategy Analysis", "品牌定位策略分析"), "\n\n",
                  "*", report_text("Strategy analysis module data is loading, please try again later...", "策略分析模組資料正在載入中，請稍後重試..."), "*\n\n"
                )
                add_debug("Position strategy not available")
              }
            }
          }

          # Section 3: Market Track Analysis - Use self-contained data first
          if ("market_track" %in% selected_modules) {
            add_debug("Processing market_track section...")

            # Try self-contained data first
            if (!is.null(db_data$poisson) && nrow(db_data$poisson) > 0) {
              # Generate insights from self-contained poisson data
              total_poisson_records <- nrow(db_data$poisson)
              poisson_cols <- names(db_data$poisson)
              num_metrics <- length(poisson_cols)

              report_sections$market <- paste0(
                "## 3. ", report_text("Market Track Analysis", "市場賽道分析"), "\n\n",
                "### ", report_text("Product Track Competitiveness Analysis", "產品賽道競爭力分析"), "\n",
                "- ", report_text("Poisson analysis records", "Poisson 分析記錄"), ": ", total_poisson_records, "\n",
                "- ", report_text("Number of analysis indicators", "分析指標數"), ": ", num_metrics, "\n",
                "- ", report_text("Data source", "資料來源"), ": ", report_text("Poisson Analysis Database", "Poisson 分析資料庫"), "\n",
                "- ", report_text("Analysis date", "分析時間"), ": ", Sys.Date(), "\n\n",
                "**", report_text("Market Insights", "市場洞察"), "**\n",
                report_text(
                  "Based on the Poisson analysis, the distribution of product reviews and ratings indicates strong market activity.",
                  "基於 Poisson 分析，產品評論和評分分布顯示市場活躍度高。"
                ), "\n\n"
              )
              add_debug("Market analysis section generated from self-contained data")
            } else if (!is.null(module_results) && is.reactive(module_results)) {
              mod_res <- module_results()

              comment_text <- extract_reactive_value_debug(
                mod_res$insightforge$poisson_comment,
                NULL,
                "Market_Comment_Analysis"
              )

              if (!is.null(comment_text) && length(comment_text) > 0) {
                # Fix for vector case
                if (is.character(comment_text) && all(nzchar(comment_text))) {
                  if (length(comment_text) > 1) {
                    comment_text <- paste(comment_text, collapse = "\n")
                  }
                  report_sections$market <- paste0(
                    "## 3. 市場賽道分析\n\n",
                    comment_text, "\n\n"
                  )
                  add_debug("Market analysis text included")
                } else {
                  report_sections$market <- paste0(
                    "## 3. ", report_text("Market Track Analysis", "市場賽道分析"), "\n\n",
                    "### ", report_text("Product Track Competitiveness Analysis", "產品賽道競爭力分析"), "\n",
                    "- ", report_text("Rating and review analysis in progress", "評分與評論分析進行中"), "\n",
                    "- ", report_text("Market positioning recommendations are being generated", "市場定位建議生成中"), "\n\n"
                  )
                  add_debug("Market analysis data processing")
                }
              } else {
                report_sections$market <- paste0(
                  "## 3. ", report_text("Market Track Analysis", "市場賽道分析"), "\n\n",
                  "*", report_text("Market analysis module data is loading, please try again later...", "市場分析模組資料正在載入中，請稍後重試..."), "*\n\n"
                )
                add_debug("Market analysis not available")
              }
            }
          }

        }, error = function(e) {
          add_debug(sprintf("ERROR in section generation: %s", e$message))
          report_sections$error <- paste0(
            "## ⚠️ ", translate("Report generation encountered an issue"), "\n\n",
            translate("Some module data could not be loaded correctly. Please check:"), "\n",
            "1. ", translate("Whether each analysis module has finished computing"), "\n",
            "2. ", translate("Whether the data connection is working"), "\n",
            "3. ", translate("Whether the API key is configured correctly"), "\n\n"
          )
        })

        incProgress(0.5, detail = translate("Using AI to generate the complete report in one pass..."))
        add_debug("=== BATCH API OPTIMIZATION: Generating ENTIRE report in ONE API call ===")

        # MP099: Batch API Processing for optimal performance
        # User requirement: "按了之後只要一次api的quest就會跑出所有要的東西"
        # Solution: Send ALL data to AI and generate complete report in ONE call

        if (nzchar(gpt_key) && exists("chat_api")) {
          add_debug("Initiating BATCH report generation with single API call...")

          # Prepare ALL data for batch processing
          data_summary <- list()

          # Collect customer data summary
          if (!is.null(db_data$customers) && nrow(db_data$customers) > 0) {
            data_summary$customers <- if (is_zh_ui) {
              sprintf(
                "客戶資料：%d筆記錄，%d個產品",
                nrow(db_data$customers),
                length(unique(db_data$customers[[1]]))
              )
            } else {
              sprintf(
                "Customer data: %d records, %d products",
                nrow(db_data$customers),
                length(unique(db_data$customers[[1]]))
              )
            }
          }

          # Collect position data summary
          if (!is.null(db_data$position) && nrow(db_data$position) > 0) {
            data_summary$position <- if (is_zh_ui) {
              sprintf(
                "定位資料：%d筆記錄，%d個分析指標",
                nrow(db_data$position),
                length(names(db_data$position))
              )
            } else {
              sprintf(
                "Position data: %d records, %d analysis indicators",
                nrow(db_data$position),
                length(names(db_data$position))
              )
            }
          }

          # Collect poisson data summary
          if (!is.null(db_data$poisson) && nrow(db_data$poisson) > 0) {
            data_summary$poisson <- if (is_zh_ui) {
              sprintf(
                "Poisson分析：%d筆記錄，%d個指標",
                nrow(db_data$poisson),
                length(names(db_data$poisson))
              )
            } else {
              sprintf(
                "Poisson analysis: %d records, %d indicators",
                nrow(db_data$poisson),
                length(names(db_data$poisson))
              )
            }
          }

          # Create comprehensive prompts in the active UI language so the
          # generated report stays consistent with the interface language.
          if (is_zh_ui) {
            sys_prompt <- paste0(
              "你是 MAMBA Enterprise Platform 的高級商業分析顧問。",
              "請用繁體中文撰寫完整的整合分析報告。",
              "報告必須包含所有要求的章節，格式嚴謹專業。"
            )

            user_prompt <- paste0(
              "請基於以下資料生成完整的 MAMBA 整合分析報告（Markdown 格式）：\n\n",
              "**資料摘要：**\n",
              if (length(data_summary) > 0) paste(unlist(data_summary), collapse = "\n") else "資料載入中",
              "\n\n",
              "**報告要求（請完整生成以下所有章節）：**\n\n",
              "## 1. 宏觀市場指標\n",
              "### 關鍵績效指標\n",
              "- 分析客戶資料趨勢\n",
              "- 產品市場覆蓋狀況\n",
              "- 關鍵績效指標解讀\n\n",
              "## 2. 品牌定位策略分析\n",
              "### 策略定位分析\n",
              "- 四象限定位分析\n",
              "- 競爭優勢識別\n",
              "- 品牌定位建議\n",
              "- **策略建議**（具體3-5個行動方案）\n\n",
              "## 3. 市場賽道分析\n",
              "### 產品賽道競爭力分析\n",
              "- Poisson 分析結果解讀\n",
              "- 評論與評分趨勢\n",
              "- 市場活躍度評估\n",
              "- **市場洞察**（關鍵發現2-3點）\n\n",
              "## 4. 整合策略建議\n",
              "### 跨模組整合洞察\n",
              "1. **整合洞察摘要**（3個關鍵發現）\n",
              "2. **立即行動建議**（優先級排序的3個行動）\n",
              "3. **潛在風險提醒**（2個主要風險）\n",
              "4. **長期戰略方向**（1個核心戰略）\n\n",
              "**格式要求：**\n",
              "- 使用 Markdown 格式\n",
              "- 每個章節必須包含具體資料和分析\n",
              "- 總字數控制在800-1000字\n",
              "- 語言簡潔專業\n",
              "- 所有建議必須可執行且具體\n\n",
              "請立即生成完整報告（包含所有4個章節）："
            )
          } else {
            sys_prompt <- paste0(
              "You are a senior business analyst for MAMBA Enterprise Platform. ",
              "Write a complete integrated analysis report in English. ",
              "The report must include all required sections and maintain a professional tone and structure."
            )

            user_prompt <- paste0(
              "Generate a complete MAMBA integrated analysis report in Markdown based on the following data:\n\n",
              "**Data Summary:**\n",
              if (length(data_summary) > 0) paste(unlist(data_summary), collapse = "\n") else "Data loading",
              "\n\n",
              "**Report Requirements (include all sections below):**\n\n",
              "## 1. Macro Market Indicators\n",
              "### Key Performance Indicators\n",
              "- Analyze customer data trends\n",
              "- Assess product market coverage\n",
              "- Interpret key performance indicators\n\n",
              "## 2. Brand Positioning Strategy Analysis\n",
              "### Strategic Positioning Analysis\n",
              "- Review four-quadrant positioning\n",
              "- Identify competitive advantages\n",
              "- Provide brand positioning recommendations\n",
              "- **Strategic Recommendations** (3-5 concrete action items)\n\n",
              "## 3. Market Track Analysis\n",
              "### Product Track Competitiveness Analysis\n",
              "- Interpret Poisson analysis results\n",
              "- Summarize review and rating trends\n",
              "- Assess market activity levels\n",
              "- **Market Insights** (2-3 key findings)\n\n",
              "## 4. Integrated Strategy Recommendations\n",
              "### Cross-Module Integrated Insights\n",
              "1. **Integrated Insights Summary** (3 key findings)\n",
              "2. **Immediate Action Recommendations** (3 prioritized actions)\n",
              "3. **Potential Risk Alerts** (2 major risks)\n",
              "4. **Long-Term Strategic Direction** (1 core strategy)\n\n",
              "**Formatting Requirements:**\n",
              "- Use Markdown format\n",
              "- Every section must include concrete data and analysis\n",
              "- Target 800-1000 words\n",
              "- Keep the language concise and professional\n",
              "- All recommendations must be specific and actionable\n\n",
              "Generate the full report now and include all 4 sections."
            )
          }

          # BATCH API CALL - Generate entire report in ONE request
          add_debug("[BATCH] Sending comprehensive prompt to OpenAI API...")
          add_debug(sprintf("[BATCH] Prompt length: %d characters", nchar(user_prompt)))

          tryCatch({
            incProgress(0.6, detail = translate("AI is generating the complete report..."))

            ai_full_report <- chat_api(
              list(
                list(role = "system", content = sys_prompt),
                list(role = "user", content = user_prompt)
              ),
              gpt_key,
              model = "gpt-5.2",  # 統一使用 ai_prompts.yaml 中的標準模型
              timeout_sec = 120  # Allow more time for comprehensive generation
            )

            incProgress(0.8, detail = translate("Report generation complete, formatting..."))

            if (!is.null(ai_full_report) && nzchar(ai_full_report)) {
              add_debug(sprintf("[BATCH] AI generated complete report (%d characters)", nchar(ai_full_report)))

              # Use AI-generated content as the primary report
              report_sections <- list(
                title = paste0("# ", translate("MAMBA Integrated Analysis Report"), "\n\n"),
                date = paste0("**", translate("Report Date"), ":** ", Sys.Date(), "\n"),
                time = paste0("**", translate("Generated Time"), ":** ", format(Sys.time(), "%H:%M:%S"), "\n\n"),
                content = ai_full_report  # Complete AI-generated report
              )

              add_debug("[BATCH] Report sections updated with AI content")
            } else {
              add_debug("[ERROR] AI API returned empty response")
              # Fallback to basic template if AI fails
              report_sections$error <- paste0(
                "## ⚠️ ", translate("AI report generation failed"), "\n\n",
                translate("The API returned an empty response. Please check:"), "\n",
                "1. ", translate("Whether the OpenAI API key is valid"), "\n",
                "2. ", translate("Whether the network connection is working"), "\n",
                "3. ", translate("Whether the API quota is sufficient"), "\n\n"
              )
            }
          }, error = function(e) {
            add_debug(sprintf("[ERROR] Batch API call failed: %s", e$message))
            report_sections$error <- paste0(
              "## ⚠️ ", translate("Report generation encountered an issue"), "\n\n",
              translate("AI service is temporarily unavailable:"), "\n",
              translate("Error message:"), " ", e$message, "\n\n",
              translate("Please try again later or contact the system administrator."), "\n\n"
            )
          })
        } else {
          add_debug("[ERROR] Cannot perform batch API call - missing prerequisites")
          if (!nzchar(gpt_key)) {
            add_debug("  - OpenAI API key not configured")
          }
          if (!exists("chat_api")) {
            add_debug("  - chat_api function not available")
          }

          # Fallback to manual sections if API not available
          report_sections$error <- paste0(
            "## ⚠️ ", translate("Report generation encountered an issue"), "\n\n",
            translate("Please set the OPENAI_API_KEY environment variable to enable AI report generation."), "\n\n"
          )
        }

        incProgress(0.9, detail = translate("Formatting report..."))
        add_debug("Formatting final report...")

        # Combine all sections
        final_report <- paste(unlist(report_sections), collapse = "")

        # Add footer
        final_report <- paste0(
          final_report,
          "\n---\n",
          "*", translate("This report was automatically generated by MAMBA Enterprise Platform"), "*\n",
          "*", translate("Generated Time"), ": ", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "*\n",
          "*", translate("Powered by AI Marketing Intelligence"), "*"
        )

        report_content(final_report)
        add_debug(sprintf("[CONTENT] Report content saved (%d characters)", nchar(final_report)))

        # Convert to HTML
        if (requireNamespace("markdown", quietly = TRUE)) {
          add_debug("[HTML] Converting markdown to HTML...")
          html_content <- markdown::markdownToHTML(
            text = final_report,
            fragment.only = FALSE
          )
          add_debug(sprintf("[HTML] Raw HTML generated (%d characters)", nchar(html_content)))

          # Add styling
          styled_html <- paste0(
            "<html><head>",
            "<meta charset='utf-8'>",
            "<style>",
            "body { font-family: 'Microsoft YaHei', sans-serif; max-width: 900px; margin: 0 auto; padding: 20px; background: #f8f9fa; }",
            "h1 { color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }",
            "h2 { color: #34495e; margin-top: 30px; border-left: 4px solid #3498db; padding-left: 10px; }",
            "h3 { color: #7f8c8d; }",
            "strong { color: #e74c3c; }",
            "ul { line-height: 1.8; }",
            "li { margin: 5px 0; }",
            "hr { margin: 40px 0; border: none; border-top: 2px solid #ecf0f1; }",
            "em { color: #95a5a6; }",
            "</style>",
            "</head><body>",
            html_content,
            "</body></html>"
          )

          # Save the styled HTML to reactive value
          add_debug(sprintf("[HTML] Setting report_html reactive value (%d characters)", nchar(styled_html)))
          report_html(styled_html)

          # Verify it was set
          verify_content <- report_html()
          if (!is.null(verify_content)) {
            add_debug(sprintf("[HTML] Report HTML verified in reactive (%d characters)", nchar(verify_content)))
          } else {
            add_debug("[ERROR] Report HTML not properly set in reactive!")
          }
        } else {
          fallback_html <- paste0("<pre>", final_report, "</pre>")
          add_debug(sprintf("[HTML] Markdown package not available, using plain text (%d characters)", nchar(fallback_html)))
          report_html(fallback_html)
        }

        # Force UI update
        add_debug("[UI] Triggering UI update for report display")

        # MP106: Console Output Transparency - Log current state
        add_debug(sprintf("[UI] Report content exists: %s", !is.null(report_content())))
        add_debug(sprintf("[UI] Report HTML exists: %s", !is.null(report_html())))
        add_debug(sprintf("[UI] Generation in progress: %s", generation_in_progress()))

        incProgress(1.0, detail = translate("Report generation complete!"))
        add_debug("=== Report Generation Complete with Auto-Loading ===")

        # Clear generation message and mark as complete
        # MP099: Clear progress message when done
        generation_message(NULL)
        generation_in_progress(FALSE)

        # Ensure report preview is shown
        # DEV_R036: Use session$ns for proper namespace
        # CRITICAL FIX: In moduleServer context, element IDs are ALREADY namespaced
        # Do NOT use session$ns() here as it will double-namespace the ID
        shinyjs::show("report_preview_section")  # Element ID without namespace prefix

        # Also ensure initial guidance is hidden
        shinyjs::hide("initial_guidance")
      })
    }, ignoreInit = TRUE)

    # Render report preview with proper reactive invalidation
    output$report_preview <- renderUI({
      html_content <- report_html()

      # MP106: Console Output Transparency - Detailed debugging
      if (is.null(html_content)) {
        add_debug("[RENDER] Report HTML content is NULL")
        return(NULL)
      }

      content_length <- nchar(html_content)
      add_debug(sprintf("[RENDER] Report HTML content length: %d characters", content_length))

      if (content_length == 0) {
        add_debug("[RENDER] Report HTML content is empty string")
        return(NULL)
      }

      # Check if HTML contains expected structure
      has_html_tag <- grepl("<html", html_content, ignore.case = TRUE)
      has_body_tag <- grepl("<body", html_content, ignore.case = TRUE)
      has_h1_tag <- grepl("<h1", html_content, ignore.case = TRUE)

      add_debug(sprintf("[RENDER] HTML structure check - html: %s, body: %s, h1: %s",
                       has_html_tag, has_body_tag, has_h1_tag))

      add_debug("[RENDER] Creating iframe for report preview")

      # Return the iframe
      iframe_element <- tags$iframe(
        srcdoc = html_content,
        width = "100%",
        height = "600px",
        style = "border: 1px solid #ddd; border-radius: 4px; background: white;",
        id = session$ns("report_iframe")
      )

      add_debug("[RENDER] Iframe element created and returned")
      return(iframe_element)
    })

    # Observer to ensure report section shows when content is ready
    # MP099: Real-time progress reporting and monitoring
    # DEV_R036: ShinyJS module namespace handling - use session$ns
    observeEvent(report_html(), {
      html_content <- report_html()

      add_debug("[OBSERVER] Report HTML changed event triggered")

      if (is.null(html_content)) {
        add_debug("[OBSERVER] Report HTML is NULL, not showing preview")
        return()
      }

      content_length <- nchar(html_content)
      add_debug(sprintf("[OBSERVER] Report HTML length: %d characters", content_length))

      if (content_length > 0) {
        add_debug("[OBSERVER] Report HTML ready, showing preview section")

        # Clear any remaining generation message
        generation_message(NULL)
        add_debug("[OBSERVER] Generation message cleared")

        # Hide initial guidance if still visible
        shinyjs::hide("initial_guidance")
        add_debug("[OBSERVER] Initial guidance hidden")

        # Show the report preview section
        # CRITICAL FIX: In moduleServer context, element IDs are ALREADY namespaced
        # Do NOT use session$ns() here as it will double-namespace the ID
        add_debug("[OBSERVER] Showing report_preview_section (already namespaced)")

        shinyjs::show("report_preview_section")
        add_debug("[OBSERVER] shinyjs::show() called - Report preview section should now be visible")

        # Double-check visibility - use the module-namespaced ID for JS
        module_ns_id <- session$ns("report_preview_section")
        shinyjs::runjs(sprintf(
          "console.log('[JS] Element visibility for %s:', $('#%s').is(':visible'));",
          module_ns_id, module_ns_id
        ))
      } else {
        add_debug("[OBSERVER] Report HTML is empty, not showing preview")
      }
    }, ignoreNULL = TRUE)

    # Download handler
    output$download_report <- downloadHandler(
      filename = function() {
        paste0("MAMBA_Report_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".html")
      },
      content = function(file) {
        writeLines(
          report_html() %||% paste0("<html><body>", translate("No report generated"), "</body></html>"),
          file
        )
      }
    )

    # Render generation progress message
    # MP099: Real-time progress reporting
    # DEV_R036: ShinyJS module namespace handling
    output$generation_progress <- renderUI({
      msg <- generation_message()
      if (!is.null(msg)) {
        add_debug(sprintf("Displaying progress message: %s", msg))
        tagList(
          div(
            class = "alert alert-info",
            style = "padding: 10px; margin: 10px 0;",
            icon("spinner", class = "fa-spin"),
            span(style = "margin-left: 10px;", msg)
          )
        )
      } else {
        # Return empty div when no message
        NULL
      }
    })

    # MP106: Module loading status removed from UI - progress shown via withProgress

    # Return reactive values
    return(list(
      report_content = report_content,
      report_html = report_html,
      debug_messages = debug_messages,
      module_loading_status = module_loading_status,
      data_loaded = data_loaded,
      generation_in_progress = generation_in_progress,
      generation_message = generation_message
    ))
  })
}

# Initialize function
reportIntegrationInitialize <- function(id, app_data_connection = NULL, module_results = NULL) {
  translate_fn <- if (exists("translate", mode = "function", inherits = TRUE)) {
    get("translate", mode = "function", inherits = TRUE)
  } else {
    function(x) x
  }

  list(
    ui = reportIntegrationUI(id, translate_fn),
    server = function(input, output, session) {
      reportIntegrationServer(id, app_data_connection, module_results, translate_fn)
    }
  )
}

#' Report Integration Component Wrapper - Enhanced
#' @description Component wrapper with improved debugging
#' @note CRITICAL: This component's UI filter will be rendered OUTSIDE the module server context
#' in union_production_test.R line 484 (output$dynamic_filter). Therefore, we must NOT use ns()
#' for the button ID, as it would create a double-namespaced ID that the moduleServer cannot see.
reportIntegrationComponent <- function(id, app_data_connection = NULL, config = NULL, translate = function(x) x) {
  # CRITICAL FIX: Do NOT create ns here - button must be created with module-aware ID
  # The button will be rendered outside module context but must connect to module server
  # Solution: Manually construct the namespaced ID
  ns <- NS(id)

  # Create UI components - Following MP014: Company Centered Design, R72: Component ID Consistency
  ui_filter <- wellPanel(
    class = "filter-panel",
    style = "padding: 15px;",  # Standard styling matching other modules (R09: UI-Server-Defaults Triple)
    h4(translate("Integrated Report Center"), icon("file-alt"), style = "border-bottom: none; margin-bottom: 10px;"),  # Remove any border and set margin
    tags$hr(style = "margin: 10px 0;"),  # Single separator with controlled margins

    # Module information section first - Following intuitive UI hierarchy
    p(translate("Auto-integrated analysis modules:"), style = "color: #666; font-size: 12px; margin: 0; padding-top: 5px;"),  # Controlled margins
    tags$div(
      style = "font-size: 11px; color: #666; padding-left: 10px; margin-bottom: 15px;",  # Added margin-bottom for spacing
      p(style = "margin: 2px 0;", icon("chart-line", style = "width: 15px;"), paste0(" ", translate("Marketing Vital-Signs"), " ", translate("Market Indicators"))),
      p(style = "margin: 2px 0;", icon("tag", style = "width: 15px;"), paste0(" ", translate("TagPilot"), " ", translate("Customer DNA Analysis"))),
      p(style = "margin: 2px 0;", icon("gem", style = "width: 15px;"), paste0(" ", translate("BrandEdge"), " ", translate("Brand Positioning"))),
      p(style = "margin: 2px 0;", icon("lightbulb", style = "width: 15px;"), paste0(" ", translate("InsightForge 360"), " ", translate("Market Insights")))
    ),

    # API Status info - positioned before action button
    tags$div(
      style = "margin-top: 10px; margin-bottom: 15px; padding: 10px; background: #f8f9fa; border-radius: 5px;",  # Adjusted margins
      tags$small(
        translate("API Status:"),
        tags$span(
          id = ns("api_status"),
          ifelse(
            nzchar(Sys.getenv("OPENAI_API_KEY")),
            paste0("✓ ", translate("Ready")),
            paste0("✗ ", translate("Missing"))
          ),
          style = ifelse(
            nzchar(Sys.getenv("OPENAI_API_KEY")),
            "color: #28a745;",  # Standard success color
            "color: #dc3545;"   # Standard danger color
          )
        )
      )
    ),

    # Report generation button at bottom - Following MP014: Company Centered Design for intuitive UI flow
    actionButton(
      ns("generate_report"),
      translate("Generate Integrated Report"),
      icon = icon("magic"),
      class = "btn-primary btn-block",  # btn-primary for consistency with other modules
      width = "100%",
      style = "margin-top: 10px;"  # Added top margin for visual separation
    )
  )

  ui_display <- reportIntegrationUI(id, translate)

  # Return component structure - Following R09: UI-Server-Defaults Triple
  # Must maintain compatibility with union_production_test.R expectations
  list(
    ui = list(
      filter = ui_filter,  # Left panel with button
      display = ui_display  # Right panel with report display
    ),
    server = function(parent_input, parent_output, parent_session, module_results = NULL) {
      # CRITICAL FIX (2025-10-03): Proper module server invocation
      # The reportIntegrationServer uses moduleServer() internally (line 119)
      # When called from parent, we should NOT pass parent's input/output/session
      # Instead, let moduleServer create its own scoped input/output/session

      # Previous issue: Double-wrapping caused input$generate_report to be invisible
      # Solution: Call moduleServer-based function without parent context
      # IMPORTANT: module_results must be passed as parameter to the server function
      reportIntegrationServer(id, app_data_connection, module_results, translate)
    }
  )
}

# Export the main functions
# Following R69: Function File Naming
# Following MP47: Functional Programming
