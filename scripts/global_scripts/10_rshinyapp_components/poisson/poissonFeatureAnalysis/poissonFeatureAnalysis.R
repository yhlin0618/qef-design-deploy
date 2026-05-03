#LOCK FILE
#
# poissonFeatureAnalysis.R
# Poisson 特徵分析組件（InsightForge 風格）
#
# Following principles:
# - MP56: Connected Component Principle (component structure)
# - MP73: Interactive Visualization Preference (DT and plotly for visualizations)
# - MP81: Explicit Parameter Specification (function arguments)
# - R116: Enhanced Data Access with tbl2 (data access)
# - R09: UI-Server-Defaults Triple (component organization)
# - MP88: Immediate Feedback (real-time filtering without Apply button)
#
# Features:
#   • InsightForge 賽道倍數和邊際效應分析
#   • 戰略重點（賽道倍數）vs 日常優化（邊際效應）
#   • 清晰的商業意義解讀
#   • 互動式視覺化呈現
# -----------------------------------------------------------------------------

# Source covariate exclusion utility (configuration-driven approach)
# Following: Configuration-Driven Development, DRY Principle
source("scripts/global_scripts/04_utils/fn_should_exclude_covariate.R")

# helper ----------------------------------------------------------------------
`%+%` <- function(x, y) paste0(x, y)
`%||%` <- function(x, y) if (is.null(x)) y else x

# Stage notification messages for AI analysis progress
# Following UI_R007: 標準化介面文字 (Traditional Chinese)
# Following UI_R019: AI Process Notification Rule
# Following MP088: Immediate Feedback Principle
FEATURE_ANALYSIS_STAGE_MESSAGES <- list(
  precision_start = "🎯 正在準備精準行銷分析...",
  precision_analyzing = "🤖 AI 正在分析市場機會...",
  precision_complete = "✅ 精準行銷分析完成！",
  product_start = "💡 正在準備產品開發分析...",
  product_analyzing = "🤖 AI 正在分析產品機會...",
  product_complete = "✅ 產品開發分析完成！",
  error = "❌ AI 分析失敗"
)

# Helper function to calculate attribute range dynamically
# Following MP047: Functional Programming - create reusable function
# Following MP081: Explicit Parameter Specification
# Following ISSUE_244B_ENHANCED: Enhanced Chinese variable pattern detection
# This function estimates the attribute range for calculating track multiplier
# Priorities:
# 1. Chinese dummy patterns (配送, 完美, etc.) -> 1
# 2. Categorical dummy patterns (underscore) -> 1
# 3. Chinese rating keywords (評分, 星級) -> 4
# 4. Chinese quantity keywords (數量, 次數) -> 10
# 5. English patterns (existing) -> varies
# 6. Data-driven (future) -> actual range
# 7. Conservative default -> 2 (changed from 4)
calculate_attribute_range <- function(predictor_name, data_connection = NULL) {
  # Priority 1: Chinese dummy variable patterns
  # Common Chinese dummy variable starting words
  if (grepl("^(配送|完美|包含|是否|有無|使用|提供|含有|具備)", predictor_name)) {
    return(1)  # Likely dummy (0/1)
  }

  # Priority 2: Underscore pattern (套件內容_XXX indicates categorical dummy)
  # Categorical dummy encodings usually have underscore separators
  # Pattern explanation:
  # - _\d+_: underscore, digits, underscore (e.g., _45_)
  # - Contains multiple underscores OR starts with recognizable categorical prefix
  # This is intentionally broad to catch categorical encodings
  if (grepl("_\\d+_", predictor_name) ||
      grepl("^(套件內容|product_type|special_|category_|type_|group_)", predictor_name)) {
    # e.g., 套件內容_45_度飽腹按摩, product_type_A, special_price
    return(1)  # Categorical dummy
  }

  # Priority 3: Chinese rating/score keywords
  if (grepl("(評分|分數|星級|等級|品質)", predictor_name)) {
    return(4)  # Likely 5-point scale (1-5)
  }

  # Priority 4: Chinese quantity keywords
  if (grepl("(數量|件數|次數|筆數)", predictor_name)) {
    return(10)  # Count variable
  }

  # Priority 5: English patterns (existing)
  if (grepl("rating|score|star", predictor_name, ignore.case = TRUE)) {
    return(4)  # 5-point scale
  } else if (grepl("binary|flag|is_|has_", predictor_name, ignore.case = TRUE)) {
    return(1)  # Binary
  } else if (grepl("percentage|percent|rate", predictor_name, ignore.case = TRUE)) {
    return(100)
  } else if (grepl("count|quantity|number", predictor_name, ignore.case = TRUE)) {
    return(10)
  } else if (grepl("price|cost|revenue", predictor_name, ignore.case = TRUE)) {
    return(50)
  }

  # Priority 6: Data-driven range detection (optional - leave as placeholder)
  # if (!is.null(data_connection)) {
  #   actual_range <- try_get_actual_range(predictor_name, data_connection)
  #   if (!is.null(actual_range) && !is.na(actual_range)) {
  #     return(actual_range)
  #   }
  # }

  # Priority 7: Conservative default (changed from 4 to 2)
  # Smaller default is more conservative, avoids over-inflating track_multiplier
  return(2)
}

# Helper function to calculate track multiplier with dynamic range
# Following MP088: Immediate Feedback - provide clear calculation basis
calculate_track_multiplier <- function(coefficient, predictor_name, incidence_rate_ratio = NULL) {
  # Get the attribute range dynamically
  attr_range <- calculate_attribute_range(predictor_name)
  
  # Method 1: Using coefficient (preferred when available)
  if (!is.na(coefficient)) {
    # For extreme coefficients, use capped calculation to avoid overflow
    if (abs(coefficient) > 2) {
      # Linear scaling for large coefficients to avoid exponential explosion
      track_multiplier <- exp(2) * (1 + (abs(coefficient) - 2) * 0.5)
    } else {
      # Standard calculation: exp(range × |coefficient|)
      # But cap the range effect to avoid unrealistic values
      effective_range <- min(attr_range, 10)  # Cap range effect at 10
      track_multiplier <- exp(abs(coefficient) * sqrt(effective_range))  # Use sqrt to moderate the effect
    }
  }
  # Method 2: Using incidence rate ratio (fallback)
  else if (!is.null(incidence_rate_ratio) && !is.na(incidence_rate_ratio)) {
    # Calculate power based on actual range
    # Use sqrt of range to moderate the exponential effect
    power <- sqrt(attr_range)
    track_multiplier <- incidence_rate_ratio ^ power
  }
  else {
    return(NA)
  }
  
  # Cap at reasonable maximum (100x)
  return(round(min(track_multiplier, 100), 1))
}

# Filter UI (InsightForge Style) ----------------------------------------------
poissonFeatureAnalysisFilterUI <- function(id, translate = identity) {
  ns <- NS(id)
  
  wellPanel(
    style = "padding:15px;",
    h4(translate("精準行銷分析")),
    p(translate("使用 InsightForge 360 技術分析產品屬性影響力")),
    
    hr(),
    
    # AI Analysis buttons
    actionButton(
      inputId = ns("generate_precision_insight"),
      label = translate("生成 AI 精準行銷洞察"),
      class = "btn-primary btn-block",
      icon = icon("magic")
    ),
    
    br(), br(),
    
    actionButton(
      inputId = ns("generate_product_development"),
      label = translate("生成 AI 新產品開發建議"),
      class = "btn-success btn-block",
      icon = icon("lightbulb")
    )
  )
}

# Display UI (InsightForge Style) ---------------------------------------------
poissonFeatureAnalysisUI <- function(id, translate = identity) {
  ns <- NS(id)
  
  tagList(
    # Include shinyjs dependency
    shinyjs::useShinyjs(),

    # FIX BUG 2: Add CSS for overflow handling (UI_R024: Component-specific styling)
    tags$style(HTML("
      .info-box-content h4 {
        white-space: nowrap;
        overflow: hidden;
        text-overflow: ellipsis;
        max-width: 100%;
        cursor: help;
      }

      .info-box {
        min-height: 100px;
        display: flex;
        align-items: center;
      }

      .info-box-content {
        width: 100%;
        padding: 10px;
      }

      .info-box-content h4 {
        margin-top: 0;
        margin-bottom: 5px;
        font-size: 1.2rem;
      }

      .info-box-content p {
        margin-bottom: 0;
        font-size: 0.9rem;
      }
    ")),

    div(class = "component-header mb-3 text-center",
        h3(translate("🎯 產品屬性影響力分析")),
        p(translate("運用賽道倍數與邊際效應，精準識別戰略重點與日常優化方向"))),

    # TYPE B METADATA BANNER (MP135 v2.0 + UI_R024)
    uiOutput(ns("metadata_banner")),

    # InsightForge 風格的摘要卡片
    fluidRow(
      column(3,
        div(class = "info-box bg-danger",
            div(class = "info-box-content",
                # FIX BUG 2: Add title attribute for full text tooltip (UI_R024)
                h4(textOutput(ns("track_champion")),
                   class = "text-white",
                   title = textOutput(ns("track_champion_full"))),
                p(translate("🏁 賽道冠軍"), class = "text-white")))),
      column(3,
        div(class = "info-box bg-danger",
            div(class = "info-box-content",
                h4(textOutput(ns("track_multiplier_value")), class = "text-white"),
                p(translate("最大賽道倍數"), class = "text-white")))),
      column(3,
        div(class = "info-box bg-warning",
            div(class = "info-box-content",
                # FIX BUG 2: Add title attribute for full text tooltip (UI_R024)
                h4(textOutput(ns("marginal_champion")),
                   class = "text-white",
                   title = textOutput(ns("marginal_champion_full"))),
                p(translate("⚡ 邊際冠軍"), class = "text-white")))),
      column(3,
        div(class = "info-box bg-warning",
            div(class = "info-box-content",
                h4(textOutput(ns("marginal_effect_value")), class = "text-white"),
                p(translate("最大邊際效應"), class = "text-white"))))
    ),
    
    # 決策指南
    div(class = "alert alert-info mb-3",
        h5("📊 決策指南"),
        tags$ul(
          tags$li("賽道倍數 > 2.0：極重要因素，是核心競爭力"),
          tags$li("賽道倍數 1.2-2.0：重要影響因素，應重點關注"),
          tags$li("邊際效應 > 50%：強烈影響，小改進大效果"),
          tags$li("邊際效應 20-50%：中等影響，穩定改進策略")
        )
    ),
    
    # 主要視覺化區域
    div(class = "component-output p-3",
        fluidRow(
          column(12,
            div(class = "card",
                div(class = "card-header",
                    h4("🏁 屬性賽道倍數分析")),
                div(class = "card-body",
                    plotly::plotlyOutput(ns("track_multiplier_plot"), height = "500px")))
          )
        ),
        br(),
        fluidRow(
          column(6,
            div(class = "card",
                div(class = "card-header",
                    h4("⚡ 邊際效應排行")),
                div(class = "card-body",
                    plotly::plotlyOutput(ns("marginal_effect_plot"), height = "400px")))
          ),
          column(6,
            div(class = "card",
                div(class = "card-header",
                    h4("💡 策略建議")),
                div(class = "card-body",
                    htmlOutput(ns("strategy_recommendation"))))
          )
        ),
        br(),
        fluidRow(
          column(12,
            div(class = "card",
                div(class = "card-header bg-light",
                    h4("📋 詳細分析結果", style = "margin: 0; padding: 10px 0;")),
                div(class = "card-body", style = "padding-top: 20px;",
                    # UI_R018: Table Download Button Placement Rule
                    # Download buttons must be placed ABOVE the table, not below
                    # Following UI_R021: Dual Download Buttons for Statistical Data
                    # Statistical components must provide both full and significant results
                    div(class = "table-control-bar",
                        style = "display: flex; justify-content: flex-end; gap: 10px; margin-bottom: 15px;",

                        # Download full data (all results with significance markers)
                        downloadButton(ns("download_full"),
                                      "下載完整數據",
                                      class = "btn-primary btn-sm",
                                      icon = icon("download")),

                        # Download significant results only (p < 0.05)
                        tags$span(
                          title = "下載統計顯著的分析結果 (p < 0.05)",
                          `data-toggle` = "tooltip",
                          downloadButton(ns("download_significant"),
                                        "下載顯著結果",
                                        class = "btn-success btn-sm",
                                        icon = icon("download"))
                        )),
                    # Table BELOW control bar (UI_R018 compliance)
                    DT::DTOutput(ns("analysis_table"), width = "100%")))
          )
        ),
        br(),
        # AI Insights Section
        fluidRow(
          column(12,
            div(class = "card",
                id = ns("ai_insights_section"),
                style = "display: none;",  # Initially hidden
                div(class = "card-header bg-primary text-white",
                    h4("🤖 InsightForge 360 - 精準行銷洞察報告", style = "margin: 0; padding: 10px 0;")),
                div(class = "card-body", style = "padding: 30px;",
                    if (requireNamespace("shinycssloaders", quietly = TRUE)) {
                      shinycssloaders::withSpinner(
                        htmlOutput(ns("precision_insight_output")),
                        type = 6,
                        color = "#0d6efd"
                      )
                    } else {
                      htmlOutput(ns("precision_insight_output"))
                    }
                )
            )
          )
        ),
        br(),
        # AI New Product Development Section
        fluidRow(
          column(12,
            div(class = "card",
                id = ns("ai_product_development_section"),
                style = "display: none;",  # Initially hidden
                div(class = "card-header bg-success text-white",
                    h4("🚀 AI 新產品開發建議", style = "margin: 0; padding: 10px 0;")),
                div(class = "card-body", style = "padding: 30px;",
                    if (requireNamespace("shinycssloaders", quietly = TRUE)) {
                      shinycssloaders::withSpinner(
                        htmlOutput(ns("product_development_output")),
                        type = 6,
                        color = "#28a745"
                      )
                    } else {
                      htmlOutput(ns("product_development_output"))
                    }
                )
            )
          )
        )
    )
  )
}

# Server (InsightForge Style) -------------------------------------------------
poissonFeatureAnalysisServer <- function(id, app_data_connection = NULL, config = NULL,
                                                    session = getDefaultReactiveDomain(),
                                                    translate = identity) {
  moduleServer(id, function(input, output, session) {
    
    # 狀態追蹤
    component_status <- reactiveVal("idle")
    
    # 提取配置參數
    platform_id <- reactive({
      if (is.null(config)) return("cbz")
      
      if (is.function(config)) {
        cfg <- if (shiny::is.reactive(config)) config() else config
      } else {
        cfg <- config
      }
      
      cfg$filters$platform_id %||% cfg$platform_id %||% "cbz"
    })
    
    # 提取產品線參數
    product_line_id <- reactive({
      if (is.null(config)) return("all")
      
      if (is.function(config)) {
        cfg <- if (shiny::is.reactive(config)) config() else config
      } else {
        cfg <- config
      }
      
      cfg$filters$product_line_id_sliced %||% cfg$filters$product_line_id_chosen %||% "all"
    })
    
    # 載入並處理數據
    analysis_data <- reactive({
      component_status("loading")
      
      tryCatch({
        if (is.null(app_data_connection)) {
          component_status("error")
          return(data.frame())
        }
        
        # 載入 Poisson 分析結果
        # 暫時統一使用 Cyberbiz 的數據
        # platform <- platform_id()
        platform <- "cbz"  # 固定使用 Cyberbiz
        prod_line <- product_line_id()
        
        # App data prefers aggregated tables; filter by product_line_id when needed
        table_name <- paste0("df_", platform, "_poisson_analysis_all")

        # Fetch data with basic filters (convergence, time features, estimation status)
        # ONCE before checking column structure
        data <- tbl2(app_data_connection, table_name) %>%
          filter((is.na(predictor_type) | predictor_type != "time_feature") &
                 convergence == "converged" &
                 estimation_status == "estimated") %>%
          collect()

        if (prod_line != "all" && "product_line_id" %in% names(data)) {
          data <- data %>% filter(product_line_id == prod_line)
        }

        data <- data %>%
          # PHASE 4: Add display_name with fallback to predictor
          # Following MP031: Defensive Programming
          mutate(display_name_safe = dplyr::coalesce(display_name, predictor))

        # Apply YAML-based covariate exclusion (presentation layer filtering)
        # Following DM_R043: Predictor Data Classification
        data <- filter_excluded_covariates(
          data,
          predictor_col = "predictor",
          app_type = "poisson_regression"
        )

        # Check if data has coefficient column (not if function exists!)
        # Following MP064: Data-driven logic, not environment-driven
        if ("coefficient" %in% names(data)) {
          # Path 1: Data has coefficient column
          # 計算 InsightForge 指標
          # Following MP047: Functional Programming - use helper functions
          # Following MP088: Immediate Feedback - show calculation basis

          # Defensive check: Ensure required columns exist
          required_cols <- c("coefficient", "predictor", "p_value")
          missing_cols <- setdiff(required_cols, names(data))
          if (length(missing_cols) > 0) {
            stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
          }

          message("DEBUG: Starting mutate() with ", nrow(data), " rows")

          data <- data %>%
            mutate(
              # 限制邊際效應在合理範圍內
              marginal_effect_pct = round(ifelse(abs(coefficient) > 5,
                                                sign(coefficient) * 500,  # 最大 ±500%
                                                (exp(coefficient) - 1) * 100), 1),
              # Dynamic track multiplier calculation based on actual attribute range
              track_multiplier = tryCatch({
                mapply(calculate_track_multiplier,
                       coefficient,
                       predictor,
                       MoreArgs = list(incidence_rate_ratio = NULL))
              }, error = function(e) {
                message("DEBUG: Error in calculate_track_multiplier: ", e$message)
                rep(NA_real_, length(coefficient))
              }),
              # Following Phase 3 (ISSUE_244A_CRITICAL + ISSUE_244B_ENHANCED):
              # Integrate statistical significance + track multiplier (user preference)
              # User feedback: "應該用顯著搭配賽道，因為邊際的話不一定達得到"
              # Track multiplier now accurately calculated with Chinese variable support (Phase 2)
              # Statistical significance (P-value) determines if we pay attention
              # Track multiplier determines the importance level
              # Track multiplier represents total opportunity size from min to max
              practical_meaning = case_when(
                # Priority 1: Check statistical significance
                p_value >= 0.05 ~ "影響不顯著，暫不關注",

                # Priority 2: Highly significant (P < 0.001) - classify by track multiplier
                p_value < 0.001 & track_multiplier >= 3.0 ~
                  paste0(ifelse(coefficient > 0, "⭐ 極重要正向因素", "⚠️ 極重要負面因素"),
                         "，核心競爭力 (賽道倍數: ", round(track_multiplier, 1), "x)"),

                p_value < 0.001 & track_multiplier >= 2.0 ~
                  paste0(ifelse(coefficient > 0, "✓ 重要正向因素", "✗ 重要負面因素"),
                         "，應重點關注 (賽道倍數: ", round(track_multiplier, 1), "x)"),

                p_value < 0.001 & track_multiplier >= 1.2 ~
                  paste0(ifelse(coefficient > 0, "有正向影響", "有負向影響"),
                         "，可考慮優化 (賽道倍數: ", round(track_multiplier, 1), "x)"),

                p_value < 0.001 ~
                  paste0(ifelse(coefficient > 0, "有正向影響", "有負向影響"),
                         "，但機會較小 (賽道倍數: ", round(track_multiplier, 1), "x)"),

                # Priority 3: Moderately significant (P < 0.01)
                p_value < 0.01 & track_multiplier >= 2.5 ~
                  paste0(ifelse(coefficient > 0, "重要正向因素", "重要負面因素"),
                         "，應重點關注 (賽道倍數: ", round(track_multiplier, 1), "x)"),

                p_value < 0.01 & track_multiplier >= 1.5 ~
                  paste0(ifelse(coefficient > 0, "有正向影響", "有負向影響"),
                         "，可考慮優化 (賽道倍數: ", round(track_multiplier, 1), "x)"),

                p_value < 0.01 ~
                  paste0(ifelse(coefficient > 0, "有正向影響", "有負向影響"),
                         "，但機會較小 (賽道倍數: ", round(track_multiplier, 1), "x)"),

                # Priority 4: Marginally significant (P < 0.05)
                p_value < 0.05 & track_multiplier >= 2.0 ~
                  paste0(ifelse(coefficient > 0, "可能有正向影響", "可能有負向影響"),
                         "，建議進一步驗證 (賽道倍數: ", round(track_multiplier, 1), "x)"),

                p_value < 0.05 ~
                  paste0(ifelse(coefficient > 0, "可能有正向影響", "可能有負向影響"),
                         "，機會較小 (賽道倍數: ", round(track_multiplier, 1), "x)"),

                # Default
                TRUE ~ "影響較小或不確定"
              ),
              track_explanation = tryCatch({
                ranges <- sapply(predictor, calculate_attribute_range)
                paste0(
                  "從最", ifelse(coefficient > 0, "低", "高"), "到最",
                  ifelse(coefficient > 0, "高", "低"), "，銷量可相差",
                  track_multiplier, "倍",
                  " (基於屬性範圍: ", ranges, ")"
                )
              }, error = function(e) {
                message("DEBUG: Error in calculate_attribute_range: ", e$message)
                paste0("從最", ifelse(coefficient > 0, "低", "高"), "到最",
                       ifelse(coefficient > 0, "高", "低"), "，銷量可相差",
                       track_multiplier, "倍")
              })
            )
        } else {
          # Path 2: Data has incidence_rate_ratio instead of coefficient
          # 使用基本計算

          # Defensive check: Ensure required columns exist
          required_cols <- c("incidence_rate_ratio", "predictor", "p_value")
          missing_cols <- setdiff(required_cols, names(data))
          if (length(missing_cols) > 0) {
            stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
          }

          message("DEBUG: Starting mutate() (Path 2) with ", nrow(data), " rows")

          data <- data %>%
            mutate(
              # 限制邊際效應在合理範圍內
              marginal_effect_pct = round(pmin(pmax((incidence_rate_ratio - 1) * 100, -90), 500), 1),
              # Dynamic track multiplier calculation using actual attribute ranges
              # Following R116: Enhanced Data Access - use appropriate calculations
              track_multiplier = tryCatch({
                mapply(calculate_track_multiplier,
                       coefficient = NA,
                       predictor,
                       incidence_rate_ratio)
              }, error = function(e) {
                message("DEBUG: Error in calculate_track_multiplier (Path 2): ", e$message)
                rep(NA_real_, length(predictor))
              }),
              # Following Phase 3 (ISSUE_244A_CRITICAL + ISSUE_244B_ENHANCED):
              # Integrate statistical significance + track multiplier (user preference)
              # User feedback: "應該用顯著搭配賽道，因為邊際的話不一定達得到"
              # Track multiplier now accurately calculated with Chinese variable support (Phase 2)
              # Statistical significance (P-value) determines if we pay attention
              # Track multiplier determines the importance level
              # Track multiplier represents total opportunity size from min to max
              practical_meaning = case_when(
                # Priority 1: Check statistical significance
                p_value >= 0.05 ~ "影響不顯著，暫不關注",

                # Priority 2: Highly significant (P < 0.001) - classify by track multiplier
                p_value < 0.001 & track_multiplier >= 3.0 ~
                  paste0(ifelse(incidence_rate_ratio > 1, "⭐ 極重要正向因素", "⚠️ 極重要負面因素"),
                         "，核心競爭力 (賽道倍數: ", round(track_multiplier, 1), "x)"),

                p_value < 0.001 & track_multiplier >= 2.0 ~
                  paste0(ifelse(incidence_rate_ratio > 1, "✓ 重要正向因素", "✗ 重要負面因素"),
                         "，應重點關注 (賽道倍數: ", round(track_multiplier, 1), "x)"),

                p_value < 0.001 & track_multiplier >= 1.2 ~
                  paste0(ifelse(incidence_rate_ratio > 1, "有正向影響", "有負向影響"),
                         "，可考慮優化 (賽道倍數: ", round(track_multiplier, 1), "x)"),

                p_value < 0.001 ~
                  paste0(ifelse(incidence_rate_ratio > 1, "有正向影響", "有負向影響"),
                         "，但機會較小 (賽道倍數: ", round(track_multiplier, 1), "x)"),

                # Priority 3: Moderately significant (P < 0.01)
                p_value < 0.01 & track_multiplier >= 2.5 ~
                  paste0(ifelse(incidence_rate_ratio > 1, "重要正向因素", "重要負面因素"),
                         "，應重點關注 (賽道倍數: ", round(track_multiplier, 1), "x)"),

                p_value < 0.01 & track_multiplier >= 1.5 ~
                  paste0(ifelse(incidence_rate_ratio > 1, "有正向影響", "有負向影響"),
                         "，可考慮優化 (賽道倍數: ", round(track_multiplier, 1), "x)"),

                p_value < 0.01 ~
                  paste0(ifelse(incidence_rate_ratio > 1, "有正向影響", "有負向影響"),
                         "，但機會較小 (賽道倍數: ", round(track_multiplier, 1), "x)"),

                # Priority 4: Marginally significant (P < 0.05)
                p_value < 0.05 & track_multiplier >= 2.0 ~
                  paste0(ifelse(incidence_rate_ratio > 1, "可能有正向影響", "可能有負向影響"),
                         "，建議進一步驗證 (賽道倍數: ", round(track_multiplier, 1), "x)"),

                p_value < 0.05 ~
                  paste0(ifelse(incidence_rate_ratio > 1, "可能有正向影響", "可能有負向影響"),
                         "，機會較小 (賽道倍數: ", round(track_multiplier, 1), "x)"),

                # Default
                TRUE ~ "影響較小或不確定"
              ),
              track_explanation = tryCatch({
                ranges <- sapply(predictor, calculate_attribute_range)
                paste0(
                  "從最", ifelse(incidence_rate_ratio > 1, "低", "高"), "到最",
                  ifelse(incidence_rate_ratio > 1, "高", "低"), "，銷量可相差",
                  track_multiplier, "倍",
                  " (基於屬性範圍: ", ranges, ")"
                )
              }, error = function(e) {
                message("DEBUG: Error in calculate_attribute_range (Path 2): ", e$message)
                paste0("從最", ifelse(incidence_rate_ratio > 1, "低", "高"), "到最",
                       ifelse(incidence_rate_ratio > 1, "高", "低"), "，銷量可相差",
                       track_multiplier, "倍")
              })
            )
        }
        
        component_status("ready")
        return(data)
        
      }, error = function(e) {
        # Enhanced error logging for debugging
        error_msg <- paste0("Error loading analysis data: ", e$message, "\n",
                           "Call stack: ", paste(deparse(e$call), collapse = "\n"))
        warning(error_msg)
        message("DEBUG: ", error_msg)  # Force console output
        print(traceback())  # Print full traceback
        component_status("error")
        data.frame()
      })
    })
    
    # 篩選顯示所有有效的屬性（不限於正向影響）
    positive_data <- reactive({
      data <- analysis_data()

      # Check if data is empty (error occurred in analysis_data)
      if (nrow(data) == 0 || ncol(data) == 0) {
        message("DEBUG: analysis_data() returned empty dataframe - check error logs above")
        return(data.frame())  # Return empty dataframe gracefully
      }

      # 顯示找到多少資料
      cat("精準模型找到", nrow(data), "筆屬性資料\n")

      # 顯示所有有係數和p值的屬性，但排除評分相關屬性和異常值
      # Handle both coefficient and incidence_rate_ratio cases
      if ("coefficient" %in% names(data)) {
        # Path 1: Using coefficient
        filtered_data <- data %>%
          filter(!is.na(coefficient) & !is.na(p_value) &
                 !grepl("rating", predictor, ignore.case = TRUE) &
                 abs(coefficient) <= 10)  # 排除係數過大的異常值（如 material）
      } else if ("incidence_rate_ratio" %in% names(data)) {
        # Path 2: Using incidence_rate_ratio
        filtered_data <- data %>%
          filter(!is.na(incidence_rate_ratio) & !is.na(p_value) &
                 !grepl("rating", predictor, ignore.case = TRUE) &
                 incidence_rate_ratio >= 0.01 & incidence_rate_ratio <= 100)  # 排除極端倍數的異常值
      } else {
        # Path 3: Neither column exists - return empty
        message("DEBUG: Neither 'coefficient' nor 'incidence_rate_ratio' found in data")
        message("DEBUG: Available columns: ", paste(names(data), collapse = ", "))
        return(data.frame())
      }
      
      # Apply covariate exclusion rules for display purposes only
      # This preserves the full analysis but filters what users see
      if (nrow(filtered_data) > 0) {
        tryCatch({
          all_predictors <- unique(filtered_data$predictor)
          kept_predictors <- filter_covariates(
            var_names = all_predictors,
            app_type = "poisson_regression",
            verbose = FALSE
          )
          
          # Filter data to keep only allowed predictors
          filtered_data <- filtered_data %>%
            dplyr::filter(predictor %in% kept_predictors)
          
          # Log exclusions if verbose
          excluded_count <- length(all_predictors) - length(kept_predictors)
          if (excluded_count > 0) {
            message(sprintf("Hiding %d covariates from display based on exclusion rules", excluded_count))
          }
        }, error = function(e) {
          # If function not available, show all predictors
          warning("filter_covariates not available, showing all covariates: ", e$message)
        })
      }
      
      filtered_data %>%
        arrange(desc(abs(track_multiplier)))  # 按絕對值排序
    })

    # TYPE B METADATA BANNER (MP135 v2.0 + UI_R024)
    # Display metadata for steady-state analytics using all historical data
    output$metadata_banner <- renderUI({
      # Get analysis data
      data <- analysis_data()

      # If no data, don't show banner
      if (is.null(data) || nrow(data) == 0) {
        return(NULL)
      }

      # Extract Type B metadata from first row (all rows have same metadata)
      computed_at <- data$computed_at[1]
      data_version <- data$data_version[1]

      # FIX BUG 1: Check for NULL first, then check NA
      # Following MP031: Defensive Programming
      # is.na(NULL) returns logical(0), not TRUE/FALSE, causing error
      if (is.null(computed_at) || is.null(data_version) ||
          is.na(computed_at) || is.na(data_version)) {
        return(NULL)
      }

      # Convert to proper date/time types (after NULL/NA check)
      computed_at <- as.POSIXct(computed_at)
      data_version <- as.Date(data_version)

      # Additional safety check after conversion
      if (is.na(computed_at) || is.na(data_version)) {
        return(NULL)
      }

      # Render metadata banner (per UI_R024)
      # SECURITY FIX: Removed "數據至" field (sensitive business data)
      # UX FIX: Changed text color to #495057 for proper contrast (WCAG AA compliant)
      div(
        class = "alert alert-info",
        style = "margin-bottom: 15px; padding: 10px 15px; background-color: #e8f4f8; border-left: 4px solid #1e88e5; color: #495057;",

        div(
          style = "display: flex; align-items: center; gap: 20px; flex-wrap: wrap;",

          # Data scope
          div(
            tags$i(class = "fas fa-database", style = "color: #1565c0;"),
            strong(" 基於全部歷史數據")
          ),

          # Computed timestamp (only non-sensitive temporal metadata shown)
          div(
            tags$i(class = "fas fa-clock", style = "color: #1565c0;"),
            sprintf(" 計算時間: %s", format(computed_at, "%Y-%m-%d %H:%M"))
          )
        )
      )
    })

    # 摘要統計 - 顯示影響力最大的屬性（不論正負）
    # FIX BUG 2: Consistent truncation logic with better length calculation
    # Following MP031: Defensive Programming + UI_R024: Metadata Display
    # PHASE 4: Use display_name_safe for user-friendly names
    output$track_champion <- renderText({
      data <- positive_data()
      if (nrow(data) == 0) return("--")

      top <- data[1, ]
      # Chinese characters take more space, use conservative limit
      max_display_chars <- 20  # Increased from 15 for better readability

      if (nchar(top$display_name_safe) > max_display_chars) {
        # Truncate at max_display_chars - 3 (for "...")
        paste0(substr(top$display_name_safe, 1, max_display_chars - 3), "...")
      } else {
        top$display_name_safe
      }
    })

    # FIX BUG 2: Add full text output for tooltip
    # PHASE 4: Use display_name_safe for tooltip
    output$track_champion_full <- renderText({
      data <- positive_data()
      if (nrow(data) == 0) return("--")
      data$display_name_safe[1]  # Full text without truncation
    })

    output$track_multiplier_value <- renderText({
      data <- positive_data()
      if (nrow(data) == 0) return("--")
      paste0(data$track_multiplier[1], " 倍")
    })

    # PHASE 4: Use display_name_safe for user-friendly names
    output$marginal_champion <- renderText({
      data <- positive_data()
      if (nrow(data) == 0) return("--")

      top <- data %>% arrange(desc(abs(marginal_effect_pct))) %>% slice(1)
      # Consistent truncation logic
      max_display_chars <- 20

      if (nchar(top$display_name_safe) > max_display_chars) {
        paste0(substr(top$display_name_safe, 1, max_display_chars - 3), "...")
      } else {
        top$display_name_safe
      }
    })

    # FIX BUG 2: Add full text output for tooltip
    # PHASE 4: Use display_name_safe for tooltip
    output$marginal_champion_full <- renderText({
      data <- positive_data()
      if (nrow(data) == 0) return("--")
      top <- data %>% arrange(desc(abs(marginal_effect_pct))) %>% slice(1)
      top$display_name_safe  # Full text without truncation
    })

    output$marginal_effect_value <- renderText({
      data <- positive_data()
      if (nrow(data) == 0) return("--")

      top <- data %>% arrange(desc(abs(marginal_effect_pct))) %>% slice(1)
      paste0(abs(top$marginal_effect_pct), "%")
    })
    
    # 賽道倍數圖
    output$track_multiplier_plot <- plotly::renderPlotly({
      # 顯示前20個最重要的屬性（按賽道倍數絕對值）
      data <- positive_data() %>% 
        filter(!is.na(track_multiplier)) %>%
        slice_head(n = 20)
      
      if (nrow(data) == 0) {
        plotly::plot_ly() %>%
          plotly::add_text(x = 0.5, y = 0.5, text = "無正向影響的屬性資料",
                          textposition = "center", showlegend = FALSE)
      } else {
        # PHASE 4: Use display_name_safe in hover text and labels
        data <- data %>%
          mutate(
            hover_text = paste0(
              "屬性: ", display_name_safe, "<br>",
              "技術名稱: ", predictor, "<br>",
              "賽道倍數: ", track_multiplier, " 倍<br>",
              "邊際效應: ", marginal_effect_pct, "%<br>",
              "商業意義: ", practical_meaning, "<br>",
              track_explanation
            ),
            display_short = ifelse(nchar(display_name_safe) > 20,
                                   paste0(substr(display_name_safe, 1, 17), "..."),
                                   display_name_safe)
          )

        plotly::plot_ly(data,
                       x = ~track_multiplier,
                       y = ~reorder(display_short, track_multiplier),
                       type = "bar",
                       orientation = "h",
                       marker = list(color = ~track_multiplier,
                                   colorscale = list(c(0, "#FFF3CD"), c(0.5, "#FFC107"), c(1, "#DC3545")),
                                   cmin = 1, cmax = max(data$track_multiplier)),
                       text = ~hover_text,
                       textposition = "none",  # 不顯示文字標籤
                       hoverinfo = "text") %>%
          plotly::layout(
            title = "",
            xaxis = list(title = "賽道倍數（從最低到最高的影響倍數）"),
            yaxis = list(title = ""),
            shapes = list(
              list(type = "line", x0 = 2, x1 = 2, y0 = -0.5, y1 = length(unique(data$predictor)) - 0.5,
                   line = list(color = "red", dash = "dash")),
              list(type = "line", x0 = 1.2, x1 = 1.2, y0 = -0.5, y1 = length(unique(data$predictor)) - 0.5,
                   line = list(color = "orange", dash = "dot"))
            )
          )
      }
    })
    
    # 邊際效應圖
    output$marginal_effect_plot <- plotly::renderPlotly({
      data <- positive_data() %>%
        arrange(desc(abs(marginal_effect_pct))) %>%
        slice_head(n = 10)
      
      if (nrow(data) == 0) {
        plotly::plot_ly() %>%
          plotly::add_text(x = 0.5, y = 0.5, text = "無資料", showlegend = FALSE)
      } else {
        # PHASE 4: Use display_name_safe for labels and hover text
        data <- data %>%
          mutate(
            display_short = ifelse(nchar(display_name_safe) > 15,
                                   paste0(substr(display_name_safe, 1, 12), "..."),
                                   display_name_safe)
          )

        plotly::plot_ly(data,
                       x = ~reorder(display_short, abs(marginal_effect_pct)),
                       y = ~marginal_effect_pct,
                       type = "bar",
                       marker = list(color = ~ifelse(marginal_effect_pct > 50, "#DC3545",
                                                   ifelse(marginal_effect_pct > 20, "#FFC107", "#28A745"))),
                       text = ~paste0(round(marginal_effect_pct, 1), "%"),
                       textposition = "outside",
                       hoverinfo = "text",
                       hovertext = ~paste0("每提升1單位", display_name_safe, "，銷量增加",
                                         round(marginal_effect_pct, 1), "%")) %>%
          plotly::layout(
            title = "",
            xaxis = list(title = "", tickangle = -45),
            yaxis = list(title = "邊際效應 (%)"),
            showlegend = FALSE
          )
      }
    })
    
    # 策略建議
    output$strategy_recommendation <- renderUI({
      data <- positive_data()
      
      if (nrow(data) == 0) {
        return(p("暫無分析結果"))
      }
      
      # 找出賽道冠軍和邊際冠軍
      track_top <- data[1, ]
      marginal_top <- data %>% arrange(desc(abs(marginal_effect_pct))) %>% slice(1)
      
      # PHASE 4: Use display_name_safe in recommendations
      recommendation <- tags$div(
        h5("💡 基於分析結果的行動建議："),
        tags$ul(
          tags$li(tags$strong("戰略重點："),
                  paste0("優先提升「", track_top$display_name_safe, "」，",
                        "此屬性從最低到最高可讓銷量相差", track_top$track_multiplier, "倍")),
          tags$li(tags$strong("快速見效："),
                  paste0("立即改善「", marginal_top$display_name_safe, "」，",
                        "每提升1單位可增加銷量", abs(marginal_top$marginal_effect_pct), "%")),
          tags$li(tags$strong("資源配置："),
                  "將80%資源投入賽道倍數>2的屬性，20%用於邊際效應>50%的快速優化")
        ),
        br(),
        tags$div(class = "alert alert-success",
          tags$strong("執行建議："),
          "結合「戰略+戰術」雙重優化策略，長期布局與短期成效並重"
        )
      )
      
      return(recommendation)
    })
    
    # 詳細表格
    # Following MP122: Statistical Interpretation Transparency
    # Following ISSUE_108 Phase 2.1: Enhanced Statistical Information Display
    # Following ISSUE_154: Significance Consistency Explanation
    output$analysis_table <- DT::renderDT({
      data <- positive_data()

      # 除錯訊息
      cat("精準模型詳細表格資料筆數:", nrow(data), "\n")

      if (nrow(data) == 0) {
        return(data.frame(訊息 = "無屬性資料"))
      }

      # MP122: Include comprehensive statistical information for transparency
      # This addresses ISSUE_108 (coefficient interpretation confusion) and
      # ISSUE_154 (why same coefficient has different significance)

      # Check which column exists: coefficient or incidence_rate_ratio
      if ("coefficient" %in% names(data)) {
        # Path 1: Using coefficient
        # PHASE 4: Include display_name_safe as primary display column
        table_data <- data %>%
          dplyr::select(
            display_name_safe,
            predictor,
            track_multiplier,
            marginal_effect_pct,
            coefficient,
            std_error,      # NEW: Standard error shows uncertainty
            conf_low,       # NEW: Confidence interval lower bound
            conf_high,      # NEW: Confidence interval upper bound
            sample_size,    # NEW: Sample size affects precision
            p_value,
            practical_meaning
          ) %>%
          mutate(
            # Format numerical values appropriately
            coefficient = round(coefficient, 4),
            std_error = round(std_error, 4),
            # Create readable confidence interval format
            confidence_interval = paste0("[", round(conf_low, 2), ", ", round(conf_high, 2), "]"),
            # Format p-value with appropriate precision
            p_value_formatted = ifelse(p_value < 0.001,
                                      "< 0.001",
                                      format.pval(p_value, digits = 3)),
            # Calculate significance stars
            significance = case_when(
              p_value < 0.001 ~ "***",
              p_value < 0.01 ~ "**",
              p_value < 0.05 ~ "*",
              TRUE ~ ""
            )
          ) %>%
          # Select and order columns for display
          # PHASE 4: Use display_name_safe as primary column, predictor as hidden reference
          dplyr::select(
            display_name_safe,
            predictor,
            track_multiplier,
            marginal_effect_pct,
            coefficient,
            std_error,
            confidence_interval,
            sample_size,
            p_value_formatted,
            significance,
            practical_meaning
          )

        # Set Chinese column names with clear descriptions
        # PHASE 4: Display name as primary, predictor as secondary
        colnames(table_data) <- sapply(c(
          "Attribute Name", "Technical Name", "Track Multiplier", "Marginal Effect%",
          "Coefficient", "Standard Error", "95% Confidence Interval",
          "Sample Size", "P-Value", "Significance", "Business Meaning"
        ), translate)
      } else {
        # Path 2: Using incidence_rate_ratio
        # PHASE 4: Include display_name_safe as primary display column
        table_data <- data %>%
          dplyr::select(
            display_name_safe,
            predictor,
            track_multiplier,
            marginal_effect_pct,
            incidence_rate_ratio,
            std_error,      # NEW: Standard error shows uncertainty
            conf_low,       # NEW: Confidence interval lower bound
            conf_high,      # NEW: Confidence interval upper bound
            sample_size,    # NEW: Sample size affects precision
            p_value,
            practical_meaning
          ) %>%
          mutate(
            # Format numerical values appropriately
            incidence_rate_ratio = round(incidence_rate_ratio, 4),
            std_error = round(std_error, 4),
            # Create readable confidence interval format
            confidence_interval = paste0("[", round(conf_low, 2), ", ", round(conf_high, 2), "]"),
            # Format p-value with appropriate precision
            p_value_formatted = ifelse(p_value < 0.001,
                                      "< 0.001",
                                      format.pval(p_value, digits = 3)),
            # Calculate significance stars
            significance = case_when(
              p_value < 0.001 ~ "***",
              p_value < 0.01 ~ "**",
              p_value < 0.05 ~ "*",
              TRUE ~ ""
            )
          ) %>%
          # Select and order columns for display
          # PHASE 4: Use display_name_safe as primary column
          dplyr::select(
            display_name_safe,
            predictor,
            track_multiplier,
            marginal_effect_pct,
            incidence_rate_ratio,
            std_error,
            confidence_interval,
            sample_size,
            p_value_formatted,
            significance,
            practical_meaning
          )

        # Set Chinese column names with clear descriptions
        # PHASE 4: Display name as primary, predictor as secondary
        colnames(table_data) <- sapply(c(
          "Attribute Name", "Technical Name", "Track Multiplier", "Marginal Effect%",
          "Incidence Rate Ratio", "Standard Error", "95% Confidence Interval",
          "Sample Size", "P-Value", "Significance", "Business Meaning"
        ), translate)
      }

      # Create datatable with enhanced formatting
      # Following UI_R018: Removed DT Buttons (download button now above table)
      DT::datatable(
        table_data,
        options = list(
          pageLength = 10,
          scrollX = TRUE,
          dom = 'frtip',  # Removed 'B' (Buttons) - using independent downloadButton above
          order = list(list(1, 'desc')),  # 預設按賽道倍數排序
          columnDefs = list(
            # Improve readability with appropriate column widths
            list(width = '150px', targets = 0),  # 屬性名稱
            list(width = '80px', targets = c(1, 2, 7, 8)),  # 數值欄位
            list(width = '100px', targets = c(3, 4, 5, 6))   # 統計欄位
          )
        ),
        rownames = FALSE,
        # Add caption with statistical guidance (MP122: Transparency)
        caption = htmltools::tags$caption(
          style = 'caption-side: bottom; text-align: left; padding: 10px; font-size: 0.9em; color: #666;',
          htmltools::tags$div(
            htmltools::tags$strong("📊 統計資訊說明："),
            htmltools::tags$ul(
              style = 'margin-top: 5px; margin-bottom: 5px;',
              htmltools::tags$li(
                htmltools::tags$strong("標準誤差："),
                "係數估計的不確定性。標準誤差越大，估計越不精確。"
              ),
              htmltools::tags$li(
                htmltools::tags$strong("95% 信賴區間："),
                "真實效果有 95% 機率落在此範圍內。區間越窄，估計越精確。"
              ),
              htmltools::tags$li(
                htmltools::tags$strong("樣本數："),
                "用於估計此係數的觀測值數量。樣本數越大，估計越可靠。"
              ),
              htmltools::tags$li(
                htmltools::tags$strong("為何相同係數有不同顯著性？"),
                "標準誤差和樣本數會影響顯著性。即使係數相同，若標準誤差較大或樣本數較少，p值會較大，顯著性較低。"
              )
            )
          )
        )
      ) %>%
        # Highlight important track multipliers
        formatStyle(translate("Track Multiplier"),
                   backgroundColor = styleInterval(c(1.2, 2.0),
                                                 c("white", "#FFF3CD", "#F8D7DA")),
                   fontWeight = styleInterval(2.0, c("normal", "bold"))) %>%
        # Color-code significance levels
        formatStyle(translate("Significance"),
                   color = styleEqual(c("*", "**", "***"),
                                    c("#28A745", "#FFC107", "#DC3545"))) %>%
        # Highlight small sample sizes (potential reliability concern)
        formatStyle(translate("Sample Size"),
                   backgroundColor = styleInterval(c(100, 500),
                                                 c("#FFF3CD", "#E8F5E9", "white")))
    })

    # Following UI_R021: Dual Download Buttons for Statistical Data
    # Download 1: Full Data (all results with significance markers)
    # Following ISSUE_108 Phase 2.1: Include enhanced statistical information in exports
    # Following ISSUE_245: UTF-8 BOM for Excel compatibility
    # Following UI_R020: All downloads use CSV with UTF-8 BOM
    output$download_full <- downloadHandler(
      filename = function() {
        paste0("InsightForge_完整屬性分析_", Sys.Date(), ".csv")
      },
      content = function(file) {
        req(positive_data())

        # Include ALL results (not just significant ones)
        data <- positive_data()

        # 準備表格數據 - 包含完整統計資訊
        # PHASE 4: Include display_name_safe for user-friendly exports
        table_data <- data %>%
          dplyr::select(
            display_name_safe,
            predictor,
            track_multiplier,
            marginal_effect_pct,
            coefficient,
            std_error,
            conf_low,
            conf_high,
            sample_size,
            p_value,
            practical_meaning
          ) %>%
          mutate(
            coefficient = round(coefficient, 4),
            std_error = round(std_error, 4),
            # Create confidence interval column for CSV
            confidence_interval = paste0("[", round(conf_low, 2), ", ", round(conf_high, 2), "]"),
            p_value = round(p_value, 4),
            # Add significance markers for all data
            significance = case_when(
              is.na(p_value) ~ "",
              p_value < 0.001 ~ "***",
              p_value < 0.01 ~ "**",
              p_value < 0.05 ~ "*",
              TRUE ~ ""
            )
          ) %>%
          # Select final columns for export
          # PHASE 4: Export display_name as primary, predictor as technical reference
          dplyr::select(
            display_name_safe,
            predictor,
            track_multiplier,
            marginal_effect_pct,
            coefficient,
            std_error,
            confidence_interval,
            sample_size,
            p_value,
            significance,
            practical_meaning
          )

        # 設定中文欄位名稱
        # PHASE 4: Include both user-friendly and technical names
        colnames(table_data) <- sapply(c(
          "Attribute Name", "Technical Name", "Track Multiplier", "Marginal Effect%",
          "Coefficient", "Standard Error", "95% Confidence Interval",
          "Sample Size", "P-Value", "Significance", "Business Meaning"
        ), translate)

        # 匯出 CSV with UTF-8 BOM (ISSUE_245)
        # Requires: source("scripts/global_scripts/04_utils/fn_write_utf8_csv.R")
        write_utf8_csv_with_bom(table_data, file)
      }
    )

    # Download 2: Significant Results Only (p < 0.05)
    # Following ISSUE_108 Phase 2.1: Include enhanced statistical information in exports
    # Following ISSUE_245: UTF-8 BOM for Excel compatibility
    # Following UI_R020: All downloads use CSV with UTF-8 BOM
    output$download_significant <- downloadHandler(
      filename = function() {
        paste0("InsightForge_顯著屬性分析_", Sys.Date(), ".csv")
      },
      content = function(file) {
        req(positive_data())

        # 只選擇顯著的結果
        data <- positive_data() %>%
          filter(p_value < 0.05)

        # 準備表格數據 - 包含完整統計資訊
        # Handle both coefficient and incidence_rate_ratio cases
        if ("coefficient" %in% names(data)) {
          # Path 1: Using coefficient
          # PHASE 4: Include display_name_safe for user-friendly exports
          table_data <- data %>%
            dplyr::select(
              display_name_safe,
              predictor,
              track_multiplier,
              marginal_effect_pct,
              coefficient,
              std_error,      # NEW: Include standard error in export
              conf_low,       # NEW: Include confidence interval
              conf_high,      # NEW: Include confidence interval
              sample_size,    # Moved to better position
              p_value,
              practical_meaning
            ) %>%
            mutate(
              coefficient = round(coefficient, 4),
              std_error = round(std_error, 4),
              # Create confidence interval column for CSV
              confidence_interval = paste0("[", round(conf_low, 2), ", ", round(conf_high, 2), "]"),
              p_value = round(p_value, 4),
              significance = case_when(
                p_value < 0.001 ~ "***",
                p_value < 0.01 ~ "**",
                p_value < 0.05 ~ "*",
                TRUE ~ ""
              )
            ) %>%
            # Select final columns for export
            # PHASE 4: Export display_name as primary, predictor as technical reference
            dplyr::select(
              display_name_safe,
              predictor,
              track_multiplier,
              marginal_effect_pct,
              coefficient,
              std_error,
              confidence_interval,
              sample_size,
              p_value,
              significance,
              practical_meaning
            )

          # 設定中文欄位名稱
          # PHASE 4: Include both user-friendly and technical names
          colnames(table_data) <- sapply(c(
            "Attribute Name", "Technical Name", "Track Multiplier", "Marginal Effect%",
            "Coefficient", "Standard Error", "95% Confidence Interval",
            "Sample Size", "P-Value", "Significance", "Business Meaning"
          ), translate)
        } else {
          # Path 2: Using incidence_rate_ratio
          # PHASE 4: Include display_name_safe for user-friendly exports
          table_data <- data %>%
            dplyr::select(
              display_name_safe,
              predictor,
              track_multiplier,
              marginal_effect_pct,
              incidence_rate_ratio,
              std_error,      # NEW: Include standard error in export
              conf_low,       # NEW: Include confidence interval
              conf_high,      # NEW: Include confidence interval
              sample_size,    # Moved to better position
              p_value,
              practical_meaning
            ) %>%
            mutate(
              incidence_rate_ratio = round(incidence_rate_ratio, 4),
              std_error = round(std_error, 4),
              # Create confidence interval column for CSV
              confidence_interval = paste0("[", round(conf_low, 2), ", ", round(conf_high, 2), "]"),
              p_value = round(p_value, 4),
              significance = case_when(
                p_value < 0.001 ~ "***",
                p_value < 0.01 ~ "**",
                p_value < 0.05 ~ "*",
                TRUE ~ ""
              )
            ) %>%
            # Select final columns for export
            # PHASE 4: Export display_name as primary, predictor as technical reference
            dplyr::select(
              display_name_safe,
              predictor,
              track_multiplier,
              marginal_effect_pct,
              incidence_rate_ratio,
              std_error,
              confidence_interval,
              sample_size,
              p_value,
              significance,
              practical_meaning
            )

          # 設定中文欄位名稱
          # PHASE 4: Include both user-friendly and technical names
          colnames(table_data) <- sapply(c(
            "Attribute Name", "Technical Name", "Track Multiplier", "Marginal Effect%",
            "Incidence Rate Ratio", "Standard Error", "95% Confidence Interval",
            "Sample Size", "P-Value", "Significance", "Business Meaning"
          ), translate)
        }

        # 匯出 CSV with UTF-8 BOM (ISSUE_245)
        # Requires: source("scripts/global_scripts/04_utils/fn_write_utf8_csv.R")
        write_utf8_csv_with_bom(table_data, file)
      }
    )

    # ------------ AI Precision Marketing Insights Generation -------------
    ai_insight_result <- reactiveVal(NULL)
    
    # Get OpenAI API key from environment
    gpt_key <- Sys.getenv("OPENAI_API_KEY", "")
    if (!nzchar(gpt_key)) {
      gpt_key <- NULL
    }
    
    observeEvent(input$generate_precision_insight, {
      data <- positive_data()

      if (is.null(data) || nrow(data) == 0) {
        showNotification("無可用的屬性分析資料", type = "warning")
        return()
      }

      if (is.null(gpt_key)) {
        showNotification("OpenAI API 金鑰未設定。AI 分析功能已停用。", type = "error")
        return()
      }

      # Following UI_R019: AI Process Notification Rule
      # Following MP088: Immediate Feedback Principle
      # Show initial notification without auto-dismiss
      showNotification(
        FEATURE_ANALYSIS_STAGE_MESSAGES$precision_start,
        id = "precision_analysis_progress",
        type = "message",
        duration = NULL,  # Don't auto-dismiss
        closeButton = FALSE
      )

      # MP031: Defensive Programming - wrap in tryCatch
      tryCatch({

        # MP029: NO FAKE DATA - Use ALL real attributes, implement intelligent batching
        # Strategy: Prioritize by impact, batch for GPT-5 processing
        total_attributes <- nrow(data)
        cat("AI分析準備：總共", total_attributes, "個屬性\n")

        # INTELLIGENT CHUNKING STRATEGY:
        # - Top 30 attributes by track_multiplier (战略重点)
        # - GPT-5 can handle ~50-100 attributes with max_output_tokens: 16000
        # - If total < 30, use all
        attributes_to_analyze <- min(total_attributes, 50)

        top_attributes <- data %>%
          slice_head(n = attributes_to_analyze)

        cat("AI分析範圍：前", nrow(top_attributes), "個最重要屬性\n")
        cat("涵蓋率：", round(nrow(top_attributes) / total_attributes * 100, 1), "%\n")

        # Convert to structured format for GPT
        # PHASE 4: Use display_name_safe for user-friendly AI insights
        # Handle both coefficient and incidence_rate_ratio cases
        if ("coefficient" %in% names(top_attributes)) {
          attributes_summary <- data.frame(
            屬性 = top_attributes$display_name_safe,
            技術名稱 = top_attributes$predictor,
            賽道倍數 = top_attributes$track_multiplier,
            邊際效應 = paste0(top_attributes$marginal_effect_pct, "%"),
            商業意義 = top_attributes$practical_meaning,
            係數 = round(top_attributes$coefficient, 4),
            P值 = round(top_attributes$p_value, 4)
          )
        } else {
          attributes_summary <- data.frame(
            屬性 = top_attributes$display_name_safe,
            技術名稱 = top_attributes$predictor,
            賽道倍數 = top_attributes$track_multiplier,
            邊際效應 = paste0(top_attributes$marginal_effect_pct, "%"),
            商業意義 = top_attributes$practical_meaning,
            發生率比 = round(top_attributes$incidence_rate_ratio, 4),
            P值 = round(top_attributes$p_value, 4)
          )
        }

        attributes_json <- jsonlite::toJSON(attributes_summary, dataframe = "rows", auto_unbox = TRUE)

        # OpenAI functions should already be loaded from union_production_test.R
        if (!exists("chat_api")) {
          stop("OpenAI functions not loaded. Please check union_production_test.R initialization.")
        }

        # Load centralized prompt configuration (MP123: AI Prompt Configuration Management)
        prompt_config <- load_openai_prompt("poisson_analysis.precision_marketing_strategy")

        # Replace user template variables only (system_prompt already resolved by load_openai_prompt)
        user_content <- prompt_config$user_prompt_template
        user_content <- gsub("{attributes_summary}", attributes_json, user_content, fixed = TRUE)
        user_content <- gsub("{key_attributes}", attributes_json, user_content, fixed = TRUE)

        # Create messages (system_prompt already resolved by load_openai_prompt, MP032: DRY)
        sys <- list(role = "system", content = prompt_config$system_prompt)
        usr <- list(role = "user", content = user_content)

        # Update notification for AI analysis stage
        # Following UI_R019: Multi-stage notifications for processes > 10s
        showNotification(
          FEATURE_ANALYSIS_STAGE_MESSAGES$precision_analyzing,
          id = "precision_analysis_progress",
          type = "message",
          duration = NULL,
          closeButton = FALSE
        )

        # Use model from centralized config (MP051: Explicit Parameter Specification)
        txt <- chat_api(list(sys, usr), gpt_key, model = prompt_config$model)

        ai_insight_result(txt)

        # Show AI insights section
        shinyjs::show("ai_insights_section")

        # Scroll to AI insights
        shinyjs::runjs(paste0("document.getElementById('", session$ns("ai_insights_section"), "').scrollIntoView({behavior: 'smooth'});"))

        # Remove progress notification and show completion
        # Following MP088: Immediate Feedback Principle
        removeNotification("precision_analysis_progress")
        showNotification(
          FEATURE_ANALYSIS_STAGE_MESSAGES$precision_complete,
          type = "message",
          duration = 3  # Auto-dismiss after 3 seconds
        )

      }, error = function(e) {
        # Following MP031: Defensive Programming
        # Remove progress notification and show error
        removeNotification("precision_analysis_progress")
        showNotification(
          paste(FEATURE_ANALYSIS_STAGE_MESSAGES$error, "：", e$message),
          type = "error",
          duration = 5
        )
      })
    })
    
    # Render AI insights
    output$precision_insight_output <- renderUI({
      txt <- ai_insight_result()
      
      if (is.null(txt)) {
        return(NULL)
      }
      
      # Clean and convert to HTML
      res <- strip_code_fence(txt)
      if (requireNamespace("markdown", quietly = TRUE)) {
        html <- markdown::markdownToHTML(text = res, fragment.only = TRUE)
        HTML(html)
      } else {
        # Fallback
        HTML(paste0("<pre>", res, "</pre>"))
      }
    })
    
    # ------------ AI Product Development Suggestions Generation -------------
    ai_product_result <- reactiveVal(NULL)
    
    observeEvent(input$generate_product_development, {
      data <- positive_data()

      if (is.null(data) || nrow(data) == 0) {
        showNotification("無可用的產品屬性分析資料", type = "warning")
        return()
      }

      if (is.null(gpt_key)) {
        showNotification("OpenAI API 金鑰未設定。AI 分析功能已停用。", type = "error")
        return()
      }

      # Following UI_R019: AI Process Notification Rule
      # Following MP088: Immediate Feedback Principle
      # Show initial notification without auto-dismiss
      showNotification(
        FEATURE_ANALYSIS_STAGE_MESSAGES$product_start,
        id = "product_analysis_progress",
        type = "message",
        duration = NULL,  # Don't auto-dismiss
        closeButton = FALSE
      )

      # MP031: Defensive Programming - wrap in tryCatch
      tryCatch({

        # MP029: NO FAKE DATA - Analyze ALL positive attributes
        # Handle both coefficient and incidence_rate_ratio cases
        if ("coefficient" %in% names(data)) {
          # Path 1: Using coefficient
          positive_vars_all <- data %>%
            filter(coefficient > 0) %>%
            arrange(desc(coefficient))
        } else {
          # Path 2: Using incidence_rate_ratio
          positive_vars_all <- data %>%
            filter(incidence_rate_ratio > 1) %>%
            arrange(desc(incidence_rate_ratio))
        }

        total_positive <- nrow(positive_vars_all)
        cat("新產品開發分析：總共", total_positive, "個正向屬性\n")

        # INTELLIGENT CHUNKING: Analyze top 30 positive attributes
        # This covers the most impactful product features while staying within GPT limits
        attributes_to_analyze <- min(total_positive, 30)

        positive_vars <- positive_vars_all %>%
          slice_head(n = attributes_to_analyze)

        # Debug: Check positive vars data
        cat("分析範圍：前", nrow(positive_vars), "個正向變數\n")
        cat("涵蓋率：", round(nrow(positive_vars) / total_positive * 100, 1), "%\n")
        if (nrow(positive_vars) > 0) {
          cat("前3個正向變數:\n")
          if ("coefficient" %in% names(positive_vars)) {
            print(positive_vars %>% select(predictor, coefficient, track_multiplier, marginal_effect_pct) %>% head(3))
          } else {
            print(positive_vars %>% select(predictor, incidence_rate_ratio, track_multiplier, marginal_effect_pct) %>% head(3))
          }
        }
        
        # Create structured summary for AI analysis
        if (nrow(positive_vars) == 0) {
          product_dev_summary <- "無正向影響屬性資料"
        } else {
          # Handle both coefficient and incidence_rate_ratio cases
          # PHASE 4: Use display_name_safe for user-friendly AI insights
          if ("coefficient" %in% names(positive_vars)) {
            product_dev_summary <- positive_vars %>%
              mutate(
                var_description = paste0(
                  display_name_safe, " (", predictor, "): 係數=", round(coefficient, 3),
                  ", 賽道倍數=", track_multiplier,
                  ", 邊際效應=", marginal_effect_pct, "%"
                )
              ) %>%
              pull(var_description)
          } else {
            product_dev_summary <- positive_vars %>%
              mutate(
                var_description = paste0(
                  display_name_safe, " (", predictor, "): 發生率比=", round(incidence_rate_ratio, 3),
                  ", 賽道倍數=", track_multiplier,
                  ", 邊際效應=", marginal_effect_pct, "%"
                )
              ) %>%
              pull(var_description)
          }
        }
        
        # Debug: Check summary
        cat("產品開發摘要長度:", length(product_dev_summary), "\n")
        if (length(product_dev_summary) > 0) {
          cat("前2個摘要項目:\n")
          cat(paste(head(product_dev_summary, 2), collapse = "\n"), "\n")
        }

        # Use prompt from app_configs
        product_prompt <- app_configs$ai_prompts$poisson_analysis$product_development_strategy

        # Create AI prompt using centralized template
        attributes_text <- paste(product_dev_summary, collapse = "\n")
        cat("要傳給AI的屬性資料:\n", attributes_text, "\n")

        user_content <- gsub("{positive_attributes}",
                            attributes_text,
                            product_prompt$user_prompt_template, fixed = TRUE)

        # Debug: Check if replacement worked
        if (grepl("{positive_attributes}", user_content, fixed = TRUE)) {
          cat("⚠️ Warning: 模板變數替換失敗!\n")
        } else {
          cat("✅ 模板變數替換成功\n")
        }

        # Handle system prompt template variable
        system_content <- gsub("{system_prompts.product_strategist.content}",
                              app_configs$ai_prompts$system_prompts$product_strategist$content,
                              product_prompt$system_prompt, fixed = TRUE)

        sys <- list(role = "system", content = system_content)
        usr <- list(role = "user", content = user_content)

        # Update notification for AI analysis stage
        # Following UI_R019: Multi-stage notifications for processes > 10s
        showNotification(
          FEATURE_ANALYSIS_STAGE_MESSAGES$product_analyzing,
          id = "product_analysis_progress",
          type = "message",
          duration = NULL,
          closeButton = FALSE
        )

        # Use the model specified in YAML configuration
        txt <- chat_api(list(sys, usr), gpt_key, model = product_prompt$model)

        ai_product_result(txt)

        # Show AI product development section
        shinyjs::show("ai_product_development_section")

        # Scroll to AI product development section
        shinyjs::runjs(paste0("document.getElementById('", session$ns("ai_product_development_section"), "').scrollIntoView({behavior: 'smooth'});"))

        # Remove progress notification and show completion
        # Following MP088: Immediate Feedback Principle
        removeNotification("product_analysis_progress")
        showNotification(
          FEATURE_ANALYSIS_STAGE_MESSAGES$product_complete,
          type = "message",
          duration = 3  # Auto-dismiss after 3 seconds
        )

      }, error = function(e) {
        # Following MP031: Defensive Programming
        # Remove progress notification and show error
        removeNotification("product_analysis_progress")
        showNotification(
          paste(FEATURE_ANALYSIS_STAGE_MESSAGES$error, "：", e$message),
          type = "error",
          duration = 5
        )
      })
    })
    
    # Render AI product development suggestions
    output$product_development_output <- renderUI({
      txt <- ai_product_result()
      
      if (is.null(txt)) {
        return(NULL)
      }
      
      # Clean and convert to HTML
      res <- strip_code_fence(txt)
      if (requireNamespace("markdown", quietly = TRUE)) {
        html <- markdown::markdownToHTML(text = res, fragment.only = TRUE)
        HTML(html)
      } else {
        # Fallback
        HTML(paste0("<pre>", res, "</pre>"))
      }
    })
    
    # 返回響應式值
    return(list(
      analysis_data = analysis_data,
      positive_data = positive_data,
      component_status = component_status,
      ai_insight_result = ai_insight_result,
      ai_product_result = ai_product_result
    ))
  })
}

# 組件包裝器 ------------------------------------------------------------------
poissonFeatureAnalysisComponent <- function(id, app_data_connection = NULL, 
                                                       config = NULL, translate = identity) {
  list(
    ui = list(
      filter = poissonFeatureAnalysisFilterUI(id, translate),
      display = poissonFeatureAnalysisUI(id, translate)
    ),
    server = function(input, output, session) {
      poissonFeatureAnalysisServer(id, app_data_connection, config, session, translate = translate)
    }
  )
}
