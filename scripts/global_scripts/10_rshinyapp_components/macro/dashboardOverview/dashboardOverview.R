# =============================================================================
# dashboardOverview.R — Dashboard Overview Component (Issue #327)
# Two panels: Growth Monitoring Engine (12 KPIs) + Customer DNA (12 KPIs)
# CONSUMES: df_dna_by_customer, df_macro_monthly_summary
# Following: UI_R001, UI_R011, UI_R027, MP029, DEV_R001, DEV_R052
# =============================================================================

dashboardOverviewComponent <- function(id, app_connection, comp_config, translate) {
  ns <- NS(id)

  # ---- UI ----
  ui_filter <- tagList()

  panel_header <- function(title_text, icon_name, color) {
    fluidRow(
      column(12,
        div(style = paste0(
          "background: linear-gradient(135deg, ", color, " 0%, ", color, "cc 100%); ",
          "padding: 12px 20px; margin: 15px 0 10px 0; border-radius: 8px; ",
          "color: white; font-size: 18px; font-weight: 600; ",
          "display: flex; align-items: center; gap: 10px;"
        ),
          icon(icon_name, style = "font-size: 22px;"),
          span(title_text)
        )
      )
    )
  }

  ui_display <- tagList(
    # === Panel 1: Growth Monitoring Engine ===
    uiOutput(ns("panel1_header")),
    fluidRow(
      column(3, uiOutput(ns("gm_revenue"))),
      column(3, uiOutput(ns("gm_new_rate"))),
      column(3, uiOutput(ns("gm_repeat_rate"))),
      column(3, uiOutput(ns("gm_churn_rate")))
    ),
    fluidRow(
      column(3, uiOutput(ns("gm_conversion_rate"))),
      column(3, uiOutput(ns("gm_activity"))),
      column(3, uiOutput(ns("gm_s1_repurchase"))),
      column(3, uiOutput(ns("gm_s2_repurchase")))
    ),
    fluidRow(
      column(3, uiOutput(ns("gm_s3_repurchase"))),
      column(3, uiOutput(ns("gm_new_aov"))),
      column(3, uiOutput(ns("gm_core_aov"))),
      column(3, uiOutput(ns("gm_frequency")))
    ),

    # === Panel 2: Customer DNA ===
    uiOutput(ns("panel2_header")),
    fluidRow(
      column(3, uiOutput(ns("dna_value"))),
      column(3, uiOutput(ns("dna_activity"))),
      column(3, uiOutput(ns("dna_new_count"))),
      column(3, uiOutput(ns("dna_e0_count")))
    ),
    fluidRow(
      column(3, uiOutput(ns("dna_s1_count"))),
      column(3, uiOutput(ns("dna_s2_count"))),
      column(3, uiOutput(ns("dna_s3_count"))),
      column(3, uiOutput(ns("dna_ipt")))
    ),
    fluidRow(
      column(3, uiOutput(ns("dna_pcv"))),
      column(3, uiOutput(ns("dna_clv"))),
      column(3, uiOutput(ns("dna_dormancy"))),
      column(3, uiOutput(ns("dna_cri")))
    )
  )

  # ---- Server ----
  server_fn <- function(input, output, session) {
    moduleServer(id, function(input, output, session) {

      # --- Data reactives ---

      dna_data <- reactive({
        cfg <- comp_config()
        req(cfg$filters$platform_id)
        tryCatch({
          pl_id <- cfg$filters$product_line_id_sliced
          if (is.null(pl_id) || pl_id == "all") pl_id <- "all"
          plt <- cfg$filters$platform_id
          df <- tbl2(app_connection, "df_dna_by_customer") %>%
            dplyr::filter(platform_id == !!plt, product_line_id_filter == !!pl_id) %>%
            dplyr::select(nes_status, nt, cai, total_spent, ipt, pcv, clv, p_alive, cri,
                          dna_m_score, dna_f_score, dna_r_score) %>%
            dplyr::collect()
          if (nrow(df) == 0) { message("[dashboardOverview] No DNA data"); return(NULL) }
          message("[dashboardOverview] DNA data: ", nrow(df), " records")
          df
        }, error = function(e) {
          message("[dashboardOverview] DNA load error: ", e$message)
          NULL
        })
      })

      macro_latest <- reactive({
        cfg <- comp_config()
        req(cfg$filters$platform_id)
        tryCatch({
          pl_id <- cfg$filters$product_line_id_sliced
          if (is.null(pl_id) || pl_id == "all") pl_id <- "all"
          plt <- cfg$filters$platform_id
          df <- tbl2(app_connection, "df_macro_monthly_summary") %>%
            dplyr::filter(platform_id == !!plt, product_line_id_filter == !!pl_id) %>%
            dplyr::arrange(dplyr::desc(year_month)) %>%
            dplyr::select(total_revenue, order_count, active_customers, new_customers, avg_order_value) %>%
            head(1) %>%
            dplyr::collect()
          if (nrow(df) == 0) { message("[dashboardOverview] No macro data"); return(NULL) }
          message("[dashboardOverview] Macro latest month loaded")
          df
        }, error = function(e) {
          message("[dashboardOverview] Macro load error: ", e$message)
          NULL
        })
      })

      # #400: all-time total revenue (sum across all monthly rows)
      macro_total_revenue <- reactive({
        cfg <- comp_config()
        req(cfg$filters$platform_id)
        tryCatch({
          pl_id <- cfg$filters$product_line_id_sliced
          if (is.null(pl_id) || pl_id == "all") pl_id <- "all"
          plt <- cfg$filters$platform_id
          df <- tbl2(app_connection, "df_macro_monthly_summary") %>%
            dplyr::filter(platform_id == !!plt, product_line_id_filter == !!pl_id) %>%
            dplyr::summarise(total_revenue = sum(total_revenue, na.rm = TRUE)) %>%
            dplyr::collect()
          if (nrow(df) == 0) { message("[dashboardOverview] No total revenue data"); return(NULL) }
          df
        }, error = function(e) {
          message("[dashboardOverview] Total revenue load error: ", e$message)
          NULL
        })
      })

      # --- Helper: create value box ---
      make_vbox <- function(val, subtitle, icon_name, color) {
        bs4ValueBox(value = val, subtitle = subtitle,
                    icon = icon(icon_name), color = color, width = 12)
      }

      fmt_pct <- function(x) if (is.null(x) || is.na(x) || is.nan(x)) "-" else paste0(round(x, 1), "%")
      fmt_num <- function(x) if (is.null(x) || is.na(x) || is.nan(x)) "-" else format(round(x, 0), big.mark = ",")
      fmt_dec <- function(x, d = 2) if (is.null(x) || is.na(x) || is.nan(x)) "-" else round(x, d)

      # --- Panel headers ---

      output$panel1_header <- renderUI({
        panel_header(translate("Growth Monitoring Engine"), "chart-line", "#007bff")
      })

      output$panel2_header <- renderUI({
        panel_header(translate("Customer DNA Overview"), "dna", "#6f42c1")
      })

      # =======================================================================
      # Panel 1: Growth Monitoring Engine (12 KPIs)
      # =======================================================================

      # 1. Total revenue (all-time, #400)
      output$gm_revenue <- renderUI({
        m <- macro_total_revenue()
        val <- if (!is.null(m)) fmt_num(m$total_revenue[1]) else "-"
        make_vbox(val, translate("Total Revenue"), "dollar-sign", "primary")
      })

      # 2. New customer rate
      output$gm_new_rate <- renderUI({
        m <- macro_latest()
        val <- if (!is.null(m) && m$active_customers[1] > 0) {
          fmt_pct(m$new_customers[1] / m$active_customers[1] * 100)
        } else "-"
        make_vbox(val, translate("New Customer Rate"), "user-plus", "info")
      })

      # 3. Repeat purchase rate
      output$gm_repeat_rate <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df) && nrow(df) > 0) {
          fmt_pct(sum(df$nt >= 2, na.rm = TRUE) / nrow(df) * 100)
        } else "-"
        make_vbox(val, translate("Repeat Purchase Rate"), "redo", "success")
      })

      # 4. Churn rate: (S2+S3) / (E0+S1+S2+S3)
      output$gm_churn_rate <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df)) {
          existing <- sum(df$nes_status %in% c("E0", "S1", "S2", "S3"), na.rm = TRUE)
          churned <- sum(df$nes_status %in% c("S2", "S3"), na.rm = TRUE)
          if (existing > 0) fmt_pct(churned / existing * 100) else "-"
        } else "-"
        make_vbox(val, translate("Churn Rate"), "user-minus", "danger")
      })

      # 5. Conversion rate (#320): repeat buyers (nt>=2) / total customers
      output$gm_conversion_rate <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df) && nrow(df) > 0) {
          n_repeat <- sum(df$nt >= 2, na.rm = TRUE)
          fmt_pct(n_repeat / nrow(df) * 100)
        } else "-"
        make_vbox(val, translate("Conversion Rate"), "exchange-alt", "purple")
      })

      # 6. Activity (avg CAI)
      output$gm_activity <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df)) fmt_dec(mean(df$cai, na.rm = TRUE), 3) else "-"
        make_vbox(val, translate("Activity (CAI)"), "bolt", "olive")
      })

      # 7. S1 (Drowsy) repurchase rate
      output$gm_s1_repurchase <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df)) {
          s1 <- df[df$nes_status == "S1", , drop = FALSE]
          if (nrow(s1) > 0) fmt_pct(sum(s1$nt >= 2, na.rm = TRUE) / nrow(s1) * 100) else "-"
        } else "-"
        make_vbox(val, translate("S1 Repurchase Rate"), "bed", "warning")
      })

      # 8. S2 (Half-sleep) repurchase rate
      output$gm_s2_repurchase <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df)) {
          s2 <- df[df$nes_status == "S2", , drop = FALSE]
          if (nrow(s2) > 0) fmt_pct(sum(s2$nt >= 2, na.rm = TRUE) / nrow(s2) * 100) else "-"
        } else "-"
        make_vbox(val, translate("S2 Repurchase Rate"), "moon", "orange")
      })

      # 9. S3 (Dormant) repurchase rate
      output$gm_s3_repurchase <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df)) {
          s3 <- df[df$nes_status == "S3", , drop = FALSE]
          if (nrow(s3) > 0) fmt_pct(sum(s3$nt >= 2, na.rm = TRUE) / nrow(s3) * 100) else "-"
        } else "-"
        make_vbox(val, translate("S3 Repurchase Rate"), "power-off", "danger")
      })

      # 10. New customer AOV
      output$gm_new_aov <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df)) {
          new_cust <- df[df$nes_status == "N", , drop = FALSE]
          if (nrow(new_cust) > 0) fmt_num(mean(new_cust$total_spent, na.rm = TRUE)) else "-"
        } else "-"
        make_vbox(val, translate("New Customer AOV"), "tag", "info")
      })

      # 11. Core customer AOV
      output$gm_core_aov <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df)) {
          e0 <- df[df$nes_status == "E0", , drop = FALSE]
          if (nrow(e0) > 0) fmt_num(mean(e0$total_spent, na.rm = TRUE)) else "-"
        } else "-"
        make_vbox(val, translate("Core Customer AOV"), "gem", "purple")
      })

      # 12. Purchase frequency (avg nt)
      output$gm_frequency <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df)) fmt_dec(mean(df$nt, na.rm = TRUE), 1) else "-"
        make_vbox(val, translate("Avg Purchase Frequency"), "shopping-cart", "teal")
      })

      # =======================================================================
      # Panel 2: Customer DNA (12 KPIs)
      # =======================================================================

      # 1. Customer value (avg RFM score)
      output$dna_value <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df)) {
          rfm <- rowMeans(cbind(df$dna_m_score, df$dna_f_score, df$dna_r_score), na.rm = TRUE)
          fmt_dec(mean(rfm, na.rm = TRUE), 2)
        } else "-"
        make_vbox(val, translate("Customer Value (RFM)"), "star", "primary")
      })

      # 2. Customer activity (avg CAI)
      output$dna_activity <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df)) fmt_dec(mean(df$cai, na.rm = TRUE), 3) else "-"
        make_vbox(val, translate("Customer Activity (CAI)"), "bolt", "olive")
      })

      # 3. New customer count
      output$dna_new_count <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df)) fmt_num(sum(df$nes_status == "N", na.rm = TRUE)) else "-"
        make_vbox(val, translate("New Customer Count"), "user-plus", "info")
      })

      # 4. Core customer count (E0)
      output$dna_e0_count <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df)) fmt_num(sum(df$nes_status == "E0", na.rm = TRUE)) else "-"
        make_vbox(val, translate("Core Customer Count"), "gem", "success")
      })

      # 5. S1 (Drowsy) count
      output$dna_s1_count <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df)) fmt_num(sum(df$nes_status == "S1", na.rm = TRUE)) else "-"
        make_vbox(val, translate("S1 Customer Count"), "bed", "warning")
      })

      # 6. S2 (Half-sleep) count
      output$dna_s2_count <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df)) fmt_num(sum(df$nes_status == "S2", na.rm = TRUE)) else "-"
        make_vbox(val, translate("S2 Customer Count"), "moon", "orange")
      })

      # 7. S3 (Dormant) count
      output$dna_s3_count <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df)) fmt_num(sum(df$nes_status == "S3", na.rm = TRUE)) else "-"
        make_vbox(val, translate("S3 Customer Count"), "power-off", "danger")
      })

      # 8. Purchase cycle (avg IPT)
      output$dna_ipt <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df)) {
          avg_ipt <- mean(df$ipt, na.rm = TRUE)
          if (is.nan(avg_ipt) || is.na(avg_ipt)) "-" else paste0(round(avg_ipt, 1), translate("days"))
        } else "-"
        make_vbox(val, translate("Purchase Cycle (IPT)"), "clock", "indigo")
      })

      # 9. Past customer value (avg PCV)
      output$dna_pcv <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df)) fmt_num(mean(df$pcv, na.rm = TRUE)) else "-"
        make_vbox(val, translate("Past Customer Value (PCV)"), "history", "primary")
      })

      # 10. CLV (avg)
      output$dna_clv <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df)) fmt_num(mean(df$clv, na.rm = TRUE)) else "-"
        make_vbox(val, translate("CLV"), "infinity", "success")
      })

      # 11. Dormancy probability (avg 1 - p_alive)
      output$dna_dormancy <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df) && !all(is.na(df$p_alive))) {
          fmt_pct(mean(1 - df$p_alive, na.rm = TRUE) * 100)
        } else "-"
        make_vbox(val, translate("Dormancy Probability"), "skull-crossbones", "danger")
      })

      # 12. Transaction stability (avg CRI)
      output$dna_cri <- renderUI({
        df <- dna_data()
        val <- if (!is.null(df)) fmt_dec(mean(df$cri, na.rm = TRUE), 3) else "-"
        make_vbox(val, translate("Customer Retention Index (CRI)"), "balance-scale", "teal")
      })

    })
  }

  list(ui = list(filter = ui_filter, display = ui_display), server = server_fn)
}
