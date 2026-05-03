# =============================================================================
# customerRetention.R — Customer Retention Component (VitalSigns)
# CONSUMES: df_dna_by_customer (from D01)
# Following: UI_R001, UI_R011, MP064, MP029, DEV_R001
# Issue #274: Added repurchase conversion rate, category repurchase rate, awakening potential
# =============================================================================

customerRetentionComponent <- function(id, app_connection, comp_config, translate) {
  ns <- NS(id)

  # ---- UI ----
  ui_filter <- tagList(
    ai_insight_button_ui(ns, translate)
  )

  ui_display <- tagList(
    # KPI Row 1: Churn + Repurchase metrics (#290: removed kpi_retention)
    fluidRow(
      column(4, uiOutput(ns("kpi_churn"))),
      column(4, uiOutput(ns("kpi_repurchase"))),
      column(4, uiOutput(ns("kpi_core_ratio")))
    ),
    # KPI Row 2: NES counts
    fluidRow(
      column(2, uiOutput(ns("kpi_n"))),
      column(2, uiOutput(ns("kpi_e0"))),
      column(2, uiOutput(ns("kpi_s1"))),
      column(2, uiOutput(ns("kpi_s2"))),
      column(2, uiOutput(ns("kpi_s3"))),
      column(2, uiOutput(ns("kpi_dormant_pred")))
    ),
    # Charts Row 1: NES structure + Churn risk
    fluidRow(
      column(6, bs4Card(title = translate("Customer Structure (NES)"), status = "primary",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("structure_bar"), height = "350px"))),
      column(6, bs4Card(title = translate("Churn Risk Analysis"), status = "danger",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("risk_stacked"), height = "350px")))
    ),
    # Charts Row 2: Category repurchase rate + Dormant repurchase rate (#274, #292)
    fluidRow(
      column(6, bs4Card(title = translate("Category Repurchase Rate"), status = "info",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("category_repurchase"), height = "350px"))),
      column(6, bs4Card(title = translate("Awakening Potential by Status"), status = "warning",
        width = 12, solidHeader = TRUE, plotly::plotlyOutput(ns("awakening_chart"), height = "350px")))
    ),
    # Strategy tabs
    fluidRow(
      column(12, bs4Card(title = translate("Retention Strategy by Status"), status = "success",
        width = 12, solidHeader = TRUE,
        tabsetPanel(
          tabPanel(translate("Retention Overview"), uiOutput(ns("tab_overview"))),
          tabPanel(translate("New Customer"), uiOutput(ns("tab_new"))),
          tabPanel(translate("Core Customer"), uiOutput(ns("tab_core"))),
          tabPanel(translate("Drowsy"), uiOutput(ns("tab_s1"))),
          tabPanel(translate("Half-Sleeping"), uiOutput(ns("tab_s2"))),
          tabPanel(translate("Dormant"), uiOutput(ns("tab_s3")))
        )
      ))
    ),
    # AI Insight Result — bottom of display (GUIDE03)
    fluidRow(
      column(12, ai_insight_result_ui(ns, translate))
    )
  )

  # ---- Server ----
  server_fn <- function(input, output, session) {
    moduleServer(id, function(input, output, session) {

      # #376: Migrated from raw DBI::dbGetQuery() to tbl2() + dbplyr per
      # DM_R023 v1.2. Country filter applied via semi_join when active.
      dna_data <- reactive({
        cfg <- comp_config()
        req(cfg$filters$platform_id)
        tryCatch({
          plt <- cfg$filters$platform_id
          country <- cfg$filters$country
          pl_id <- cfg$filters$product_line_id_sliced
          if (is.null(pl_id) || pl_id == "all") pl_id <- "all"

          dna_lazy <- tbl2(app_connection, "df_dna_by_customer") %>%
            dplyr::filter(platform_id == !!plt,
                          product_line_id_filter == !!pl_id)

          if (!is.null(country) && country != "all") {
            country_map_lazy <- tbl2(app_connection, "df_customer_country_map") %>%
              dplyr::filter(platform_id == !!plt, ship_country == !!country) %>%
              dplyr::select(customer_id)
            dna_lazy <- dna_lazy %>%
              dplyr::semi_join(country_map_lazy, by = "customer_id")
          }

          df <- dna_lazy %>%
            dplyr::select(customer_id, nes_status, nrec_prob, be2,
                          m_value, total_spent, ni, f_value, clv,
                          cri, r_value, p_alive, ipt) %>%
            dplyr::collect()

          if (nrow(df) == 0) { message("[customerRetention] No data returned"); return(NULL) }

          # Preserve legacy column aliases used by downstream reactives
          df <- dplyr::rename(df,
                              be2_prob = be2,
                              spent_total = total_spent,
                              clv_value = clv,
                              cri_value = cri)

          message("[customerRetention] Loaded ", nrow(df), " records | cols: ", paste(names(df), collapse=", "))
          message("[customerRetention] UI targets: kpi_churn, kpi_at_risk, kpi_core_ratio, kpi_n/e0/s1/s2/s3/dormant_pred, structure_bar, risk_stacked, tab_overview/new/core/s1/s2/s3")
          df
        }, error = function(e) {
          message("[customerRetention] Data load error: ", e$message)
          NULL
        })
      })

      # NES counts helper
      nes_counts <- reactive({
        df <- dna_data()
        if (is.null(df)) return(list(N = 0, E0 = 0, S1 = 0, S2 = 0, S3 = 0, total = 0))
        list(
          N  = sum(df$nes_status == "N", na.rm = TRUE),
          E0 = sum(df$nes_status == "E0", na.rm = TRUE),
          S1 = sum(df$nes_status == "S1", na.rm = TRUE),
          S2 = sum(df$nes_status == "S2", na.rm = TRUE),
          S3 = sum(df$nes_status == "S3", na.rm = TRUE),
          total = nrow(df)
        )
      })

      # Time-windowed repurchase helper (#316)
      # W = 1.5 × median(ipt); "converted" if ni > 1 AND ipt <= W
      # Used when viewing a specific product line slice. For "all" slice, see
      # overall_repurchase_data() which aggregates per-category windows via SQL.
      # Fallback to ni > 1 only when < 2 valid ipt values (cannot compute median).
      compute_repurchase <- function(df) {
        valid_ipt <- df$ipt[!is.na(df$ipt) & df$ipt > 0 & df$ni > 1]
        if (length(valid_ipt) >= 2) {
          med <- median(valid_ipt)
          window <- 1.5 * med
          n_repeat <- sum(df$ni > 1 & !is.na(df$ipt) & df$ipt <= window, na.rm = TRUE)
          list(n_repeat = n_repeat, total = nrow(df), median_ipt = med, window = window, method = "time_windowed")
        } else {
          n_repeat <- sum(df$ni > 1, na.rm = TRUE)
          list(n_repeat = n_repeat, total = nrow(df), median_ipt = NA_real_, window = NA_real_, method = "simple")
        }
      }

      # Overall repurchase rate via per-category windows (#316, P2 fix)
      # When viewing "all", sums per-category time-windowed repeat buyers for accuracy.
      # Each customer evaluated against their category's median IPT, not a global median.
      #
      # #376: Migrated from raw DBI::dbGetQuery() with DuckDB-only MEDIAN() to
      # tbl2() + dbplyr, so dbplyr translates median() to driver-specific SQL
      # (DuckDB MEDIAN, PostgreSQL percentile_cont). DM_R023 v1.2 compliance.
      overall_repurchase_data <- reactive({
        cfg <- comp_config()
        req(cfg$filters$platform_id)
        pl <- cfg$filters$product_line_id_sliced
        # For specific category, use compute_repurchase (category median = global median)
        if (!is.null(pl) && pl != "all") {
          df <- dna_data()
          if (is.null(df) || nrow(df) == 0) return(list(n_repeat = 0, total = 0, method = "no_data"))
          return(compute_repurchase(df))
        }
        # For "all": aggregate per-category results via dbplyr
        tryCatch({
          plt <- cfg$filters$platform_id
          country <- cfg$filters$country

          # Base lazy query: df_dna_by_customer for this platform
          dna_lazy <- tbl2(app_connection, "df_dna_by_customer") %>%
            dplyr::filter(platform_id == !!plt)

          # Sync country filter with dna_data() (#316 P2 fix)
          if (!is.null(country) && country != "all") {
            country_map_lazy <- tbl2(app_connection, "df_customer_country_map") %>%
              dplyr::filter(platform_id == !!plt, ship_country == !!country) %>%
              dplyr::select(customer_id)
            dna_lazy <- dna_lazy %>%
              dplyr::semi_join(country_map_lazy, by = "customer_id")
          }

          # Step 1: per-category median IPT (excluding all/unclassified slices)
          medians_lazy <- dna_lazy %>%
            dplyr::filter(product_line_id_filter != "all",
                          product_line_id_filter != "unclassified",
                          ni > 1, ipt > 0) %>%
            dplyr::group_by(product_line_id_filter) %>%
            dplyr::summarise(median_ipt = median(ipt, na.rm = TRUE),
                             .groups = "drop")

          # Step 2: count customers whose ipt is within 1.5x category median
          repeat_count <- dna_lazy %>%
            dplyr::filter(product_line_id_filter != "all",
                          product_line_id_filter != "unclassified") %>%
            dplyr::inner_join(medians_lazy, by = "product_line_id_filter") %>%
            dplyr::filter(ni > 1, ipt > 0, ipt <= 1.5 * median_ipt) %>%
            dplyr::summarise(repeat_buyers = dplyr::n()) %>%
            dplyr::collect()

          # Step 3: total = count of customers in 'all' product line slice
          total_count <- dna_lazy %>%
            dplyr::filter(product_line_id_filter == "all") %>%
            dplyr::summarise(total = dplyr::n()) %>%
            dplyr::collect()

          if (nrow(repeat_count) == 0 || nrow(total_count) == 0 ||
              is.na(total_count$total[1]) || total_count$total[1] == 0 ||
              is.na(repeat_count$repeat_buyers[1])) {
            return(list(n_repeat = 0, total = 0, median_ipt = NA_real_, window = NA_real_, method = "no_data"))
          }
          list(
            n_repeat = as.integer(repeat_count$repeat_buyers[1]),
            total = as.integer(total_count$total[1]),
            median_ipt = NA_real_,
            window = NA_real_,
            method = "per_category_windowed"
          )
        }, error = function(e) {
          message("[customerRetention] Overall repurchase error: ", e$message)
          df <- dna_data()
          if (is.null(df) || nrow(df) == 0) return(list(n_repeat = 0, total = 0, median_ipt = NA_real_, window = NA_real_, method = "error_fallback"))
          compute_repurchase(df)
        })
      })

      # KPIs Row 1 (#290: removed kpi_retention)
      # #314: Churn Rate = (S1+S2+S3)/(E0+S1+S2+S3), excluding N (new customers)
      output$kpi_churn <- renderUI({
        nc <- nes_counts()
        sleeping <- nc$S1 + nc$S2 + nc$S3
        existing <- nc$E0 + sleeping
        pct <- if (existing > 0) round(sleeping / existing * 100, 1) else 0
        bs4ValueBox(value = paste0(pct, "%"), subtitle = translate("Churn Rate"),
                    icon = icon("user-minus"), color = "danger", width = 12)
      })

      # Repurchase conversion rate KPI (#274, #316): time-windowed via overall_repurchase_data()
      output$kpi_repurchase <- renderUI({
        rp <- overall_repurchase_data()
        if (rp$total == 0) return(bs4ValueBox(value = "-", subtitle = translate("Repurchase Rate"),
                                            icon = icon("sync-alt"), color = "warning", width = 12))
        pct <- round(rp$n_repeat / max(rp$total, 1) * 100, 1)
        bs4ValueBox(value = paste0(pct, "%"), subtitle = translate("Repurchase Rate"),
                    icon = icon("sync-alt"), color = "warning", width = 12)
      })

      output$kpi_core_ratio <- renderUI({
        nc <- nes_counts()
        pct <- if (nc$total > 0) round(nc$E0 / nc$total * 100, 1) else 0
        bs4ValueBox(value = paste0(pct, "%"), subtitle = translate("Core Customer Ratio"),
                    icon = icon("star"), color = "info", width = 12)
      })

      # KPIs Row 2: NES status counts
      render_nes_kpi <- function(status, label, color, icon_name) {
        renderUI({
          nc <- nes_counts()
          bs4ValueBox(value = nc[[status]], subtitle = label,
                      icon = icon(icon_name), color = color, width = 12)
        })
      }

      output$kpi_n  <- render_nes_kpi("N", translate("N (New)"), "info", "user-plus")
      output$kpi_e0 <- render_nes_kpi("E0", translate("E0 (Core)"), "success", "user-check")
      output$kpi_s1 <- render_nes_kpi("S1", translate("S1 (Drowsy)"), "warning", "moon")
      output$kpi_s2 <- render_nes_kpi("S2", translate("S2 (Half-Sleep)"), "orange", "bed")
      output$kpi_s3 <- render_nes_kpi("S3", translate("S3 (Dormant)"), "danger", "skull")
      output$kpi_dormant_pred <- renderUI({
        df <- dna_data()
        if (is.null(df)) return(bs4ValueBox(value = "-", subtitle = translate("Predicted Dormant"),
                                            icon = icon("chart-line"), color = "secondary", width = 12))
        # #312: IPT-based high risk count (replacing nrec_prob > 0.7)
        valid_ipt <- !is.na(df$ipt) & df$ipt > 0 & !is.na(df$r_value)
        n_pred <- sum(valid_ipt & df$r_value > df$ipt * 2.5, na.rm = TRUE)
        bs4ValueBox(value = n_pred, subtitle = translate("Predicted Dormant"),
                    icon = icon("chart-line"), color = "secondary", width = 12)
      })

      # Customer structure bar
      output$structure_bar <- renderPlotly({
        nc <- nes_counts()
        if (nc$total == 0) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))

        status_df <- data.frame(
          status = c("N", "E0", "S1", "S2", "S3"),
          label = c(translate("New"), translate("Core"), translate("Drowsy"),
                    translate("Half-Sleeping"), translate("Dormant")),
          count = c(nc$N, nc$E0, nc$S1, nc$S2, nc$S3),
          stringsAsFactors = FALSE
        )
        status_df$label <- factor(status_df$label, levels = status_df$label)
        colors <- c("#17a2b8", "#28a745", "#ffc107", "#fd7e14", "#dc3545")

        plotly::plot_ly(status_df, x = ~label, y = ~count, type = "bar",
                        marker = list(color = colors),
                        text = ~paste0(count, " (", round(count / nc$total * 100, 1), "%)"),
                        textposition = "outside") %>%
          plotly::layout(xaxis = list(title = ""), yaxis = list(title = translate("Customer Count")))
      })

      # Churn risk stacked bar
      output$risk_stacked <- renderPlotly({
        df <- dna_data()
        if (is.null(df)) return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))

        # #312: IPT-based risk tiers — r_value vs ipt multiples
        # DEV_R052: English canonical keys in business logic
        df$risk_level <- ifelse(
          is.na(df$ipt) | df$ipt <= 0 | is.na(df$r_value), "Unknown",
          ifelse(df$r_value > df$ipt * 2.5, "High Risk",
                 ifelse(df$r_value > df$ipt * 1.5, "Medium Risk", "Low Risk"))
        )

        tbl <- as.data.frame(table(df$nes_status, df$risk_level), stringsAsFactors = FALSE)
        names(tbl) <- c("NES", "Risk", "Count")
        # DEV_R052: translate at UI render layer
        tbl$Risk <- sapply(tbl$Risk, translate)

        risk_colors <- setNames(
          c("#28a745", "#ffc107", "#dc3545", "#adb5bd"),
          c(translate("Low Risk"), translate("Medium Risk"), translate("High Risk"), translate("Unknown"))
        )

        plotly::plot_ly(tbl, x = ~NES, y = ~Count, color = ~Risk, type = "bar",
                        colors = risk_colors) %>%
          plotly::layout(barmode = "stack",
                         xaxis = list(title = translate("Customer Status")),
                         yaxis = list(title = translate("Customer Count")))
      })

      # Category repurchase rate data (#274, #316): time-windowed per category
      # W_i = 1.5 × median(ipt) per category; repeat = ni > 1 AND ipt <= W_i
      #
      # #376: Migrated from raw DBI::dbGetQuery() with DuckDB-only MEDIAN() to
      # tbl2() + dbplyr per DM_R023 v1.2; dbplyr translates median() to
      # driver-specific SQL.
      category_data <- reactive({
        cfg <- comp_config()
        req(cfg$filters$platform_id)
        tryCatch({
          plt <- cfg$filters$platform_id

          dna_lazy <- tbl2(app_connection, "df_dna_by_customer") %>%
            dplyr::filter(platform_id == !!plt,
                          product_line_id_filter != "all",
                          product_line_id_filter != "unclassified")

          # Per-category median IPT (only valid repeat buyers contribute)
          medians_lazy <- dna_lazy %>%
            dplyr::filter(ni > 1, ipt > 0) %>%
            dplyr::group_by(product_line_id_filter) %>%
            dplyr::summarise(median_ipt = median(ipt, na.rm = TRUE),
                             .groups = "drop")

          # Join median back, count totals + repeat buyers per category
          result <- dna_lazy %>%
            dplyr::inner_join(medians_lazy, by = "product_line_id_filter") %>%
            dplyr::group_by(product_line_id_filter, median_ipt) %>%
            dplyr::summarise(
              total = dplyr::n(),
              repeat_buyers = sum(
                dplyr::if_else(ni > 1 & ipt > 0 & ipt <= 1.5 * median_ipt, 1L, 0L),
                na.rm = TRUE
              ),
              .groups = "drop"
            ) %>%
            dplyr::filter(total >= 10) %>%
            dplyr::arrange(dplyr::desc(total)) %>%
            dplyr::collect()

          if (is.null(result) || nrow(result) == 0) return(NULL)

          # Rename for backward compat with downstream callers (was `category` column)
          result <- dplyr::rename(result, category = product_line_id_filter)
          as.data.frame(result)
        }, error = function(e) {
          message("[customerRetention] Category data error: ", e$message)
          NULL
        })
      })

      # Category repurchase rate bar chart (#274)
      output$category_repurchase <- renderPlotly({
        cat_df <- category_data()
        if (is.null(cat_df) || nrow(cat_df) == 0) {
          return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))
        }
        cat_df$rate <- round(cat_df$repeat_buyers / cat_df$total * 100, 1)
        cat_df <- cat_df[order(-cat_df$rate), ]
        cat_df$category_display <- sapply(cat_df$category, translate)
        cat_df$category_display <- factor(cat_df$category_display, levels = cat_df$category_display)

        # #316: Show time window in tooltip when median_ipt available
        cat_df$tooltip <- if ("median_ipt" %in% names(cat_df) && any(!is.na(cat_df$median_ipt))) {
          paste0(cat_df$rate, "% (", cat_df$repeat_buyers, "/", cat_df$total,
                 ", W=", round(1.5 * cat_df$median_ipt, 0), translate("days"), ")")
        } else {
          paste0(cat_df$rate, "% (", cat_df$repeat_buyers, "/", cat_df$total, ")")
        }

        plotly::plot_ly(cat_df, x = ~category_display, y = ~rate, type = "bar",
                        marker = list(color = "#17a2b8"),
                        text = ~tooltip,
                        textposition = "outside") %>%
          plotly::layout(
            xaxis = list(title = translate("Product Category")),
            yaxis = list(title = translate("Repurchase Rate (%)"), range = c(0, max(cat_df$rate, na.rm = TRUE) * 1.15))
          )
      })

      # Dormant repurchase rate chart (#274, #292): mean p_alive per NES sleeping segment
      output$awakening_chart <- renderPlotly({
        df <- dna_data()
        if (is.null(df) || nrow(df) == 0 || !("p_alive" %in% names(df))) {
          return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))
        }

        sleeping_df <- df[df$nes_status %in% c("S1", "S2", "S3"), ]
        if (nrow(sleeping_df) == 0) {
          return(plotly::plot_ly() %>% plotly::layout(title = translate("No Sleeping Customers")))
        }

        awaken_stats <- do.call(rbind, lapply(c("S1", "S2", "S3"), function(s) {
          sub <- sleeping_df[sleeping_df$nes_status == s, ]
          if (nrow(sub) == 0) return(NULL)
          data.frame(
            status = s,
            label = switch(s,
              "S1" = translate("S1 (Drowsy)"),
              "S2" = translate("S2 (Half-Sleep)"),
              "S3" = translate("S3 (Dormant)")
            ),
            n = nrow(sub),
            mean_p_alive = round(mean(sub$p_alive, na.rm = TRUE) * 100, 1),
            stringsAsFactors = FALSE
          )
        }))
        if (is.null(awaken_stats) || nrow(awaken_stats) == 0) {
          return(plotly::plot_ly() %>% plotly::layout(title = translate("No Data")))
        }
        awaken_stats$label <- factor(awaken_stats$label, levels = awaken_stats$label)

        colors <- c("#ffc107", "#fd7e14", "#dc3545")

        plotly::plot_ly(awaken_stats, x = ~label, y = ~mean_p_alive, type = "bar",
                        marker = list(color = colors[seq_len(nrow(awaken_stats))]),
                        text = ~paste0(mean_p_alive, "% (n=", n, ")"),
                        textposition = "outside") %>%
          plotly::layout(
            xaxis = list(title = ""),
            yaxis = list(title = translate("Awakening Potential (%)"), range = c(0, 100))
          )
      })

      # Load marketing strategies from YAML (DEV_R050: externalized display data)
      strategies_yaml <- tryCatch({
        yaml_path <- file.path(GLOBAL_DIR, "30_global_data", "parameters", "marketing_strategies.yaml")
        if (file.exists(yaml_path)) yaml::read_yaml(yaml_path)$strategies else NULL
      }, error = function(e) { message("[customerRetention] YAML load: ", e$message); NULL })

      # Rich strategy tab renderer
      render_rich_tab <- function(title, subtitle, metrics_fn, strategy_keys) {
        renderUI({
          df <- dna_data()
          if (is.null(df)) return(tags$p(translate("No data available")))
          metrics <- metrics_fn(df)

          strategy_cards <- if (!is.null(strategies_yaml)) {
            lapply(strategy_keys, function(key) {
              s <- strategies_yaml[[key]]
              if (is.null(s)) return(NULL)
              rec_html <- gsub("<br>\\s*", "<br>", s$recommendation)
              tags$div(class = "card mb-3",
                tags$div(class = "card-header bg-light",
                  tags$strong(key), tags$span(class = "badge bg-info ms-2 ml-2", s$purpose)
                ),
                tags$div(class = "card-body", tags$p(HTML(rec_html)))
              )
            })
          }

          tags$div(
            tags$h5(title),
            tags$p(class = "text-muted", subtitle),
            tags$div(class = "row mb-3",
              lapply(metrics, function(m) {
                tags$div(class = "col-auto",
                  tags$div(class = "border rounded p-2 text-center",
                    tags$strong(m$value), tags$br(), tags$small(class = "text-muted", m$label)
                  ))
              })
            ),
            if (length(strategy_cards) > 0) tagList(
              tags$h6(class = "mt-3", icon("bullhorn"), " ", translate("Marketing Strategy")),
              tagList(strategy_cards)
            )
          )
        })
      }

      output$tab_overview <- render_rich_tab(
        translate("Retention Analysis Overview"),
        translate("Overall customer retention status and key metrics"),
        function(df) {
          nc <- nes_counts()
          # #314: Exclude N (new customers) from retention/churn denominator
          existing <- nc$E0 + nc$S1 + nc$S2 + nc$S3
          sleeping <- nc$S1 + nc$S2 + nc$S3
          rp <- overall_repurchase_data()
          repurchase_pct <- round(rp$n_repeat / max(rp$total, 1) * 100, 1)
          list(
            list(value = format(nc$total, big.mark = ","), label = translate("Total Customers")),
            list(value = paste0(round(nc$E0 / max(existing, 1) * 100, 1), "%"), label = translate("Retention Rate")),
            list(value = paste0(repurchase_pct, "%"), label = translate("Repurchase Rate")),
            # #312: IPT-based risk metrics (replacing nrec_prob)
            list(value = {
              valid <- !is.na(df$ipt) & df$ipt > 0
              if (sum(valid) > 0) paste0(round(mean(df$ipt[valid], na.rm = TRUE), 1), " ", translate("days")) else translate("N/A")
            }, label = translate("IPT")),
            list(value = sum(!is.na(df$ipt) & df$ipt > 0 & !is.na(df$r_value) & df$r_value > df$ipt * 2.5, na.rm = TRUE), label = translate("High Churn Risk")),
            list(value = paste0("$", format(round(mean(df$clv_value, na.rm = TRUE), 0), big.mark = ",")), label = translate("Avg CLV"))
          )
        },
        character(0)
      )

      output$tab_new <- render_rich_tab(
        translate("New Customer Strategy"),
        translate("Convert one-time buyers into repeat customers — key period for building loyalty"),
        function(df) {
          new_df <- df[df$nes_status == "N", ]
          list(
            list(value = format(nrow(new_df), big.mark = ","), label = translate("New Customers")),
            list(value = paste0(round(nrow(new_df) / max(nrow(df), 1) * 100, 1), "%"), label = translate("% of Total")),
            list(value = paste0("$", format(round(mean(new_df$m_value, na.rm = TRUE), 0), big.mark = ",")), label = translate("Avg Spend")),
            list(value = round(mean(new_df$nrec_prob, na.rm = TRUE), 3), label = translate("Avg Churn Prob"))
          )
        },
        c("New Customer Nurturing")
      )

      output$tab_core <- render_rich_tab(
        translate("Core Customer Deepening"),
        translate("Strengthen relationships and increase lifetime value of active customers"),
        function(df) {
          core_df <- df[df$nes_status == "E0", ]
          list(
            list(value = format(nrow(core_df), big.mark = ","), label = translate("Core Customers")),
            list(value = paste0("$", format(round(mean(core_df$clv_value, na.rm = TRUE), 0), big.mark = ",")), label = translate("Avg CLV")),
            list(value = paste0("$", format(round(mean(core_df$spent_total, na.rm = TRUE), 0), big.mark = ",")), label = translate("Avg Total Spent")),
            list(value = round(mean(core_df$f_value, na.rm = TRUE), 1), label = translate("Avg Frequency"))
          )
        },
        c("Standard Nurturing (Core)", "Standard Nurturing (Advanced)")
      )

      output$tab_s1 <- render_rich_tab(
        translate("Drowsy Customer Awakening"),
        translate("Re-engage customers showing early signs of inactivity — highest recovery potential"),
        function(df) {
          s1_df <- df[df$nes_status == "S1", ]
          list(
            list(value = format(nrow(s1_df), big.mark = ","), label = translate("Drowsy Customers")),
            list(value = round(mean(s1_df$r_value, na.rm = TRUE), 0), label = translate("Avg Days Inactive")),
            list(value = round(mean(s1_df$nrec_prob, na.rm = TRUE), 3), label = translate("Avg Churn Prob")),
            list(value = paste0("$", format(round(mean(s1_df$spent_total, na.rm = TRUE), 0), big.mark = ",")), label = translate("Avg Historical Spend"))
          )
        },
        c("Awakening / Return", "Standard Nurturing (Conservative)")
      )

      output$tab_s2 <- render_rich_tab(
        translate("Half-Sleeping Customer Retention"),
        translate("Urgent action needed — these customers are at high risk of permanent churn"),
        function(df) {
          s2_df <- df[df$nes_status == "S2", ]
          list(
            list(value = format(nrow(s2_df), big.mark = ","), label = translate("Half-Sleeping")),
            list(value = round(mean(s2_df$r_value, na.rm = TRUE), 0), label = translate("Avg Days Inactive")),
            list(value = round(mean(s2_df$nrec_prob, na.rm = TRUE), 3), label = translate("Avg Churn Prob")),
            list(value = paste0("$", format(round(mean(s2_df$clv_value, na.rm = TRUE), 0), big.mark = ",")), label = translate("Avg CLV"))
          )
        },
        c("Relationship Repair", "Awakening / Return")
      )

      output$tab_s3 <- render_rich_tab(
        translate("Dormant Customer Activation"),
        translate("Low-cost strategies for long-dormant customers — focus on cost efficiency"),
        function(df) {
          s3_df <- df[df$nes_status == "S3", ]
          list(
            list(value = format(nrow(s3_df), big.mark = ","), label = translate("Dormant Customers")),
            list(value = round(mean(s3_df$r_value, na.rm = TRUE), 0), label = translate("Avg Days Inactive")),
            list(value = paste0("$", format(round(mean(s3_df$spent_total, na.rm = TRUE), 0), big.mark = ",")), label = translate("Avg Historical Spend")),
            list(value = round(mean(s3_df$nrec_prob, na.rm = TRUE), 3), label = translate("Avg Churn Prob"))
          )
        },
        c("Cost Control", "Low-Cost Nurturing")
      )

      # AI Insight — non-blocking via ExtendedTask (GUIDE03, TD_P004 compliant)
      gpt_key <- Sys.getenv("OPENAI_API_KEY", "")
      ai_task <- create_ai_insight_task(gpt_key)

      setup_ai_insight_server(
        input, output, session, ns,
        task = ai_task,
        gpt_key = gpt_key,
        prompt_key = "vitalsigns_analysis.retention_insights",
        get_template_vars = function() {
          df <- dna_data()
          if (is.null(df) || nrow(df) == 0) return(NULL)

          nc <- nes_counts()
          n <- nc$total
          # #314: Exclude N (new customers) from retention/churn denominator
          existing <- nc$E0 + nc$S1 + nc$S2 + nc$S3
          sleeping <- nc$S1 + nc$S2 + nc$S3

          # Repurchase conversion rate (#274, #316): time-windowed
          rp <- overall_repurchase_data()
          repurchase_rate <- round(rp$n_repeat / max(n, 1) * 100, 1)

          # Awakening potential per NES segment (#274)
          awaken_summary <- if ("p_alive" %in% names(df)) {
            sleeping <- df[df$nes_status %in% c("S1", "S2", "S3"), ]
            paste0(
              "S1 (Drowsy) mean P(alive): ", round(mean(df$p_alive[df$nes_status == "S1"], na.rm = TRUE) * 100, 1), "%\n",
              "S2 (Half-Sleeping) mean P(alive): ", round(mean(df$p_alive[df$nes_status == "S2"], na.rm = TRUE) * 100, 1), "%\n",
              "S3 (Dormant) mean P(alive): ", round(mean(df$p_alive[df$nes_status == "S3"], na.rm = TRUE) * 100, 1), "%"
            )
          } else "P(alive) data not available"

          # Build filter context for AI (#324)
          cfg <- comp_config()
          pl <- cfg$filters$product_line_id_sliced
          cty <- cfg$filters$country
          filter_context_str <- paste0(
            "Analysis scope:\n",
            "- Platform: ", cfg$filters$platform_id, "\n",
            "- Product line: ", if (!is.null(pl) && pl != "all") pl else "All", "\n",
            "- Country: ", if (!is.null(cty) && cty != "all") cty else "All"
          )

          list(
            filter_context = filter_context_str,
            total_customers = as.character(n),
            retention_summary = paste0(
              "Retention rate: ", round(nc$E0 / max(existing, 1) * 100, 1), "%\n",
              "Churn rate: ", round(sleeping / max(existing, 1) * 100, 1), "%\n",
              "Core customer ratio: ", round(nc$E0 / max(n, 1) * 100, 1), "%\n",
              "Repurchase conversion rate: ", repurchase_rate, "%",
              if (rp$method %in% c("time_windowed", "per_category_windowed")) paste0(
                " (time-windowed: W=1.5×median_ipt",
                if (!is.na(rp$window)) paste0("=", round(rp$window, 1), " days)") else ", per-category)")
              else if (rp$method == "simple") " (simple: ni>1, insufficient ipt data for time window)"
              else ""  # no_data / error_fallback: no annotation needed
            ),
            nes_distribution = paste0(
              "N (New): ", nc$N, " (", round(nc$N / max(n, 1) * 100, 1), "%)\n",
              "E0 (Core): ", nc$E0, " (", round(nc$E0 / max(n, 1) * 100, 1), "%)\n",
              "S1 (Drowsy): ", nc$S1, " (", round(nc$S1 / max(n, 1) * 100, 1), "%)\n",
              "S2 (Half-Sleeping): ", nc$S2, " (", round(nc$S2 / max(n, 1) * 100, 1), "%)\n",
              "S3 (Dormant): ", nc$S3, " (", round(nc$S3 / max(n, 1) * 100, 1), "%)"
            ),
            churn_risk_summary = paste0(
              "Risk metric: IPT-based (r_value vs ipt multiples)\n",
              "High risk (r_value > 2.5x IPT): ", sum(df$r_value > df$ipt * 2.5 & !is.na(df$ipt) & df$ipt > 0, na.rm = TRUE), "\n",
              "Medium risk (1.5x-2.5x IPT): ", sum(df$r_value > df$ipt * 1.5 & df$r_value <= df$ipt * 2.5 & !is.na(df$ipt) & df$ipt > 0, na.rm = TRUE), "\n",
              "Low risk (<= 1.5x IPT): ", sum(df$r_value <= df$ipt * 1.5 & !is.na(df$ipt) & df$ipt > 0, na.rm = TRUE), "\n",
              "Avg IPT: ", round(mean(df$ipt[df$ipt > 0], na.rm = TRUE), 1), " days\n",
              "Avg days since last purchase: ", round(mean(df$r_value, na.rm = TRUE), 1), " days"
            ),
            value_at_risk = paste0(
              "Avg CLV of sleeping customers: $", format(round(mean(df$clv_value[df$nes_status %in% c("S1", "S2", "S3")], na.rm = TRUE), 0), big.mark = ","), "\n",
              "Total CLV at risk: $", format(round(sum(df$clv_value[df$nes_status %in% c("S1", "S2", "S3")], na.rm = TRUE), 0), big.mark = ",")
            ),
            awakening_potential = awaken_summary
          )
        },
        component_label = "customerRetention"
      )

    })
  }

  list(ui = list(filter = ui_filter, display = ui_display), server = server_fn)
}
