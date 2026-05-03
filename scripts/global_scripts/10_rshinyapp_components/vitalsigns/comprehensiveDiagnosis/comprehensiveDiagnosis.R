# =============================================================================
# comprehensiveDiagnosis.R — VitalSigns Comprehensive Operations Diagnosis
# AI-only component: aggregates revenue, structure, retention, engagement,
# growth trends, and geographic data into a comprehensive diagnosis
# Issues: #280 (original), #326 (enhanced 19+ indicators)
# Following: UI_R001, UI_R026, DEV_R052, MP029
# =============================================================================

vsComprehensiveDiagnosisComponent <- function(id, app_connection, comp_config, translate) {
  ns <- NS(id)

  # ---- UI ----
  ui_filter <- tagList(
    ai_insight_button_ui(ns, translate)
  )

  ui_display <- tagList(
    fluidRow(
      column(12,
        tags$div(class = "callout callout-info",
          tags$h5(icon("stethoscope"), translate("Comprehensive Operations Diagnosis")),
          tags$p(translate("Click the AI analysis button on the left to generate a comprehensive operations diagnosis report covering revenue, customer structure, retention, and engagement indicators."))
        )
      )
    ),
    fluidRow(
      column(12, ai_insight_result_ui(ns, translate))
    )
  )

  # ---- Server ----
  server_fn <- function(input, output, session) {
    moduleServer(id, function(input, output, session) {

      # #376: Migrated from raw DBI::dbGetQuery() to tbl2() per DM_R023 v1.2
      dna_data <- reactive({
        if (is.null(app_connection) || !DBI::dbIsValid(app_connection)) return(NULL)
        tryCatch({
          df <- tbl2(app_connection, "df_dna_by_customer") %>%
            dplyr::collect()
          if (nrow(df) == 0) return(NULL)
          df
        }, error = function(e) {
          message("[vsComprehensiveDiagnosis] Failed to load dna_data: ", e$message)
          NULL
        })
      })

      macro_data <- reactive({
        if (is.null(app_connection) || !DBI::dbIsValid(app_connection)) return(NULL)
        tryCatch({
          if (!DBI::dbExistsTable(app_connection, "df_macro_monthly_summary")) return(NULL)
          tbl2(app_connection, "df_macro_monthly_summary") %>%
            dplyr::filter(product_line_id_filter == "all") %>%
            dplyr::arrange(dplyr::desc(year_month)) %>%
            head(13) %>%
            dplyr::collect()
        }, error = function(e) {
          message("[vsComprehensiveDiagnosis] Failed to load macro_data: ", e$message)
          NULL
        })
      })

      geo_data <- reactive({
        if (is.null(app_connection) || !DBI::dbIsValid(app_connection)) return(NULL)
        tryCatch({
          if (!DBI::dbExistsTable(app_connection, "df_geo_sales_by_country")) return(NULL)
          tbl2(app_connection, "df_geo_sales_by_country") %>%
            dplyr::filter(product_line_id_filter == "all") %>%
            dplyr::collect()
        }, error = function(e) {
          message("[vsComprehensiveDiagnosis] Failed to load geo_data: ", e$message)
          NULL
        })
      })

      # AI Insight wiring
      gpt_key <- Sys.getenv("OPENAI_API_KEY", "")
      ai_task <- create_ai_insight_task(gpt_key)

      setup_ai_insight_server(
        input, output, session, ns,
        task = ai_task,
        gpt_key = gpt_key,
        prompt_key = "vitalsigns_analysis.comprehensive_diagnosis",
        get_template_vars = function() {
          df <- dna_data()
          if (is.null(df) || nrow(df) == 0) return(NULL)

          n <- nrow(df)

          # ---- 1. Revenue summary (enhanced: NES-specific AOV, PCV) ----
          rev_parts <- c(
            paste0("Total customers: ", n),
            paste0("Total revenue: $", format(round(sum(df$m_value, na.rm = TRUE), 0), big.mark = ",")),
            paste0("Avg monetary (AOV proxy): $", format(round(mean(df$m_value, na.rm = TRUE), 0), big.mark = ",")),
            paste0("Median monetary: $", format(round(stats::median(df$m_value, na.rm = TRUE), 0), big.mark = ","))
          )
          # New customer AOV (NES = N)
          if ("nes_status" %in% names(df)) {
            new_cust <- df[df$nes_status == "N", ]
            if (nrow(new_cust) > 0) {
              rev_parts <- c(rev_parts, paste0("New customer (N) AOV: $", format(round(mean(new_cust$m_value, na.rm = TRUE), 0), big.mark = ",")))
            }
            core_cust <- df[df$nes_status == "E0", ]
            if (nrow(core_cust) > 0) {
              rev_parts <- c(rev_parts, paste0("Core customer (E0) AOV: $", format(round(mean(core_cust$m_value, na.rm = TRUE), 0), big.mark = ",")))
            }
          }
          if ("clv" %in% names(df)) rev_parts <- c(rev_parts, paste0("Avg CLV: $", format(round(mean(df$clv, na.rm = TRUE), 0), big.mark = ",")))
          if ("clv_level" %in% names(df)) rev_parts <- c(rev_parts, paste0("CLV levels: ", paste(names(table(df$clv_level)), table(df$clv_level), sep = "=", collapse = ", ")))
          if ("pcv" %in% names(df)) {
            pcv_vals <- df$pcv[!is.na(df$pcv)]
            if (length(pcv_vals) > 0) rev_parts <- c(rev_parts, paste0("Avg PCV (transaction consistency): $", format(round(mean(pcv_vals), 0), big.mark = ",")))
          }
          revenue_summary <- paste(rev_parts, collapse = "\n")

          # ---- 2. Customer structure (enhanced: churn rate) ----
          struct_parts <- c()
          if ("nes_label" %in% names(df)) {
            struct_parts <- c(struct_parts, paste0("NES distribution: ", paste(names(table(df$nes_label)), table(df$nes_label), sep = "=", collapse = ", ")))
          }
          struct_parts <- c(struct_parts,
            paste0("Repeat buyers (F>1): ", sum(df$f_value > 1, na.rm = TRUE), " (", round(100 * sum(df$f_value > 1, na.rm = TRUE) / n, 1), "%)"),
            paste0("Single buyers (F=1): ", sum(df$f_value == 1, na.rm = TRUE), " (", round(100 * sum(df$f_value == 1, na.rm = TRUE) / n, 1), "%)")
          )
          # Churn rate = (S1+S2+S3) / (E0+S1+S2+S3) excluding N (#314 formula)
          if ("nes_status" %in% names(df)) {
            nes_tbl <- table(df$nes_status)
            e0 <- as.integer(nes_tbl["E0"] %||% 0L)
            s1 <- as.integer(nes_tbl["S1"] %||% 0L)
            s2 <- as.integer(nes_tbl["S2"] %||% 0L)
            s3 <- as.integer(nes_tbl["S3"] %||% 0L)
            denom <- e0 + s1 + s2 + s3
            if (denom > 0) {
              churn_rate <- (s1 + s2 + s3) / denom
              struct_parts <- c(struct_parts, paste0("Churn rate (S/(E0+S)): ", round(100 * churn_rate, 1), "%"))
            }
          }
          if ("value_tier" %in% names(df)) struct_parts <- c(struct_parts, paste0("Value tiers: ", paste(names(table(df$value_tier)), table(df$value_tier), sep = "=", collapse = ", ")))
          customer_structure <- paste(struct_parts, collapse = "\n")

          # ---- 3. Retention summary (enhanced: dormant prediction, awakening) ----
          retention_parts <- c()
          if ("p_alive" %in% names(df)) {
            pa <- df$p_alive[!is.na(df$p_alive)]
            retention_parts <- c(retention_parts, paste0(
              "Avg P(alive): ", round(mean(pa), 3), "\n",
              "Active (P>0.7): ", sum(pa > 0.7), " (", round(100 * sum(pa > 0.7) / max(length(pa), 1), 1), "%)\n",
              "At risk (P<0.3): ", sum(pa < 0.3), " (", round(100 * sum(pa < 0.3) / max(length(pa), 1), 1), "%)"
            ))
          }
          if ("nrec_prob" %in% names(df)) {
            nr <- df$nrec_prob[!is.na(df$nrec_prob)]
            retention_parts <- c(retention_parts, paste0("Avg churn probability: ", round(mean(nr), 3)))
          }
          if ("rsv_type" %in% names(df)) {
            retention_parts <- c(retention_parts, paste0("RSV types: ", paste(names(table(df$rsv_type)), table(df$rsv_type), sep = "=", collapse = ", ")))
          }
          # Predicted dormant: r_value > ipt * 2.5 (#312 formula)
          if (all(c("r_value", "ipt") %in% names(df))) {
            valid_ipt <- df[!is.na(df$ipt) & df$ipt > 0 & !is.na(df$r_value), ]
            if (nrow(valid_ipt) > 0) {
              dormant_count <- sum(valid_ipt$r_value > valid_ipt$ipt * 2.5)
              retention_parts <- c(retention_parts, paste0(
                "Predicted dormant (R > 2.5x IPT): ", dormant_count,
                " (", round(100 * dormant_count / nrow(valid_ipt), 1), "% of repeat buyers)"
              ))
            }
          }
          # Awakening potential: S1/S2/S3 with P(alive) > 0.3 (still reachable)
          if (all(c("nes_status", "p_alive") %in% names(df))) {
            sleeping <- df[df$nes_status %in% c("S1", "S2", "S3") & !is.na(df$p_alive), ]
            if (nrow(sleeping) > 0) {
              awakening_potential <- sum(sleeping$p_alive > 0.3)
              retention_parts <- c(retention_parts, paste0(
                "Awakening potential (sleeping with P(alive)>0.3): ", awakening_potential,
                " (", round(100 * awakening_potential / nrow(sleeping), 1), "% of sleeping customers)"
              ))
            }
          }
          retention_summary <- if (length(retention_parts) > 0) paste(retention_parts, collapse = "\n") else "Retention data not available"

          # ---- 4. Engagement summary (enhanced: repurchase rate) ----
          engagement_parts <- c()
          if ("cai" %in% names(df)) {
            cai_vals <- df$cai[!is.na(df$cai)]
            engagement_parts <- c(engagement_parts, paste0(
              "Avg CAI: ", round(mean(cai_vals), 3), "\n",
              "Declining (CAI<-0.2): ", sum(cai_vals < -0.2), " (", round(100 * sum(cai_vals < -0.2) / max(length(cai_vals), 1), 1), "%)"
            ))
          }
          if ("ipt_mean" %in% names(df)) {
            ipt_vals <- df$ipt_mean[!is.na(df$ipt_mean)]
            engagement_parts <- c(engagement_parts, paste0(
              "Avg purchase interval: ", round(mean(ipt_vals), 1), " days\n",
              "Median purchase interval: ", round(stats::median(ipt_vals), 1), " days"
            ))
          }
          engagement_parts <- c(engagement_parts, paste0(
            "Avg frequency: ", round(mean(df$f_value, na.rm = TRUE), 2), " times"
          ))
          # Time-windowed repurchase rate (#316 formula: ni > 1 AND ipt <= 1.5 * median_ipt)
          if (all(c("ni", "ipt") %in% names(df))) {
            repeat_df <- df[!is.na(df$ni) & df$ni > 1 & !is.na(df$ipt) & df$ipt > 0, ]
            if (nrow(repeat_df) > 0) {
              w <- 1.5 * stats::median(repeat_df$ipt)
              converted <- sum(repeat_df$ipt <= w)
              engagement_parts <- c(engagement_parts, paste0(
                "Time-windowed repurchase rate: ", converted, "/", nrow(repeat_df),
                " (", round(100 * converted / nrow(repeat_df), 1), "%, W=", round(w, 0), " days)"
              ))
            }
          }
          engagement_summary <- paste(engagement_parts, collapse = "\n")

          # ---- 5. Growth summary (from df_macro_monthly_summary) ----
          macro <- macro_data()
          growth_summary <- "Growth data not available"
          if (!is.null(macro) && nrow(macro) >= 2) {
            latest <- macro[1, ]
            prev <- macro[2, ]
            growth_parts <- c(
              paste0("Latest month (", latest$year_month, "):"),
              paste0("  Revenue: $", format(round(latest$total_revenue, 0), big.mark = ",")),
              paste0("  Orders: ", format(latest$order_count, big.mark = ",")),
              paste0("  Active customers: ", format(latest$active_customers, big.mark = ",")),
              paste0("  New customers: ", format(latest$new_customers, big.mark = ","))
            )
            if (!is.na(latest$mom_revenue_pct)) {
              growth_parts <- c(growth_parts, paste0("  Revenue MoM: ", round(latest$mom_revenue_pct, 1), "%"))
            }
            # Customer growth rate
            if (prev$active_customers > 0) {
              cust_growth <- (latest$active_customers - prev$active_customers) / prev$active_customers * 100
              growth_parts <- c(growth_parts, paste0("  Customer growth MoM: ", round(cust_growth, 1), "%"))
            }
            # Cumulative customers (sum of new customers over available months)
            cum_new <- sum(macro$new_customers, na.rm = TRUE)
            growth_parts <- c(growth_parts, paste0("  Cumulative new customers (last ", nrow(macro), " months): ", format(cum_new, big.mark = ",")))
            growth_summary <- paste(growth_parts, collapse = "\n")
          }

          # ---- 6. Geographic summary (from df_geo_sales_by_country) ----
          geo <- geo_data()
          geographic_summary <- "Geographic data not available"
          if (!is.null(geo) && nrow(geo) > 0) {
            geo_sorted <- geo[order(-geo$total_revenue), ]
            total_geo_rev <- sum(geo_sorted$total_revenue, na.rm = TRUE)
            top_n <- min(5, nrow(geo_sorted))
            top_countries <- geo_sorted[seq_len(top_n), ]
            geo_parts <- c(
              paste0("Active markets: ", nrow(geo_sorted), " countries"),
              paste0("Top ", top_n, " markets by revenue:")
            )
            for (i in seq_len(top_n)) {
              share <- round(100 * top_countries$total_revenue[i] / total_geo_rev, 1)
              geo_parts <- c(geo_parts, paste0(
                "  ", i, ". ", top_countries$ship_country[i],
                ": $", format(round(top_countries$total_revenue[i], 0), big.mark = ","),
                " (", share, "%, ", top_countries$customer_count[i], " customers)"
              ))
            }
            # Market concentration: top 3 share
            top3_rev <- sum(geo_sorted$total_revenue[seq_len(min(3, nrow(geo_sorted)))], na.rm = TRUE)
            geo_parts <- c(geo_parts, paste0("Top 3 market concentration: ", round(100 * top3_rev / total_geo_rev, 1), "%"))
            geographic_summary <- paste(geo_parts, collapse = "\n")
          }

          list(
            revenue_summary = revenue_summary,
            customer_structure = customer_structure,
            retention_summary = retention_summary,
            engagement_summary = engagement_summary,
            growth_summary = growth_summary,
            geographic_summary = geographic_summary
          )
        },
        component_label = "vsComprehensiveDiagnosis"
      )
    })
  }

  list(ui = list(filter = ui_filter, display = ui_display), server = server_fn)
}
