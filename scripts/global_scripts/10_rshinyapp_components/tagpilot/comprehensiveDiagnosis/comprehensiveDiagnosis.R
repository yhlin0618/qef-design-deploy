# =============================================================================
# comprehensiveDiagnosis.R — TagPilot Comprehensive Customer Diagnosis (Issue #271)
# AI-only component: aggregates 11 indicators into a single diagnosis report
# Following: UI_R001, UI_R026, DEV_R052, MP029
# =============================================================================

comprehensiveDiagnosisComponent <- function(id, app_connection, comp_config, translate) {
  ns <- NS(id)

  # ---- UI ----
  ui_filter <- tagList(
    ai_insight_button_ui(ns, translate)
  )

  ui_display <- tagList(
    fluidRow(
      column(12,
        tags$div(class = "callout callout-info",
          tags$h5(icon("stethoscope"), translate("Comprehensive Customer Diagnosis")),
          tags$p(translate("Click the AI analysis button on the left to generate a comprehensive diagnosis report covering all 11 indicators: RFM, CAI, NES, IPT, PCV, AOV by NES, transaction consistency, CLV, P(alive), expected transactions, and CRI/RSV."))
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
          message("[comprehensiveDiagnosis] Failed to load dna_data: ", e$message)
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
        prompt_key = "customer_analysis.comprehensive_diagnosis",
        get_template_vars = function() {
          df <- dna_data()
          if (is.null(df) || nrow(df) == 0) return(NULL)

          n <- nrow(df)

          # 1. RFM overview (Recency + Frequency + Monetary) — Issue #315
          safe_mean <- function(x, digits = 1) {
            vals <- x[!is.na(x)]
            if (length(vals) == 0) return("N/A")
            round(mean(vals), digits)
          }
          rfm_summary <- paste0(
            "Total customers: ", n, "\n",
            "Avg Recency: ", safe_mean(df$r_value, 1), " days\n",
            "Avg Frequency: ", safe_mean(df$f_value, 2), " times\n",
            "Avg Monetary: $", if (is.numeric(safe_mean(df$m_value, 0))) format(safe_mean(df$m_value, 0), big.mark = ",") else "N/A", "\n",
            "R segments: ", paste(names(table(df$r_label)), table(df$r_label), sep = "=", collapse = ", "), "\n",
            "F segments: ", paste(names(table(df$f_label)), table(df$f_label), sep = "=", collapse = ", "), "\n",
            "M segments: ", paste(names(table(df$m_label)), table(df$m_label), sep = "=", collapse = ", ")
          )

          # 2. CAI (Customer Activity Index)
          cai_summary <- if ("cai" %in% names(df)) {
            cai_vals <- df$cai[!is.na(df$cai)]
            if (length(cai_vals) > 0) {
              paste0(
                "Customers with CAI: ", length(cai_vals), "\n",
                "Avg CAI: ", round(mean(cai_vals), 3), "\n",
                "CAI < -0.2 (declining): ", sum(cai_vals < -0.2), " (", round(100 * sum(cai_vals < -0.2) / length(cai_vals), 1), "%)\n",
                "CAI > 0.2 (growing): ", sum(cai_vals > 0.2), " (", round(100 * sum(cai_vals > 0.2) / length(cai_vals), 1), "%)"
              )
            } else "No valid CAI data"
          } else "CAI data not available"

          # 3. NES (New/Existing/Sleeping status) — all 5 states with count + ratio
          nes_summary <- if ("nes_label" %in% names(df)) {
            nes_tbl <- table(df$nes_label)
            nes_states <- c("N", "E0", "S1", "S2", "S3")
            nes_lines <- vapply(nes_states, function(s) {
              cnt <- if (s %in% names(nes_tbl)) as.integer(nes_tbl[[s]]) else 0L
              pct <- round(100 * cnt / max(n, 1), 1)
              paste0(s, ": ", cnt, " (", pct, "%)")
            }, character(1))
            paste0("NES distribution:\n", paste(nes_lines, collapse = "\n"))
          } else "NES data not available"

          # 4. IPT (Inter-Purchase Time)
          ipt_summary <- if ("ipt_mean" %in% names(df)) {
            ipt_vals <- df$ipt_mean[!is.na(df$ipt_mean)]
            if (length(ipt_vals) > 0) {
              paste0(
                "Customers with IPT: ", length(ipt_vals), "\n",
                "Avg IPT: ", round(mean(ipt_vals), 1), " days\n",
                "Median IPT: ", round(stats::median(ipt_vals), 1), " days"
              )
            } else "No valid IPT data"
          } else "IPT data not available"

          # 5. PCV (Past Customer Value)
          pcv_summary <- if ("pcv" %in% names(df)) {
            pcv_vals <- df$pcv[!is.na(df$pcv)]
            if (length(pcv_vals) > 0) {
              paste0(
                "Customers with PCV: ", length(pcv_vals), "\n",
                "Avg PCV: $", format(round(mean(pcv_vals), 0), big.mark = ","), "\n",
                "Median PCV: $", format(round(stats::median(pcv_vals), 0), big.mark = ",")
              )
            } else "No valid PCV data"
          } else "PCV data not available"

          # 6. AOV by NES segment (New vs Core customer spending)
          aov_by_segment <- if (all(c("m_value", "nes_label") %in% names(df))) {
            valid_idx <- !is.na(df$m_value) & !is.na(df$nes_label)
            if (sum(valid_idx) > 0) {
              seg_aov <- tapply(df$m_value[valid_idx], df$nes_label[valid_idx], function(x) round(mean(x, na.rm = TRUE), 0))
              paste0(
                "AOV by NES segment:\n",
                paste(names(seg_aov), paste0("$", format(seg_aov, big.mark = ",")), sep = ": ", collapse = "\n")
              )
            } else "No valid AOV by segment data"
          } else "AOV by segment data not available"

          # 7. Transaction consistency (sigma_hnorm)
          consistency_summary <- if ("sigma_hnorm_mle" %in% names(df)) {
            sig_vals <- df$sigma_hnorm_mle[!is.na(df$sigma_hnorm_mle)]
            if (length(sig_vals) > 0) {
              paste0(
                "Customers with consistency data: ", length(sig_vals), "\n",
                "Avg sigma_hnorm (lower = more consistent): ", round(mean(sig_vals), 3), "\n",
                "Highly consistent (sigma < 0.3): ", sum(sig_vals < 0.3), " (", round(100 * sum(sig_vals < 0.3) / length(sig_vals), 1), "%)\n",
                "Irregular (sigma > 0.7): ", sum(sig_vals > 0.7), " (", round(100 * sum(sig_vals > 0.7) / length(sig_vals), 1), "%)"
              )
            } else "No valid consistency data"
          } else "Transaction consistency data not available"

          # 8. CLV (Customer Lifetime Value)
          clv_summary <- if ("clv" %in% names(df)) {
            clv_vals <- df$clv[!is.na(df$clv)]
            if (length(clv_vals) > 0) {
              paste0(
                "Customers with CLV: ", length(clv_vals), "\n",
                "Avg CLV: $", format(round(mean(clv_vals), 0), big.mark = ","), "\n",
                "Median CLV: $", format(round(stats::median(clv_vals), 0), big.mark = ","),
                if ("clv_level" %in% names(df)) paste0("\nCLV levels: ", paste(names(table(df$clv_level)), table(df$clv_level), sep = "=", collapse = ", ")) else ""
              )
            } else "No valid CLV data"
          } else "CLV data not available"

          # 9. P(alive) — Dormancy prediction (BG/NBD)
          palive_summary <- if ("p_alive" %in% names(df)) {
            pa <- df$p_alive[!is.na(df$p_alive)]
            if (length(pa) > 0) {
              paste0(
                "Customers with P(alive): ", length(pa), "\n",
                "Avg P(alive): ", round(mean(pa), 3), "\n",
                "Active (P > 0.7): ", sum(pa > 0.7), " (", round(100 * sum(pa > 0.7) / length(pa), 1), "%)\n",
                "At risk (0.3-0.7): ", sum(pa >= 0.3 & pa <= 0.7), " (", round(100 * sum(pa >= 0.3 & pa <= 0.7) / length(pa), 1), "%)\n",
                "Dormant (P < 0.3): ", sum(pa < 0.3), " (", round(100 * sum(pa < 0.3) / length(pa), 1), "%)"
              )
            } else "No valid P(alive) data"
          } else "P(alive) data not available"

          # 10. Expected transactions (BG/NBD)
          expected_tx_summary <- if ("btyd_expected_transactions" %in% names(df)) {
            etx <- df$btyd_expected_transactions[!is.na(df$btyd_expected_transactions)]
            if (length(etx) > 0) {
              paste0(
                "Customers with expected tx: ", length(etx), "\n",
                "Avg expected transactions: ", round(mean(etx), 2), "\n",
                "Median expected transactions: ", round(stats::median(etx), 2), "\n",
                "Expected 0 tx (< 0.5): ", sum(etx < 0.5), " (", round(100 * sum(etx < 0.5) / length(etx), 1), "%)"
              )
            } else "No valid expected transactions data"
          } else "Expected transactions data not available"

          # 11. CRI (Customer Risk Index) + RSV
          cri_summary <- if ("cri_ecdf" %in% names(df)) {
            cri <- df$cri_ecdf[!is.na(df$cri_ecdf)]
            if (length(cri) > 0) {
              parts <- paste0(
                "Avg CRI ECDF: ", round(mean(cri), 3), "\n",
                "High risk (CRI ECDF > 0.7): ", sum(cri > 0.7), " (", round(100 * sum(cri > 0.7) / length(cri), 1), "%)"
              )
              if ("rsv_type" %in% names(df)) {
                parts <- paste0(parts, "\nRSV types: ", paste(names(table(df$rsv_type)), table(df$rsv_type), sep = "=", collapse = ", "))
              }
              parts
            } else "No valid CRI data"
          } else "CRI data not available"

          list(
            rfm_summary = rfm_summary,
            cai_summary = cai_summary,
            nes_summary = nes_summary,
            ipt_summary = ipt_summary,
            pcv_summary = pcv_summary,
            aov_by_segment = aov_by_segment,
            consistency_summary = consistency_summary,
            clv_summary = clv_summary,
            palive_summary = palive_summary,
            expected_tx_summary = expected_tx_summary,
            cri_summary = cri_summary
          )
        },
        component_label = "comprehensiveDiagnosis"
      )
    })
  }

  list(ui = list(filter = ui_filter, display = ui_display), server = server_fn)
}
