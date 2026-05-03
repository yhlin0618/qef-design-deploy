# =============================================================================
# worldMap.R — World Market Map Component (VitalSigns)
# CONSUMES: df_geo_sales_by_country, df_geo_sales_by_state (from D03_01)
# Following: UI_R001, UI_R011, UI_R028, MP064, MP029, DEV_R050, 10-ui-layout
# =============================================================================

worldMapComponent <- function(id, app_connection, comp_config, translate) {
  ns <- NS(id)

  # ---- KPI choices (DEV_R050: display text from translate, not hardcoded) ----
  kpi_choices <- c(
    "revenue"   = "revenue",
    "orders"    = "orders",
    "customers" = "customers",
    "aov"       = "aov"
  )

  # ---- UI ----
  # #348: Filter only has KPI select + AI button.
  # Country select and view_mode radio removed — drill-down via map click.
  ui_filter <- tagList(
    selectInput(
      ns("kpi_select"),
      label = translate("Select KPI"),
      choices = stats::setNames(
        kpi_choices,
        c(translate("Revenue"), translate("Order Count"),
          translate("Customer Count"), translate("Avg Order Value"))
      ),
      selected = "revenue"
    ),
    ai_insight_button_ui(ns, translate)
  )

  ui_display <- tagList(
    # KPI Row
    fluidRow(
      column(3, uiOutput(ns("kpi_countries"))),
      column(3, uiOutput(ns("kpi_largest"))),
      column(3, uiOutput(ns("kpi_top3_share"))),
      column(3, uiOutput(ns("kpi_total_revenue")))
    ),
    # Map Row — #348: back button shown when drilled into states
    fluidRow(
      column(12, bs4Card(
        title = uiOutput(ns("map_title")),
        status = "primary",
        width = 12, solidHeader = TRUE,
        plotly::plotlyOutput(ns("geo_map"), height = "480px")
      ))
    ),
    # Detail Table
    fluidRow(
      column(12, bs4Card(
        title = translate("Country Details"), status = "primary",
        width = 12, solidHeader = TRUE,
        downloadButton(ns("download_csv"), translate("Download CSV"),
                       class = "btn-sm btn-outline-primary mb-2"),
        DT::dataTableOutput(ns("detail_table"))
      ))
    ),
    # AI Insight Result — bottom of display (10-ui-layout)
    fluidRow(
      column(12, ai_insight_result_ui(ns, translate))
    )
  )

  # ---- Server ----
  server_fn <- function(input, output, session) {
    moduleServer(id, function(input, output, session) {

      # #348: View mode controlled by reactiveVal, not radioButtons
      view_mode <- reactiveVal("world")

      # ---- Map title: changes based on current view (#351) ----
      output$map_title <- renderUI({
        if (view_mode() == "us_states") {
          # #351: click map background to return (UX_R001: single-click)
          tagList(
            icon("flag-usa"),
            " ",
            translate("US States Distribution"),
            tags$small(
              style = "color: #ffffffcc; margin-left: 12px; font-weight: normal;",
              translate("Click outside states to return")
            )
          )
        } else {
          translate("World Market Distribution")
        }
      })

      # ---- Back to world: click background of state map (#351, UX_R001) ----
      observeEvent(input$state_bg_click, {
        view_mode("world")
        message("[worldMap] Back to world view (background click)")
      })

      # ---- Plotly click: drill down to US states (#348) ----

      # plotly source IDs must be namespaced in modules (uses root session input)
      world_source <- session$ns("worldmap")

      # World map: click US -> drill to states
      observe({
        click <- plotly::event_data("plotly_click", source = world_source, priority = "event")
        if (is.null(click)) return()
        loc <- click$customdata
        message("[worldMap] World map clicked: customdata=", loc)
        if (!is.null(loc) && loc %in% c("USA", "US")) {
          view_mode("us_states")
          message("[worldMap] Drilled into US states")
        }
      })

      # ---- Data: all countries (no country filter — #348) ----
      # #376: Migrated from raw DBI::dbGetQuery() to tbl2() per DM_R023 v1.2
      geo_data <- reactive({
        cfg <- comp_config()
        req(cfg$filters$platform_id)
        tryCatch({
          plt <- cfg$filters$platform_id
          pl_id <- cfg$filters$product_line_id_sliced
          if (is.null(pl_id) || pl_id == "all") pl_id <- "all"

          df <- tbl2(app_connection, "df_geo_sales_by_country") %>%
            dplyr::filter(platform_id == !!plt,
                          product_line_id_filter == !!pl_id) %>%
            dplyr::select(ship_country, total_revenue, order_count,
                          customer_count, avg_order_value, avg_quantity) %>%
            dplyr::collect()

          if (nrow(df) == 0) {
            message("[worldMap] No data returned")
            return(NULL)
          }
          message("[worldMap] Loaded ", nrow(df), " countries")
          df
        }, error = function(e) {
          message("[worldMap] Data load error: ", e$message)
          NULL
        })
      })

      # Helper: get column by selected KPI
      kpi_col <- reactive({
        req(input$kpi_select)
        switch(input$kpi_select,
               "revenue"   = "total_revenue",
               "orders"    = "order_count",
               "customers" = "customer_count",
               "aov"       = "avg_order_value",
               "total_revenue")
      })

      kpi_label <- reactive({
        req(input$kpi_select)
        switch(input$kpi_select,
               "revenue"   = translate("Revenue"),
               "orders"    = translate("Order Count"),
               "customers" = translate("Customer Count"),
               "aov"       = translate("Avg Order Value"),
               translate("Revenue"))
      })

      # ---- KPIs ----
      output$kpi_countries <- renderUI({
        df <- geo_data()
        val <- if (is.null(df)) "-" else as.character(nrow(df))
        bs4ValueBox(value = val, subtitle = translate("Total Countries"),
                    icon = icon("globe"), color = "primary", width = 12)
      })

      output$kpi_largest <- renderUI({
        df <- geo_data()
        if (is.null(df)) return(bs4ValueBox(value = "-", subtitle = translate("Largest Market"),
                                            icon = icon("trophy"), color = "success", width = 12))
        top <- df[which.max(df$total_revenue), ]
        bs4ValueBox(value = country_name_zh(top$ship_country),
                    subtitle = paste0(translate("Largest Market"), " ($",
                                      format(round(top$total_revenue, 0), big.mark = ","), ")"),
                    icon = icon("trophy"), color = "success", width = 12)
      })

      output$kpi_top3_share <- renderUI({
        df <- geo_data()
        if (is.null(df) || nrow(df) < 1) return(bs4ValueBox(
          value = "-", subtitle = translate("Top 3 Market Share"),
          icon = icon("chart-pie"), color = "info", width = 12))
        ordered <- df[order(-df$total_revenue), ]
        top3_rev <- sum(utils::head(ordered$total_revenue, 3))
        total_rev <- sum(ordered$total_revenue)
        pct <- round(top3_rev / total_rev * 100, 1)
        bs4ValueBox(value = paste0(pct, "%"), subtitle = translate("Top 3 Market Share"),
                    icon = icon("chart-pie"), color = "info", width = 12)
      })

      output$kpi_total_revenue <- renderUI({
        df <- geo_data()
        if (is.null(df)) return(bs4ValueBox(value = "-", subtitle = translate("Revenue"),
                                            icon = icon("dollar-sign"), color = "warning", width = 12))
        total <- sum(df$total_revenue, na.rm = TRUE)
        bs4ValueBox(value = paste0("$", format(round(total, 0), big.mark = ",")),
                    subtitle = translate("Revenue"),
                    icon = icon("dollar-sign"), color = "warning", width = 12)
      })

      # ---- State-level data ----
      # #376: Migrated from raw DBI::dbGetQuery() to tbl2() per DM_R023 v1.2
      state_data <- reactive({
        cfg <- comp_config()
        req(cfg$filters$platform_id)
        tryCatch({
          plt <- cfg$filters$platform_id
          pl_id <- cfg$filters$product_line_id_sliced
          if (is.null(pl_id) || pl_id == "all") pl_id <- "all"

          df <- tbl2(app_connection, "df_geo_sales_by_state") %>%
            dplyr::filter(platform_id == !!plt,
                          product_line_id_filter == !!pl_id) %>%
            dplyr::select(ship_state, total_revenue, order_count,
                          customer_count, avg_order_value, avg_quantity) %>%
            dplyr::collect()

          if (nrow(df) == 0) return(NULL)
          message("[worldMap] Loaded ", nrow(df), " US states")
          df
        }, error = function(e) {
          message("[worldMap] State data load error: ", e$message)
          NULL
        })
      })

      # ---- ISO2 to ISO3 mapping (plotly requires ISO-3 alpha-3 codes) ----
      iso2to3 <- c(
        AD="AND",AE="ARE",AF="AFG",AG="ATG",AI="AIA",AL="ALB",AM="ARM",AO="AGO",
        AR="ARG",AS="ASM",AT="AUT",AU="AUS",AW="ABW",AZ="AZE",BA="BIH",BB="BRB",
        BD="BGD",BE="BEL",BF="BFA",BG="BGR",BH="BHR",BI="BDI",BJ="BEN",BM="BMU",
        BN="BRN",BO="BOL",BR="BRA",BS="BHS",BT="BTN",BW="BWA",BY="BLR",BZ="BLZ",
        CA="CAN",CD="COD",CF="CAF",CG="COG",CH="CHE",CI="CIV",CL="CHL",CM="CMR",
        CN="CHN",CO="COL",CR="CRI",CU="CUB",CV="CPV",CY="CYP",CZ="CZE",DE="DEU",
        DJ="DJI",DK="DNK",DM="DMA",DO="DOM",DZ="DZA",EC="ECU",EE="EST",EG="EGY",
        ER="ERI",ES="ESP",ET="ETH",FI="FIN",FJ="FJI",FK="FLK",FM="FSM",FO="FRO",
        FR="FRA",GA="GAB",GB="GBR",GD="GRD",GE="GEO",GF="GUF",GH="GHA",GI="GIB",
        GL="GRL",GM="GMB",GN="GIN",GP="GLP",GQ="GNQ",GR="GRC",GT="GTM",GU="GUM",
        GW="GNB",GY="GUY",HK="HKG",HN="HND",HR="HRV",HT="HTI",HU="HUN",ID="IDN",
        IE="IRL",IL="ISR",IN="IND",IQ="IRQ",IR="IRN",IS="ISL",IT="ITA",JM="JAM",
        JO="JOR",JP="JPN",KE="KEN",KG="KGZ",KH="KHM",KI="KIR",KM="COM",KN="KNA",
        KP="PRK",KR="KOR",KW="KWT",KY="CYM",KZ="KAZ",LA="LAO",LB="LBN",LC="LCA",
        LI="LIE",LK="LKA",LR="LBR",LS="LSO",LT="LTU",LU="LUX",LV="LVA",LY="LBY",
        MA="MAR",MC="MCO",MD="MDA",ME="MNE",MG="MDG",MH="MHL",MK="MKD",ML="MLI",
        MM="MMR",MN="MNG",MO="MAC",MP="MNP",MQ="MTQ",MR="MRT",MS="MSR",MT="MLT",
        MU="MUS",MV="MDV",MW="MWI",MX="MEX",MY="MYS",MZ="MOZ","NA"="NAM",NC="NCL",
        NE="NER",NF="NFK",NG="NGA",NI="NIC",NL="NLD",NO="NOR",NP="NPL",NR="NRU",
        NU="NIU",NZ="NZL",OM="OMN",PA="PAN",PE="PER",PF="PYF",PG="PNG",PH="PHL",
        PK="PAK",PL="POL",PM="SPM",PN="PCN",PR="PRI",PS="PSE",PT="PRT",PW="PLW",
        PY="PRY",QA="QAT",RE="REU",RO="ROU",RS="SRB",RU="RUS",RW="RWA",SA="SAU",
        SB="SLB",SC="SYC",SD="SDN",SE="SWE",SG="SGP",SH="SHN",SI="SVN",SK="SVK",
        SL="SLE",SM="SMR",SN="SEN",SO="SOM",SR="SUR",SS="SSD",ST="STP",SV="SLV",
        SX="SXM",SY="SYR",SZ="SWZ",TC="TCA",TD="TCD",TG="TGO",TH="THA",TJ="TJK",
        TL="TLS",TM="TKM",TN="TUN",TO="TON",TR="TUR",TT="TTO",TV="TUV",TW="TWN",
        TZ="TZA",UA="UKR",UG="UGA",US="USA",UY="URY",UZ="UZB",VA="VAT",VC="VCT",
        VE="VEN",VG="VGB",VI="VIR",VN="VNM",VU="VUT",WF="WLF",WS="WSM",XK="XKX",
        YE="YEM",YT="MYT",ZA="ZAF",ZM="ZMB",ZW="ZWE"
      )

      # ---- Map rendering (#348: drill-down via click) ----
      output$geo_map <- plotly::renderPlotly({
        view <- view_mode()
        col <- kpi_col()
        label <- kpi_label()

        if (view == "us_states") {
          # ---- US States View ----
          df <- state_data()
          if (is.null(df)) {
            return(plotly::plot_ly() %>%
                     plotly::layout(title = translate("No Geographic Data")))
          }

          hover_text <- paste0(
            "<b>", df$ship_state, "</b><br>",
            translate("Revenue"), ": $", format(round(df$total_revenue, 0), big.mark = ","), "<br>",
            translate("Order Count"), ": ", format(df$order_count, big.mark = ","), "<br>",
            translate("Customer Count"), ": ", format(df$customer_count, big.mark = ","), "<br>",
            translate("Avg Order Value"), ": $", format(round(df$avg_order_value, 0), big.mark = ",")
          )

          raw_vals <- df[[col]]
          log_vals <- log10(pmax(raw_vals, 1))
          max_log <- ceiling(max(log_vals, na.rm = TRUE))
          tick_vals <- seq(0, max_log, by = 1)
          tick_text <- vapply(tick_vals, function(v) {
            val <- 10^v
            if (val >= 1e6) paste0(round(val / 1e6, 1), "M")
            else if (val >= 1e3) paste0(round(val / 1e3, 0), "K")
            else as.character(round(val, 0))
          }, character(1))

          # JS: detect clicks on map background (not on a state trace)
          # plotly_click only fires on data points; DOM click fires everywhere.
          # If DOM click fires without a plotly_click within 200ms, it's a background click.
          bg_click_js <- sprintf("
            function(el) {
              var ns = '%s';
              var lastDataClick = 0;
              el.on('plotly_click', function() { lastDataClick = Date.now(); });
              el.querySelector('.main-svg').addEventListener('click', function() {
                setTimeout(function() {
                  if (Date.now() - lastDataClick > 200) {
                    Shiny.setInputValue(ns + 'state_bg_click', Date.now(), {priority: 'event'});
                  }
                }, 250);
              });
            }", session$ns(""))

          plotly::plot_geo(df, source = session$ns("statemap")) %>%
            plotly::add_trace(
              locations = df$ship_state,
              locationmode = "USA-states",
              z = log_vals,
              colorscale = "Blues",
              text = hover_text,
              hoverinfo = "text",
              colorbar = list(title = label, tickvals = tick_vals, ticktext = tick_text)
            ) %>%
            plotly::layout(
              geo = list(
                scope = "usa",
                showlakes = TRUE,
                lakecolor = plotly::toRGB("white")
              ),
              margin = list(l = 0, r = 0, t = 30, b = 0),
              dragmode = FALSE  # Disable pan in state view
            ) %>%
            plotly::config(scrollZoom = FALSE) %>%  # Disable scroll zoom in state view
            htmlwidgets::onRender(bg_click_js)

        } else {
          # ---- World View ----
          df <- geo_data()
          if (is.null(df)) {
            return(plotly::plot_ly() %>%
                     plotly::layout(title = translate("No Geographic Data")))
          }

          # Convert ISO2 to ISO3 for plotly
          iso3_codes <- iso2to3[df$ship_country]
          iso3_codes[is.na(iso3_codes)] <- df$ship_country[is.na(iso3_codes)]

          hover_text <- paste0(
            "<b>", country_name_zh(df$ship_country), "</b><br>",
            translate("Revenue"), ": $", format(round(df$total_revenue, 0), big.mark = ","), "<br>",
            translate("Order Count"), ": ", format(df$order_count, big.mark = ","), "<br>",
            translate("Customer Count"), ": ", format(df$customer_count, big.mark = ","), "<br>",
            translate("Avg Order Value"), ": $", format(round(df$avg_order_value, 0), big.mark = ",")
          )

          raw_vals <- df[[col]]
          log_vals <- log10(pmax(raw_vals, 1))
          max_log <- ceiling(max(log_vals, na.rm = TRUE))
          tick_vals <- seq(0, max_log, by = 1)
          tick_text <- vapply(tick_vals, function(v) {
            val <- 10^v
            if (val >= 1e6) paste0(round(val / 1e6, 1), "M")
            else if (val >= 1e3) paste0(round(val / 1e3, 0), "K")
            else as.character(round(val, 0))
          }, character(1))

          # source must be namespaced; customdata carries ISO3 codes for click detection
          plotly::plot_geo(df, source = world_source) %>%
            plotly::add_trace(
              locations = iso3_codes,
              locationmode = "ISO-3",
              z = log_vals,
              customdata = iso3_codes,
              colorscale = "Blues",
              text = hover_text,
              hoverinfo = "text",
              colorbar = list(title = label, tickvals = tick_vals, ticktext = tick_text)
            ) %>%
            plotly::layout(
              geo = list(
                projection = list(type = "natural earth"),
                showframe = FALSE,
                showcoastlines = TRUE,
                coastlinecolor = plotly::toRGB("grey80")
              ),
              margin = list(l = 0, r = 0, t = 30, b = 0)
            )
        }
      })

      # ---- Detail Table ----
      output$detail_table <- DT::renderDataTable({
        view <- view_mode()

        if (view == "us_states") {
          df <- state_data()
          if (is.null(df)) return(DT::datatable(
            data.frame(Message = translate("No Geographic Data"))))

          show_df <- data.frame(
            State         = df$ship_state,
            Revenue       = round(df$total_revenue, 0),
            Orders        = df$order_count,
            Customers     = df$customer_count,
            AOV           = round(df$avg_order_value, 0),
            Avg_Qty       = round(df$avg_quantity, 1),
            stringsAsFactors = FALSE
          )
          show_df <- show_df[order(-show_df$Revenue), ]
          names(show_df) <- c(translate("State"), translate("Revenue"),
                              translate("Order Count"), translate("Customer Count"),
                              translate("Avg Order Value"), translate("Avg Qty"))
        } else {
          df <- geo_data()
          if (is.null(df)) return(DT::datatable(
            data.frame(Message = translate("No Geographic Data"))))

          show_df <- data.frame(
            Country       = country_name_zh(df$ship_country, with_code = TRUE),
            Revenue       = round(df$total_revenue, 0),
            Orders        = df$order_count,
            Customers     = df$customer_count,
            AOV           = round(df$avg_order_value, 0),
            Avg_Qty       = round(df$avg_quantity, 1),
            stringsAsFactors = FALSE
          )
          show_df <- show_df[order(-show_df$Revenue), ]
          names(show_df) <- c(translate("Country"), translate("Revenue"),
                              translate("Order Count"), translate("Customer Count"),
                              translate("Avg Order Value"), translate("Avg Qty"))
        }

        DT::datatable(show_df,
                      filter = "top", rownames = FALSE,
                      options = list(pageLength = 15, scrollX = TRUE, dom = "lftip",
                                     language = list(url = "//cdn.datatables.net/plug-ins/1.13.7/i18n/zh-HANT.json")))
      })

      # ---- AI Insight (non-blocking, ExtendedTask) ----
      gpt_key <- Sys.getenv("OPENAI_API_KEY", "")
      ai_task <- create_ai_insight_task(gpt_key)

      setup_ai_insight_server(
        input, output, session, ns,
        task = ai_task,
        gpt_key = gpt_key,
        prompt_key = "vitalsigns_analysis.world_map_insights",
        get_template_vars = function() {
          df <- geo_data()
          if (is.null(df) || nrow(df) == 0) return(NULL)

          ordered <- df[order(-df$total_revenue), ]
          total_rev <- sum(ordered$total_revenue, na.rm = TRUE)

          # Country revenue summary (top 10)
          top_n <- utils::head(ordered, 10)
          country_lines <- vapply(seq_len(nrow(top_n)), function(i) {
            pct <- round(top_n$total_revenue[i] / total_rev * 100, 1)
            sprintf("%s: $%s (%s%%, %d orders, %d customers)",
                    country_name_zh(top_n$ship_country[i]),
                    format(round(top_n$total_revenue[i], 0), big.mark = ","),
                    pct, top_n$order_count[i], top_n$customer_count[i])
          }, character(1))

          # Concentration
          top1_pct <- round(ordered$total_revenue[1] / total_rev * 100, 1)
          top3_pct <- round(sum(utils::head(ordered$total_revenue, 3)) / total_rev * 100, 1)
          top5_pct <- round(sum(utils::head(ordered$total_revenue, 5)) / total_rev * 100, 1)

          # Build filter context for AI (#324)
          cfg <- comp_config()
          pl <- cfg$filters$product_line_id_sliced
          kpi_sel <- input$kpi_select
          kpi_label_map <- c(revenue = "Revenue", orders = "Order Count",
                             customers = "Customer Count", aov = "Avg Order Value")
          kpi_display <- if (!is.null(kpi_sel) && kpi_sel %in% names(kpi_label_map))
            kpi_label_map[[kpi_sel]] else "Revenue"
          filter_context_str <- paste0(
            "Analysis scope:\n",
            "- Platform: ", cfg$filters$platform_id, "\n",
            "- Product line: ", if (!is.null(pl) && pl != "all") pl else "All", "\n",
            "- Current view: ", view_mode(), "\n",
            "- Selected KPI: ", kpi_display
          )

          # P2b: Only include state data in AI context when drilled into US states
          state_summary_str <- if (view_mode() == "us_states") {
            sd <- state_data()
            if (!is.null(sd) && nrow(sd) > 0) {
              sd_ordered <- sd[order(-sd$total_revenue), ]
              top_states <- utils::head(sd_ordered, 5)
              state_lines <- vapply(seq_len(nrow(top_states)), function(i) {
                sprintf("%s: $%s (%d orders)",
                        top_states$ship_state[i],
                        format(round(top_states$total_revenue[i], 0), big.mark = ","),
                        top_states$order_count[i])
              }, character(1))
              paste0("US state breakdown (top 5):\n", paste(state_lines, collapse = "\n"))
            } else {
              "US state data: not available"
            }
          } else {
            ""  # World view: no state-level detail in AI context
          }

          list(
            filter_context = filter_context_str,
            state_summary = state_summary_str,
            total_countries = as.character(nrow(df)),
            largest_market = sprintf("%s ($%s, %.1f%%)",
                                    country_name_zh(ordered$ship_country[1]),
                                    format(round(ordered$total_revenue[1], 0), big.mark = ","),
                                    top1_pct),
            country_revenue_summary = paste(country_lines, collapse = "\n"),
            concentration_summary = sprintf(
              "Top 1: %.1f%%\nTop 3: %.1f%%\nTop 5: %.1f%%\nTotal countries: %d",
              top1_pct, top3_pct, top5_pct, nrow(df))
          )
        },
        component_label = "worldMap"
      )

      # ---- Download CSV ----
      output$download_csv <- downloadHandler(
        filename = function() paste0("world_market_", Sys.Date(), ".csv"),
        content = function(file) {
          df <- geo_data()
          if (!is.null(df)) {
            con <- file(file, "wb")
            writeBin(charToRaw("\xef\xbb\xbf"), con)
            close(con)
            # write.table (NOT write.csv) — write.csv ignores append=TRUE (DEV_R051)
            utils::write.table(df, file, row.names = FALSE, sep = ",",
                               quote = TRUE, append = TRUE, fileEncoding = "UTF-8")
          }
        }
      )

    })
  }

  list(ui = list(filter = ui_filter, display = ui_display), server = server_fn)
}
