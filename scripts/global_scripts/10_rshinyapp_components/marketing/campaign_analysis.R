# Campaign Analysis Module
# Analyzes marketing campaign performance

#' Campaign Analysis UI Function
#'
#' @param id The module ID
#'
#' @return A UI component
#'
campaignAnalysisUI <- function(id) {
  ns <- NS(id)
  
  nav_panel(
    title = "行銷活動分析",
    card(
      full_screen = TRUE,
      card_body(
        grid_container(
          layout = c(
            "area1 area1",
            "area2 area3",
            "area4 area4"
          ),
          row_sizes = c(
            "150px",
            "300px",
            "250px"
          ),
          col_sizes = c(
            "1fr",
            "1fr"
          ),
          gap_size = "10px",
          
          # Area 1: Controls
          grid_card(
            area = "area1",
            card_header("選擇行銷活動"),
            card_body(
              layout_column_wrap(
                width = "250px",
                fill = FALSE,
                
                # Campaign selector
                selectizeInput(
                  inputId = ns("campaign_selector"),
                  label = "行銷活動",
                  choices = NULL,
                  multiple = FALSE,
                  options = list(plugins = list('remove_button', 'drag_drop'))
                ),
                
                # Date range selector
                dateRangeInput(
                  inputId = ns("date_range"),
                  label = "日期範圍",
                  start = Sys.Date() - 90,
                  end = Sys.Date(),
                  format = "yyyy-mm-dd"
                )
              )
            )
          ),
          
          # Area 2: Campaign performance metrics
          grid_card(
            area = "area2",
            card_header("活動績效指標"),
            card_body(
              layout_column_wrap(
                width = "180px",
                fill = FALSE,
                
                value_box(
                  title = "總銷售額",
                  value = textOutput(ns("total_revenue")),
                  showcase = bs_icon("cash-coin"),
                  p("美金")
                ),
                value_box(
                  title = "平均訂單金額",
                  value = textOutput(ns("average_order_value")),
                  showcase = bs_icon("cart-check"),
                  p("美金")
                ),
                value_box(
                  title = "轉化率",
                  value = textOutput(ns("conversion_rate")),
                  showcase = bs_icon("arrow-down-right-circle")
                ),
                value_box(
                  title = "ROI",
                  value = textOutput(ns("roi")),
                  showcase = bs_icon("graph-up-arrow")
                )
              )
            )
          ),
          
          # Area 3: Campaign performance chart
          grid_card(
            area = "area3",
            card_header("活動績效趨勢"),
            card_body(
              plotOutput(ns("campaign_trend_plot"), height = "250px")
            )
          ),
          
          # Area 4: Campaign impact analysis
          grid_card(
            area = "area4",
            card_header("活動影響分析"),
            card_body(
              tabsetPanel(
                tabPanel(
                  "客戶細分",
                  plotOutput(ns("segment_impact_plot"), height = "200px")
                ),
                tabPanel(
                  "地區分析",
                  plotOutput(ns("region_impact_plot"), height = "200px")
                ),
                tabPanel(
                  "成本分析",
                  plotOutput(ns("cost_analysis_plot"), height = "200px")
                )
              )
            )
          )
        )
      )
    )
  )
}

#' Campaign Analysis Server Function
#'
#' @param id The module ID
#' @param data_source The data source reactive list
#'
#' @return None
#'
campaignAnalysisServer <- function(id, data_source) {
  moduleServer(id, function(input, output, session) {
    # Sample campaign data (would be replaced with actual data from database)
    campaigns <- reactive({
      data.frame(
        campaign_id = c("camp_001", "camp_002", "camp_003", "camp_004"),
        campaign_name = c("新春特促", "夏季大促", "會員專屬優惠", "黑色星期五"),
        start_date = as.Date(c("2023-01-15", "2023-06-01", "2023-09-10", "2023-11-24")),
        end_date = as.Date(c("2023-02-15", "2023-07-01", "2023-10-10", "2023-12-01")),
        budget = c(5000, 8000, 3000, 10000)
      )
    })
    
    # Update campaign selector
    observe({
      campaign_options <- setNames(
        campaigns()$campaign_id,
        campaigns()$campaign_name
      )
      
      updateSelectizeInput(
        session,
        "campaign_selector",
        choices = campaign_options,
        selected = campaign_options[1]
      )
    })
    
    # Get selected campaign
    selected_campaign <- reactive({
      req(input$campaign_selector)
      campaigns() %>% filter(campaign_id == input$campaign_selector)
    })
    
    # Get sales data in date range
    filtered_sales_data <- reactive({
      req(input$date_range)
      
      # This would be replaced with actual filtering logic
      # For now, we'll use placeholder data
      data.frame(
        date = seq.Date(from = input$date_range[1], to = input$date_range[2], by = "day"),
        revenue = runif(as.numeric(difftime(input$date_range[2], input$date_range[1], units = "days")) + 1, 500, 2000),
        orders = sample(10:50, as.numeric(difftime(input$date_range[2], input$date_range[1], units = "days")) + 1, replace = TRUE),
        visitors = sample(100:300, as.numeric(difftime(input$date_range[2], input$date_range[1], units = "days")) + 1, replace = TRUE)
      )
    })
    
    # Render campaign metrics
    output$total_revenue <- renderText({
      format(sum(filtered_sales_data()$revenue), big.mark = ",", digits = 2)
    })
    
    output$average_order_value <- renderText({
      format(sum(filtered_sales_data()$revenue) / sum(filtered_sales_data()$orders), digits = 2)
    })
    
    output$conversion_rate <- renderText({
      paste0(format(sum(filtered_sales_data()$orders) / sum(filtered_sales_data()$visitors) * 100, digits = 2), "%")
    })
    
    output$roi <- renderText({
      campaign_budget <- selected_campaign()$budget
      total_revenue <- sum(filtered_sales_data()$revenue)
      roi <- (total_revenue - campaign_budget) / campaign_budget * 100
      paste0(format(roi, digits = 2), "%")
    })
    
    # Render campaign trend plot
    output$campaign_trend_plot <- renderPlot({
      req(filtered_sales_data())
      
      # Aggregate by week for better visibility
      weekly_data <- filtered_sales_data() %>%
        mutate(week = floor_date(date, "week")) %>%
        group_by(week) %>%
        summarise(
          revenue = sum(revenue),
          orders = sum(orders),
          visitors = sum(visitors)
        )
      
      ggplot(weekly_data, aes(x = week)) +
        geom_line(aes(y = revenue, color = "Revenue"), size = 1.2) +
        geom_point(aes(y = revenue, color = "Revenue"), size = 3) +
        scale_y_continuous(name = "Revenue (USD)") +
        scale_x_date(date_breaks = "1 week", date_labels = "%b %d") +
        scale_color_manual(values = c("Revenue" = "#1976D2")) +
        theme_minimal() +
        labs(
          x = "Date",
          y = "Revenue (USD)",
          title = "活動期間銷售趨勢",
          color = ""
        ) +
        theme(
          axis.text.x = element_text(angle = 45, hjust = 1),
          plot.title = element_text(hjust = 0.5),
          legend.position = "bottom"
        )
    })
    
    # Render segment impact plot
    output$segment_impact_plot <- renderPlot({
      # Placeholder data for customer segments
      segment_data <- data.frame(
        segment = c("新客戶", "主力客戶", "瞌睡客戶", "半睡客戶", "沉睡客戶"),
        before_campaign = c(20, 35, 15, 20, 10),
        during_campaign = c(30, 40, 10, 15, 5)
      )
      
      segment_data_long <- segment_data %>%
        pivot_longer(
          cols = c(before_campaign, during_campaign),
          names_to = "period",
          values_to = "percentage"
        ) %>%
        mutate(period = factor(period, levels = c("before_campaign", "during_campaign"),
                               labels = c("活動前", "活動期間")))
      
      ggplot(segment_data_long, aes(x = segment, y = percentage, fill = period)) +
        geom_bar(stat = "identity", position = "dodge") +
        theme_minimal() +
        labs(
          x = "客戶細分",
          y = "百分比 (%)",
          title = "活動前後客戶細分變化",
          fill = ""
        ) +
        theme(
          plot.title = element_text(hjust = 0.5),
          axis.text.x = element_text(angle = 45, hjust = 1)
        ) +
        scale_fill_brewer(palette = "Set2")
    })
    
    # Render region impact plot
    output$region_impact_plot <- renderPlot({
      # Placeholder data for regional impact
      region_data <- data.frame(
        region = c("加州", "紐約", "德州", "佛羅里達", "伊利諾伊"),
        revenue_increase = c(35, 28, 22, 18, 15)
      )
      
      ggplot(region_data, aes(x = reorder(region, -revenue_increase), y = revenue_increase, fill = revenue_increase)) +
        geom_bar(stat = "identity") +
        theme_minimal() +
        labs(
          x = "地區",
          y = "銷售額增長 (%)",
          title = "按地區劃分的銷售額增長"
        ) +
        theme(
          plot.title = element_text(hjust = 0.5),
          legend.position = "none"
        ) +
        scale_fill_gradient(low = "#64B5F6", high = "#1565C0")
    })
    
    # Render cost analysis plot
    output$cost_analysis_plot <- renderPlot({
      # Placeholder data for cost analysis
      cost_data <- data.frame(
        category = c("廣告支出", "折扣", "行銷素材", "人力成本", "其他"),
        amount = c(3500, 2800, 1200, 1500, 1000)
      )
      
      ggplot(cost_data, aes(x = "", y = amount, fill = category)) +
        geom_bar(stat = "identity", width = 1) +
        coord_polar("y", start = 0) +
        theme_minimal() +
        labs(
          title = "活動成本分佈",
          fill = "類別"
        ) +
        theme(
          axis.title = element_blank(),
          axis.text = element_blank(),
          plot.title = element_text(hjust = 0.5)
        ) +
        scale_fill_brewer(palette = "Set3")
    })
  })
}