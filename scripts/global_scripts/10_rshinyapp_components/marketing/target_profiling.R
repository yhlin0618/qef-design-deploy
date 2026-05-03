# Target Profiling Module
# Provides analysis for ideal customer profiles and targeting

#' Target Profiling UI Function
#'
#' @param id The module ID
#'
#' @return A UI component
#'
targetProfilingUI <- function(id) {
  ns <- NS(id)
  
  nav_panel(
    title = "目標客群分析",
    card(
      full_screen = TRUE,
      card_body(
        grid_container(
          layout = c(
            "area1 area2",
            "area3 area4"
          ),
          row_sizes = c(
            "350px",
            "350px"
          ),
          col_sizes = c(
            "1fr",
            "1fr"
          ),
          gap_size = "10px",
          
          # Area 1: Customer segment breakdown
          grid_card(
            area = "area1",
            card_header("客戶細分"),
            card_body(
              plotOutput(ns("customer_segments_plot"), height = "300px")
            )
          ),
          
          # Area 2: Geographic distribution
          grid_card(
            area = "area2",
            card_header("地理分佈"),
            card_body(
              plotOutput(ns("geographic_distribution_plot"), height = "300px")
            )
          ),
          
          # Area 3: Purchase frequency
          grid_card(
            area = "area3",
            card_header("購買頻率"),
            card_body(
              plotOutput(ns("purchase_frequency_plot"), height = "300px")
            )
          ),
          
          # Area 4: Customer lifetime value
          grid_card(
            area = "area4",
            card_header("顧客終身價值"),
            card_body(
              layout_column_wrap(
                width = "100%",
                height = "100%",
                fill = FALSE,
                plotOutput(ns("customer_lifetime_plot"), height = "300px")
              )
            )
          )
        )
      )
    )
  )
}

#' Target Profiling Server Function
#'
#' @param id The module ID
#' @param data_source The data source reactive list
#'
#' @return None
#'
targetProfilingServer <- function(id, data_source) {
  moduleServer(id, function(input, output, session) {
    # Get customer data
    customer_data <- reactive({
      data_source$sales_by_customer()
    })
    
    # Get geographic data
    geo_data <- reactive({
      data_source$sales_by_time_state()
    })
    
    # Customer segments plot
    output$customer_segments_plot <- renderPlot({
      req(customer_data())
      
      customer_data() %>%
        group_by(nesstatus) %>%
        summarise(count = n()) %>%
        mutate(
          segment_label = case_when(
            nesstatus == "N" ~ "新客戶",
            nesstatus == "E0" ~ "主力客戶",
            nesstatus == "S1" ~ "瞌睡客戶",
            nesstatus == "S2" ~ "半睡客戶",
            nesstatus == "S3" ~ "沉睡客戶",
            TRUE ~ "其他"
          ),
          segment_label = paste0(segment_label, " (", count, ")"),
          percentage = count / sum(count) * 100
        ) %>%
        ggplot(aes(x = reorder(segment_label, -percentage), y = percentage, fill = segment_label)) +
        geom_bar(stat = "identity") +
        theme_minimal() +
        labs(
          x = "",
          y = "百分比 (%)",
          title = "客戶細分分佈"
        ) +
        theme(
          legend.position = "none",
          axis.text.x = element_text(angle = 45, hjust = 1),
          plot.title = element_text(hjust = 0.5)
        ) +
        scale_fill_brewer(palette = "Set3")
    })
    
    # Geographic distribution plot
    output$geographic_distribution_plot <- renderPlot({
      req(geo_data())
      
      geo_data() %>%
        group_by(state_filter) %>%
        summarise(total_sales = sum(total, na.rm = TRUE)) %>%
        arrange(desc(total_sales)) %>%
        head(10) %>%
        ggplot(aes(x = reorder(state_filter, total_sales), y = total_sales, fill = state_filter)) +
        geom_bar(stat = "identity") +
        coord_flip() +
        theme_minimal() +
        labs(
          x = "州",
          y = "總銷售額",
          title = "前10個州的銷售額"
        ) +
        theme(
          legend.position = "none",
          plot.title = element_text(hjust = 0.5)
        ) +
        scale_fill_viridis_d()
    })
    
    # Purchase frequency plot
    output$purchase_frequency_plot <- renderPlot({
      req(customer_data())
      
      customer_data() %>%
        ggplot(aes(x = fvalue)) +
        geom_histogram(bins = 30, fill = "#69b3a2", color = "white", alpha = 0.8) +
        theme_minimal() +
        labs(
          x = "購買頻率",
          y = "客戶數量",
          title = "客戶購買頻率分佈"
        ) +
        theme(plot.title = element_text(hjust = 0.5))
    })
    
    # Customer lifetime value plot
    output$customer_lifetime_plot <- renderPlot({
      req(customer_data())
      
      customer_data() %>%
        ggplot(aes(x = clv)) +
        geom_histogram(bins = 30, fill = "#404080", color = "white", alpha = 0.8) +
        theme_minimal() +
        labs(
          x = "顧客終身價值 (CLV)",
          y = "客戶數量",
          title = "客戶終身價值分佈"
        ) +
        theme(plot.title = element_text(hjust = 0.5))
    })
  })
}