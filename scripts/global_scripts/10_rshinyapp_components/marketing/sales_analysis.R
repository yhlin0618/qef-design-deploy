# Sales Analysis Module
# Shows detailed sales metrics and trends

#' Sales Analysis UI Function
#'
#' @param id The module ID
#'
#' @return A UI component
#'
salesAnalysisUI <- function(id) {
  ns <- NS(id)
  
  nav_panel(
    title = "銷售分析",
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
            "250px",
            "250px",
            "250px"
          ),
          col_sizes = c(
            "1fr",
            "1fr"
          ),
          gap_size = "10px",
          
          # Area 1: Sales over time chart
          grid_card(
            area = "area1",
            card_header("銷售趨勢"),
            card_body(
              plotOutput(ns("sales_trend_plot"), height = "200px")
            )
          ),
          
          # Area 2: Distribution by customer segment
          grid_card(
            area = "area2",
            card_header("客戶細分的銷售分佈"),
            card_body(
              plotOutput(ns("customer_segment_plot"), height = "200px")
            )
          ),
          
          # Area 3: Top products
          grid_card(
            area = "area3",
            card_header("熱銷產品"),
            card_body(
              plotOutput(ns("top_products_plot"), height = "200px")
            )
          ),
          
          # Area 4: Sales table with details
          grid_card(
            area = "area4",
            card_header("銷售詳情"),
            card_body(
              layout_column_wrap(
                width = "100%",
                fill = FALSE,
                style = htmltools::css(overflow = "auto"),
                DT::dataTableOutput(ns("sales_table"), width = "100%")
              )
            )
          )
        )
      )
    )
  )
}

#' Sales Analysis Server Function
#'
#' @param id The module ID
#' @param data_source The data source reactive list
#'
#' @return None
#'
salesAnalysisServer <- function(id, data_source) {
  moduleServer(id, function(input, output, session) {
    # Get sales data from data source
    sales_data <- reactive({
      data_source$sales_by_time_state()
    })
    
    # Get customer data for segment analysis
    customer_data <- reactive({
      data_source$sales_by_customer()
    })
    
    # Sales trend plot
    output$sales_trend_plot <- renderPlot({
      req(sales_data())
      
      sales_data() %>%
        group_by(time_scale) %>%
        summarise(total_sales = sum(total, na.rm = TRUE)) %>%
        ggplot(aes(x = time_scale, y = total_sales)) +
        geom_line(color = "#0073C2", size = 1.2) +
        geom_point(color = "#0073C2", size = 3) +
        theme_minimal() +
        labs(
          x = "時間",
          y = "總銷售額",
          title = "銷售額隨時間的變化"
        ) +
        theme(
          axis.text.x = element_text(angle = 45, hjust = 1),
          plot.title = element_text(hjust = 0.5)
        )
    })
    
    # Customer segment distribution plot
    output$customer_segment_plot <- renderPlot({
      req(customer_data())
      
      customer_data() %>%
        group_by(nesstatus) %>%
        summarise(
          segment_sales = sum(total, na.rm = TRUE),
          count = n()
        ) %>%
        mutate(
          segment_label = case_when(
            nesstatus == "N" ~ "新客戶",
            nesstatus == "E0" ~ "主力客戶",
            nesstatus == "S1" ~ "瞌睡客戶",
            nesstatus == "S2" ~ "半睡客戶",
            nesstatus == "S3" ~ "沉睡客戶",
            TRUE ~ "其他"
          ),
          segment_label = paste0(segment_label, " (", count, ")")
        ) %>%
        ggplot(aes(x = "", y = segment_sales, fill = segment_label)) +
        geom_bar(stat = "identity", width = 1) +
        coord_polar("y", start = 0) +
        theme_minimal() +
        labs(
          fill = "客戶細分",
          title = "各細分客戶的銷售額分佈"
        ) +
        theme(
          plot.title = element_text(hjust = 0.5),
          axis.text = element_blank(),
          axis.title = element_blank()
        ) +
        scale_fill_brewer(palette = "Set3")
    })
    
    # Top products plot
    output$top_products_plot <- renderPlot({
      req(customer_data())
      
      # This is a placeholder - in a real implementation,
      # you would use product sales data
      product_data <- data.frame(
        product_name = c("Product A", "Product B", "Product C", "Product D", "Product E"),
        sales = c(1200, 980, 750, 620, 450)
      )
      
      product_data %>%
        ggplot(aes(x = reorder(product_name, -sales), y = sales)) +
        geom_bar(stat = "identity", fill = "#4CAF50") +
        theme_minimal() +
        labs(
          x = "產品",
          y = "銷售額",
          title = "熱銷產品"
        ) +
        theme(
          axis.text.x = element_text(angle = 45, hjust = 1),
          plot.title = element_text(hjust = 0.5)
        )
    })
    
    # Sales table
    output$sales_table <- DT::renderDataTable({
      req(sales_data())
      
      sales_data() %>%
        group_by(time_interval_label) %>%
        summarise(
          總銷售額 = sum(total, na.rm = TRUE),
          銷售訂單數 = sum(num_customers, na.rm = TRUE),
          平均訂單價值 = mean(average, na.rm = TRUE),
          新客戶數 = sum(new_customers, na.rm = TRUE),
          客戶保留率 = mean(customer_retention_rate, na.rm = TRUE)
        ) %>%
        arrange(desc(time_interval_label)) %>%
        mutate(
          平均訂單價值 = round(平均訂單價值, 2),
          客戶保留率 = scales::percent(客戶保留率, accuracy = 0.1)
        )
    }, options = list(
      pageLength = 5,
      lengthMenu = c(5, 10, 15),
      dom = 'tip',
      scrollX = TRUE
    ))
  })
}