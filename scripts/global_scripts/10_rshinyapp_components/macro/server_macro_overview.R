#' Macro Overview Server Component
#'
#' This component provides the server-side logic for the macro overview dashboard.
#' It processes data and renders the KPI metrics.
#'
#' @param id The component ID
#' @param data_source The data source reactive list
#'
#' @return A server function for the macro overview dashboard
#' 
#' @examples
#' # In the server function
#' macroOverviewServer("macro_section", data_source)
#'
#' @export
macroOverviewServer <- function(id, data_source) {
  moduleServer(id, function(input, output, session) {
    # Get the latest time period data
    current_period_data <- reactive({
      sales_data <- data_source$sales_by_time_state()
      
      # Filter to the most recent time period
      latest_time <- max(sales_data$time_scale, na.rm = TRUE)
      sales_data %>% filter(time_scale == latest_time)
    })
    
    # Get the previous time period data
    previous_period_data <- reactive({
      sales_data <- data_source$sales_by_time_state()
      
      # Filter to the second most recent time period
      time_periods <- sort(unique(sales_data$time_scale), decreasing = TRUE)
      if (length(time_periods) >= 2) {
        previous_time <- time_periods[2]
        sales_data %>% filter(time_scale == previous_time)
      } else {
        NULL
      }
    })
    
    # Get NES transition data
    nes_data <- reactive({
      data_source$nes_transition()
    })
    
    # Render KPI outputs
    observe({
      # Get current and previous data
      curr <- current_period_data()
      prev <- previous_period_data()
      nes <- nes_data()
      
      if (is.null(curr) || nrow(curr) == 0) {
        return()
      }
      
      # Render values for metrics
      # Sales metrics
      output$total_now <- renderText({ format(sum(curr$total, na.rm = TRUE), big.mark = ",") })
      output$average_now <- renderText({ format(mean(curr$average, na.rm = TRUE), big.mark = ",", digits = 2) })
      output$num_customers_now <- renderText({ format(sum(curr$num_customers, na.rm = TRUE), big.mark = ",") })
      output$cum_customers_now <- renderText({ format(sum(curr$cum_customers, na.rm = TRUE), big.mark = ",") })
      
      # Customer retention and acquisition
      output$customer_retention_rate_now_perc <- renderText({ 
        scales::percent(mean(curr$customer_retention_rate, na.rm = TRUE), accuracy = 0.1) 
      })
      output$customer_acquisition_rate_now_perc <- renderText({ 
        scales::percent(mean(curr$customer_acquisition_rate, na.rm = TRUE), accuracy = 0.1) 
      })
      
      # NES transition rate (N to E0)
      output$N_to_E0_prop_perc <- renderText({
        if (!is.null(nes)) {
          n_to_e0 <- nes %>% filter(nesstatus_pre == "N" & nesstatus_now == "E0")
          if (nrow(n_to_e0) > 0) {
            scales::percent(n_to_e0$activation_rate[1], accuracy = 0.1)
          } else {
            "N/A"
          }
        } else {
          "N/A"
        }
      })
      
      # Calculate differences if previous period data exists
      if (!is.null(prev) && nrow(prev) > 0) {
        # Sales metrics differences
        total_diff <- sum(curr$total, na.rm = TRUE) - sum(prev$total, na.rm = TRUE)
        total_diff_perc <- total_diff / sum(prev$total, na.rm = TRUE)
        
        average_diff <- mean(curr$average, na.rm = TRUE) - mean(prev$average, na.rm = TRUE)
        average_diff_perc <- average_diff / mean(prev$average, na.rm = TRUE)
        
        num_customers_diff <- sum(curr$num_customers, na.rm = TRUE) - sum(prev$num_customers, na.rm = TRUE)
        num_customers_diff_perc <- num_customers_diff / sum(prev$num_customers, na.rm = TRUE)
        
        cum_customers_diff <- sum(curr$cum_customers, na.rm = TRUE) - sum(prev$cum_customers, na.rm = TRUE)
        cum_customers_diff_perc <- cum_customers_diff / sum(prev$cum_customers, na.rm = TRUE)
        
        # Customer rate differences
        retention_diff <- mean(curr$customer_retention_rate, na.rm = TRUE) - mean(prev$customer_retention_rate, na.rm = TRUE)
        retention_diff_perc <- retention_diff / mean(prev$customer_retention_rate, na.rm = TRUE)
        
        acquisition_diff <- mean(curr$customer_acquisition_rate, na.rm = TRUE) - mean(prev$customer_acquisition_rate, na.rm = TRUE)
        acquisition_diff_perc <- acquisition_diff / mean(prev$customer_acquisition_rate, na.rm = TRUE)
        
        # Render difference indicators
        output$total_diff <- renderUI({
          if (total_diff >= 0) {
            bs_icon("graph-up-arrow", class = "text-success")
          } else {
            bs_icon("graph-down-arrow", class = "text-danger")
          }
        })
        output$total_diff_perc <- renderText({ scales::percent(total_diff_perc, accuracy = 0.1) })
        
        output$average_diff <- renderUI({
          if (average_diff >= 0) {
            bs_icon("graph-up-arrow", class = "text-success")
          } else {
            bs_icon("graph-down-arrow", class = "text-danger")
          }
        })
        output$average_diff_perc <- renderText({ scales::percent(average_diff_perc, accuracy = 0.1) })
        
        output$num_customers_diff <- renderUI({
          if (num_customers_diff >= 0) {
            bs_icon("graph-up-arrow", class = "text-success")
          } else {
            bs_icon("graph-down-arrow", class = "text-danger")
          }
        })
        output$num_customers_diff_perc <- renderText({ scales::percent(num_customers_diff_perc, accuracy = 0.1) })
        
        output$cum_customers_diff <- renderUI({
          if (cum_customers_diff >= 0) {
            bs_icon("graph-up-arrow", class = "text-success")
          } else {
            bs_icon("graph-down-arrow", class = "text-danger")
          }
        })
        output$cum_customers_diff_perc <- renderText({ scales::percent(cum_customers_diff_perc, accuracy = 0.1) })
        
        output$customer_retention_rate_diff <- renderUI({
          if (retention_diff >= 0) {
            bs_icon("graph-up-arrow", class = "text-success")
          } else {
            bs_icon("graph-down-arrow", class = "text-danger")
          }
        })
        output$customer_retention_rate_diff_value_perc <- renderText({ scales::percent(retention_diff_perc, accuracy = 0.1) })
        
        output$customer_acquisition_rate_diff <- renderUI({
          if (acquisition_diff >= 0) {
            bs_icon("graph-up-arrow", class = "text-success")
          } else {
            bs_icon("graph-down-arrow", class = "text-danger")
          }
        })
        output$customer_acquisition_rate_diff_value_perc <- renderText({ scales::percent(acquisition_diff_perc, accuracy = 0.1) })
      } else {
        # If no previous period data, show question mark
        output$total_diff <- renderUI({ bs_icon("question", class = "text-info") })
        output$average_diff <- renderUI({ bs_icon("question", class = "text-info") })
        output$num_customers_diff <- renderUI({ bs_icon("question", class = "text-info") })
        output$cum_customers_diff <- renderUI({ bs_icon("question", class = "text-info") })
        output$customer_retention_rate_diff <- renderUI({ bs_icon("question", class = "text-info") })
        output$customer_acquisition_rate_diff <- renderUI({ bs_icon("question", class = "text-info") })
        
        # Render NA for percentage changes
        output$total_diff_perc <- renderText({ "N/A" })
        output$average_diff_perc <- renderText({ "N/A" })
        output$num_customers_diff_perc <- renderText({ "N/A" })
        output$cum_customers_diff_perc <- renderText({ "N/A" })
        output$customer_retention_rate_diff_value_perc <- renderText({ "N/A" })
        output$customer_acquisition_rate_diff_value_perc <- renderText({ "N/A" })
      }
      
      # NES transition indicators
      output$N_to_E0_prop_diff <- renderUI({
        if (!is.null(nes)) {
          bs_icon("shift", class = "text-success")
        } else {
          bs_icon("question", class = "text-info")
        }
      })
      
      # Customer segment metrics would be populated based on NES data
      # This would be expanded with actual data in a full implementation
      output$E0_nes_prop_1_diff <- renderUI({ bs_icon("question", class = "text-info") })
      output$S3_nes_prop_1_diff <- renderUI({ bs_icon("question", class = "text-info") })
      output$N_nes_prop_diff <- renderUI({ bs_icon("question", class = "text-info") })
      output$E0_nes_prop_diff <- renderUI({ bs_icon("question", class = "text-info") })
      output$S1_nes_prop_diff <- renderUI({ bs_icon("question", class = "text-info") })
      output$S2_nes_prop_diff <- renderUI({ bs_icon("question", class = "text-info") })
      output$S3_nes_prop_diff <- renderUI({ bs_icon("question", class = "text-info") })
      output$N_nes_mmean_diff <- renderUI({ bs_icon("question", class = "text-info") })
      output$E0_nes_mmean_diff <- renderUI({ bs_icon("question", class = "text-info") })
      output$macro_clv_diff <- renderUI({ bs_icon("question", class = "text-info") })
      output$macro_cri_diff <- renderUI({ bs_icon("question", class = "text-info") })
      output$macro_nrec_diff <- renderUI({ bs_icon("question", class = "text-info") })
      output$macro_cai_diff <- renderUI({ bs_icon("question", class = "text-info") })
      
      # Placeholder values for customer segments
      # In a full implementation, these would be calculated from actual data
      output$E0_nes_prop_1_now_perc <- renderText({ "25.0%" })
      output$S3_nes_prop_1_now_perc <- renderText({ "15.0%" })
      output$N_nes_prop_now_perc <- renderText({ "30.0%" })
      output$E0_nes_prop_now_perc <- renderText({ "25.0%" })
      output$S1_nes_prop_now_perc <- renderText({ "20.0%" })
      output$S2_nes_prop_now_perc <- renderText({ "10.0%" })
      output$S3_nes_prop_now_perc <- renderText({ "15.0%" })
      output$N_nes_mmean_now <- renderText({ "45.00" })
      output$E0_nes_mmean_now <- renderText({ "85.00" })
      output$macro_clv_now <- renderText({ "125.00" })
      output$macro_cri_now <- renderText({ "0.85" })
      output$macro_nrec_now <- renderText({ "0.15" })
      output$macro_cai_now <- renderText({ "0.75" })
      
      # Placeholder values for metric changes
      output$N_nes_prop_diff_perc <- renderText({ "+5.0%" })
      output$E0_nes_prop_diff_perc <- renderText({ "+2.5%" })
      output$S1_nes_prop_diff_perc <- renderText({ "-1.0%" })
      output$S2_nes_prop_diff_perc <- renderText({ "-3.0%" })
      output$S3_nes_prop_diff_perc <- renderText({ "-3.5%" })
      output$N_nes_mmean_diff_perc <- renderText({ "+3.5%" })
      output$E0_nes_mmean_diff_perc <- renderText({ "+5.2%" })
      output$macro_clv_diff_perc <- renderText({ "+4.8%" })
      output$macro_cri_diff_perc <- renderText({ "+1.2%" })
      output$macro_nrec_diff_perc <- renderText({ "-2.5%" })
      output$macro_cai_diff_perc <- renderText({ "+3.0%" })
    })
  })
}