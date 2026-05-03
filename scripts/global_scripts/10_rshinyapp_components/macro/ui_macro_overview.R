#' Macro Overview UI Component
#'
#' This component provides the UI elements for displaying the macro overview dashboard.
#' It shows KPI value boxes for high-level sales metrics.
#'
#' @param id The component ID
#'
#' @return A UI component for the macro overview dashboard
#' 
#' @examples
#' # In the main UI
#' fluidPage(
#'   macroOverviewUI("macro_section")
#' )
#'
#' @export
macroOverviewUI <- function(id) {
  ns <- NS(id)
  
  nav_panel(
    title = "宏觀分析",
    grid_container(
      layout = c(
        "area1"
      ),
      row_sizes = c(
        "700px"
      ),
      col_sizes = c(
        "1fr"
      ),
      gap_size = "10px",
      grid_card(
        area = "area1",
        card_body(
          full_screen = TRUE,
          layout_column_wrap(
            width = "180px",
            fill = FALSE,
            
            # Sales metrics
            createKpiBox(ns, "銷售額", "total_now", "total_diff", "total_diff_perc"),
            createKpiBox(ns, "人均購買金額", "average_now", "average_diff", "average_diff_perc"),
            createKpiBox(ns, "顧客總數", "num_customers_now", "num_customers_diff", "num_customers_diff_perc"),
            createKpiBox(ns, "累積顧客數", "cum_customers_now", "cum_customers_diff", "cum_customers_diff_perc"),
            
            # Customer metrics
            createKpiBox(ns, "顧客留存率", "customer_retention_rate_now_perc", "customer_retention_rate_diff", "customer_retention_rate_diff_value_perc"),
            createKpiBox(ns, "顧客新增率", "customer_acquisition_rate_now_perc", "customer_acquisition_rate_diff", "customer_acquisition_rate_diff_value_perc"),
            createKpiBox(ns, "顧客變動率", "E0_nes_prop_1_now_perc", "E0_nes_prop_1_diff", NULL),
            createKpiBox(ns, "顧客流失率", "S3_nes_prop_1_now_perc", "S3_nes_prop_1_diff", NULL),
            
            # Transition metrics
            createKpiBox(ns, "顧客轉化率", "N_to_E0_prop_perc", "N_to_E0_prop_diff", NULL),
            
            # Customer segment metrics
            createKpiBox(ns, "首購客比例（N）", "N_nes_prop_now_perc", "N_nes_prop_diff", "N_nes_prop_diff_perc"),
            createKpiBox(ns, "主力客（E0）比例", "E0_nes_prop_now_perc", "E0_nes_prop_diff", "E0_nes_prop_diff_perc"),
            createKpiBox(ns, "瞌睡客（S1）比例", "S1_nes_prop_now_perc", "S1_nes_prop_diff", "S1_nes_prop_diff_perc"),
            createKpiBox(ns, "半睡顧客數（S2）", "S2_nes_prop_now_perc", "S2_nes_prop_diff", "S2_nes_prop_diff_perc"),
            createKpiBox(ns, "沉睡顧客數（S3）", "S3_nes_prop_now_perc", "S3_nes_prop_diff", "S3_nes_prop_diff_perc"),
            
            # Value metrics
            createKpiBox(ns, "新客單價", "N_nes_mmean_now", "N_nes_mmean_diff", "N_nes_mmean_diff_perc"),
            createKpiBox(ns, "主力客單價", "E0_nes_mmean_now", "E0_nes_mmean_diff", "E0_nes_mmean_diff_perc"),
            createKpiBox(ns, "顧客終生價值", "macro_clv_now", "macro_clv_diff", "macro_clv_diff_perc"),
            createKpiBox(ns, "交易穩定度", "macro_cri_now", "macro_cri_diff", "macro_cri_diff_perc"),
            createKpiBox(ns, "靜止戶預測", "macro_nrec_now", "macro_nrec_diff", "macro_nrec_diff_perc"),
            createKpiBox(ns, "顧客活躍度", "macro_cai_now", "macro_cai_diff", "macro_cai_diff_perc")
          )
        )
      )
    )
  )
}