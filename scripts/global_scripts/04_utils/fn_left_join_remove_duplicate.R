left_join_remove_duplicate <- function(df1,df2,id="customer_id"){
  df1%>%
    left_join(df2, by=id, suffix=c("",".y"))%>%
    dplyr::select(-ends_with(".y"))
}