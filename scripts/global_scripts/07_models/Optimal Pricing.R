Design_matrix_Generation.optima.pricing <- function(design_list,Data,Brand=brand_name){
  
  data <- Data %>% filter(!!sym(design_list$i_k_structure[2]) %in% design_list$K ) %>% 
    filter(!!sym(design_list$i_k_structure[2]) %in% Brand )
  
  Response <- data[[design_list$Y.it]]
  
  ASIN_table <- select(data,all_of(design_list$i_k_structure)) %>% distinct()
  
  
  # I map effect
  I_only <- tibble(I=data[[design_list$i_k_structure[1]]])
  I_only <- I_only %>% mutate(I=as.factor(I)) %>% mutate(data_idx=1:n()) %>% 
    mutate(I_dummy = 1) %>%
    pivot_wider(
      names_from = I,
      values_from = I_dummy,
      values_fill = list(I_dummy = 0)
    ) %>% select(-data_idx )
  # covariates
  columns_to_multiply <- c(design_list$X.ij,design_list$X.ijt)
  
  X_logp <- data %>% select(all_of(c(columns_to_multiply,"logprice","logprice2"))) %>% as.data.table()
  
  
  X_logp[, (paste0(columns_to_multiply, "_logprice")) := lapply(.SD, function(x) x * logprice), .SDcols = columns_to_multiply]
  X_logp[, (paste0(columns_to_multiply, "_logprice2")) := lapply(.SD, function(x) x * logprice2), .SDcols = columns_to_multiply]
  
  
  
  desigm_mat <- bind_cols(X_logp,I_only)
  
  return(list(Response=Response,
              Design_matrix=desigm_mat,
              filter_data=data))
}




Design_matrix_Generation.optima.pricing.heter <- function(design_list,Data){
  
  data <- Data %>% filter(!!sym(design_list$i_k_structure[2]) %in% design_list$K ) %>% as.data.table()
  
  Response <- data[[design_list$Y.it]]
  
  ASIN_table <- select(data,all_of(design_list$i_k_structure)) %>% distinct()
  ASIN_table <- ASIN_table %>% group_by(!!sym(design_list$i_k_structure[2])) %>% 
    mutate(nested_i=n()) %>% ungroup()
  one_one_nested <- ASIN_table %>% filter(nested_i==1)
  many_one_nested <- ASIN_table %>% filter(nested_i>1)
  Overall_i <- data[[design_list$i_k_structure[1]]] %>% unique()
  
  Reduced_K_set <- many_one_nested[[design_list$i_k_structure[2]]] %>% unique()
  
  # 只考量有多餘一個ASIN 的brands
  #  brand_idx <- match(data[[design_list$i_k_structure[2]]],brand_set) 
  K_only <- tibble(K=data[[design_list$i_k_structure[2]]])
  K_only <- K_only %>% mutate(K=as.factor(K)) %>% mutate(data_idx=1:n()) %>% 
    mutate(K_dummy = ifelse(K%in%Reduced_K_set,1,0)) %>%
    pivot_wider(
      names_from = K,
      values_from = K_dummy,
      values_fill = list(K_dummy = 0)
    ) %>% select(-data_idx ) %>% select(all_of(Reduced_K_set))
  
  # I map effect
  I_only <- tibble(I=data[[design_list$i_k_structure[1]]])
  I_only <- I_only %>% mutate(I=as.factor(I)) %>% mutate(data_idx=1:n()) %>% 
    mutate(I_dummy = 1) %>%
    pivot_wider(
      names_from = I,
      values_from = I_dummy,
      values_fill = list(I_dummy = 0)
    ) %>% select(-data_idx )
  # covariates
  # m1
  X.ij_only <- data %>% select(all_of(design_list$X.ij))
  # m2
  X.ijt_only <- data %>% select(all_of(design_list$X.ijt))
  # K_map_matrix
  data_columns <- colnames(X.ij_only)
  # 創建空的列表來存儲結果
  result_list_K <- list()
  # 遍歷每個品牌的dummy變量
  for (K in Reduced_K_set) {
    # 遍歷數據矩陣的每一列
    for (col in data_columns) {
      # 創建新的變量名稱
      new_col_name <- paste(K, col, sep = "_")
      # 將品牌的dummy變量與數據矩陣的列相乘
      result_list_K[[new_col_name]] <- K_only[[K]] * X.ij_only[[col]]
    }
  }
  
  # 將結果列表轉換為數據框
  result_list_K <- as.data.table(result_list_K)
  
  
  
  # I map matrix
  data_columns <- colnames(X.ijt_only)
  I_list <-   colnames(I_only)
  # 創建空的列表來存儲結果
  result_list_I <- list()
  # 遍歷每個品牌的dummy變量
  for (I in I_list) {
    # 遍歷數據矩陣的每一列
    for (col in data_columns) {
      # 創建新的變量名稱
      new_col_name <- paste(I, col, sep = "_")
      # 將品牌的dummy變量與數據矩陣的列相乘
      result_list_I[[new_col_name]] <- I_only[[I]] * X.ijt_only[[col]]
    }
  }
  # 將結果列表轉換為數據框
  result_list_I <- as.data.table(result_list_I)
  
  
  if(is.null(design_list$mean_constraint_on_X.ij)){
    stop("Error: Identifying the existence of the constraints")
  }else{
    if(design_list$mean_constraint_on_X.ij){
      result_df <- bind_cols(I_only,X.ij_only,result_list_K,result_list_I) %>% as.data.table()
      
      columns_to_multiply <- colnames(result_df)
      
      X_logp <- cbind(result_df,data[,.(logprice,logprice2)]) %>% as.data.table()
      
      
      X_logp[, (paste0(columns_to_multiply, "_logprice")) := lapply(.SD, function(x) x * logprice), .SDcols = columns_to_multiply]
      X_logp[, (paste0(columns_to_multiply, "_logprice2")) := lapply(.SD, function(x) x * logprice2), .SDcols = columns_to_multiply]
      result_df <- X_logp
      
      data_columns <- colnames(X.ij_only)
      # 創建空的列表來存儲結果
      # 遍歷每個品牌的dummy變量
      
      for (col in data_columns) {
        cons <- rep(0,ncol(result_df))
        names(cons) <- colnames(result_df)
        for (K in Reduced_K_set) {
          # 遍歷數據矩陣的每一列
          
          # 創建新的變量名稱
          new_col_name <- paste(K, col, sep = "_")
          # 將品牌的dummy變量與數據矩陣的列相乘
          
          cons[[new_col_name]] <- 1
          
          
          
        }
        dt_con <- as.data.table(as.list(cons))
        result_df <-  rbind(result_df,dt_con)
        Response <- c(Response,0)
      }
      
      
    }else{
      result_df <- bind_cols(I_only,X.ij_only,result_list_K,result_list_I)
      columns_to_multiply <- colnames(result_df)
      
      X_logp <- cbind(result_df,data[,.(logprice,logprice2)]) %>% as.data.table()
      
      
      X_logp[, (paste0(columns_to_multiply, "_logprice")) := lapply(.SD, function(x) x * logprice), .SDcols = columns_to_multiply]
      X_logp[, (paste0(columns_to_multiply, "_logprice2")) := lapply(.SD, function(x) x * logprice2), .SDcols = columns_to_multiply]
      result_df <- X_logp
    }
  }
  
  return(list(Response=Response,
              Design_matrix=result_df,
              filter_data=data))
}
