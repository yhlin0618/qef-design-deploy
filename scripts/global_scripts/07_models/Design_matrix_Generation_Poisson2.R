library(data.table)
library(tidyverse)

#Data <- data
select <- dplyr::select
Design_matrix_Generation_Poisson <- function(design_list,Data){
  
  data <- Data %>% filter(!!sym(design_list$i_k_structure[2]) %in% design_list$K )
  
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
    #  result_df <- bind_cols(I_only,X.ij_only,result_list_K,result_list_I)
      if(length(design_list$K)<=1){
        result_df <- bind_cols(I_only,X.ij_only,result_list_K,result_list_I)
       
        
        # I_only <- rbind(I_only,rep(0,ncol(I_only)))
        # X.ij_only <- rbind(X.ij_only,rep(1,ncol(X.ij_only)))
        # result_list_K <- rbind(result_list_K,rep(0,ncol(result_list_K)))
        # result_list_I <- rbind(result_list_I,rep(0,ncol(result_list_I)))
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
          result_df <-  rbind(result_df,cons)
          Response <- c(Response,0)
        }
        result_df <- result_df %>% select(-colnames(result_list_K))
        
      }else{
        result_df <- bind_cols(I_only,X.ij_only,result_list_K,result_list_I)
        
        
        # I_only <- rbind(I_only,rep(0,ncol(I_only)))
        # X.ij_only <- rbind(X.ij_only,rep(1,ncol(X.ij_only)))
        # result_list_K <- rbind(result_list_K,rep(0,ncol(result_list_K)))
        # result_list_I <- rbind(result_list_I,rep(0,ncol(result_list_I)))
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
          result_df <-  rbind(result_df,cons)
          Response <- c(Response,0)
        }
      }

      # 
      # mean_constraint <- c(rep(0,ncol(I_only)),
      #                      rep(0,ncol(X.ij_only)),
      #                      rep(1,ncol(result_list_K)),
      #                      rep(0,ncol(result_list_I)))
      
      #   result_df <-  rbind(result_df,(mean_constraint))
    }else{
   #   result_df <- bind_cols(I_only,X.ij_only,result_list_K,result_list_I)
      if(length(design_list$K)<=1){
        result_df <- bind_cols(I_only,X.ij_only,result_list_I)
      }else{
        result_df <- bind_cols(I_only,X.ij_only,result_list_K,result_list_I)
      }
    }
  }
  
  if(!is.null(design_list$weight_name)){
   Weight_dt <- data %>% select(!!sym(design_list$weight_name))

     result_df <- bind_cols(result_df,   rbind(Weight_dt,1)) 
  }
  
  return(list(Response=Response,
              Design_matrix=result_df,
              filter_data=data))
}



# data
# res <- Design_matrix_Generation.lasso(design_list,data)
# res$Design_matrix
# View(res$Design_matrix)

####Example of Design Matrix

Y <- rnorm(10)
X_i1 <- seq(1,10)
X_i2t <- seq(11,20)
set.seed(5566)
W <- rchisq(10,df=1)
#X_i22 <- seq(21,30)
ASIN <- as.character(c(1,1,2,2,3,3,4,4,5,5))
brand <- c(rep("a",4),rep("b",2),rep("c",4))
data <- data.frame(Y,X_i1,X_i2t,ASIN,brand,W)

design_list <- list(Y.it="Y",
                    i_k_structure=c("ASIN","brand"),
                    #K=c("a","b","c"),
                    K=unique(brand),
                    X.ij=c("X_i1"),
                    X.ijt=c("X_i2t"),
                    mean_constraint_on_X.ij=TRUE,
                    weight_name="W"
)
res <- Design_matrix_Generation_Poisson(design_list,data)
res
a <- res$Design_matrix
a
