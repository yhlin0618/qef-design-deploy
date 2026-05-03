library(EnvStats)
library(nnet)
library(Matrix)
library("caret")
library(readr)
library(maxLik)
library(qgraph)
library(pracma)
library(tidyverse)
library(data.table)
library(fastmatch)
library(progress)
Design_matrix_Generation.choice_model <- function(sub_res,design_list){
  # attr
  # sub_res: data.table with the following columns:
  ## idx, Choice, Subject_ID, product_ID, ChoiceSet, ...
  ## idx: key for each observation
  ## Choice: indicator of the chosen alternative
  ## Subject_ID: id of customer
  ## product_ID: id of the product
  ## ChoiceSet: indicator of the product in choice set
  # design_list: list with the settings for design matrix
  sub_idx_map <- sub_res[,.(Subject_ID,idx )] %>% unique() 
  product_idx_map <-  sub_res[,.(product_ID,idx )] %>% unique()
  
  sub_res_times=sub_idx_map[,.N,by=Subject_ID]
  product_num <- sub_res[,product_ID] %>% unique() %>% length()
  idx_num <-  sub_res[,idx] %>% unique() %>% length()
  dup_sub <- sub_res_times[N>1]

  
  product_intercept <- do.call(rbind, replicate(idx_num,   diag(1,product_num), simplify = FALSE))
  product_intercept <- product_intercept %>% as.data.table()
  colnames(product_intercept) <- paste0("product_",c(1:product_num))
  
  if(  nrow(dup_sub)>0){
    for (i in dup_sub$Subject_ID) {
      if (i==dup_sub$Subject_ID[1]){
        sub_intercept=product_intercept
        
        s_idx <- sub_idx_map[Subject_ID==i,idx]
        row_to_keep=sub_res$idx%in%s_idx
        sub_intercept <- sub_intercept[, lapply(.SD, function(x) ifelse(row_to_keep, x, 0))]
        colnames(sub_intercept)=paste0(colnames(product_intercept),"_sub_",i)
        
      }else{
        s_inter <- product_intercept
        
        s_idx <- sub_idx_map[Subject_ID==i,idx]
        row_to_keep=sub_res$idx%in%s_idx
        s_inter <- s_inter[, lapply(.SD, function(x) ifelse(row_to_keep, x, 0))]
        
        colnames(s_inter)=paste0(colnames(product_intercept),"_sub_",i)
        sub_intercept=cbind(sub_intercept,s_inter)
      }
    }
  }else{
    sub_intercept <- NULL
  }
  

  
  if (is.null(design_list$beta_i.covariate)){
    beta_i.mat <- NULL
  }else{
    for (j in design_list$beta_i.covariate) {
      selected_cov <- sub_res[,j,with = FALSE]
      if(  nrow(dup_sub)>0){
        for (i in dup_sub$Subject_ID) {
          if (i==dup_sub$Subject_ID[1]){
            sub_selected_cov=selected_cov
            
            s_idx <- sub_idx_map[Subject_ID==i,idx]
            row_to_keep=sub_res$idx%in%s_idx
            sub_selected_cov <- sub_selected_cov[, lapply(.SD, function(x) ifelse(row_to_keep, x, 0))]
            colnames(sub_selected_cov)=paste0(colnames(selected_cov),"_sub_",i)
            
          }else{
            s_inter <- selected_cov
            
            s_idx <- sub_idx_map[Subject_ID==i,idx]
            row_to_keep=sub_res$idx%in%s_idx
            s_inter <- s_inter[, lapply(.SD, function(x) ifelse(row_to_keep, x, 0))]
            
            colnames(s_inter)=paste0(colnames(selected_cov),"_sub_",i)
            sub_selected_cov=cbind(sub_selected_cov,s_inter)
          }
        }
        beta_i.mat.small <-  cbind(selected_cov,sub_selected_cov)
      }else{
        beta_i.mat.small <- selected_cov
      }
      if(j== design_list$beta_i.covariate[1]){
        beta_i.mat <- beta_i.mat.small
      }else{
        beta_i.mat <- cbind(beta_i.mat,beta_i.mat.small)
      }
    }
    
    
  }
  
  
  
  if (is.null(design_list$beta_j.covariate)){
    beta_j.mat <- NULL
  }else{
    for (j in design_list$beta_j.covariate) {
     # j=design_list$beta_j.covariate
      selected_cov <- sub_res[,j,with = FALSE]
      for (i in unique(product_idx_map$product_ID)) {
        if(i==unique(product_idx_map$product_ID)[1]){
        
          product_selected_cov <- selected_cov
          
          row_to_keep=sub_res$product_ID==i
          product_selected_cov <- product_selected_cov[, lapply(.SD, function(x) ifelse(row_to_keep, x, 0))]
          colnames(product_selected_cov)=paste0(colnames(selected_cov),"_product_",i)
        }else{
          s_inter <- selected_cov
          
          row_to_keep=sub_res$product_ID==i
          s_inter <- s_inter[, lapply(.SD, function(x) ifelse(row_to_keep, x, 0))]
          colnames(s_inter)=paste0(colnames(selected_cov),"_product_",i)
          product_selected_cov=cbind(product_selected_cov,s_inter)
          
        }
      }
      beta_j.mat.small <-  cbind(product_selected_cov)
      if(j== design_list$beta_j.covariate[1]){
        beta_j.mat <- beta_j.mat.small
      }else{
        beta_j.mat <- cbind(beta_j.mat,beta_j.mat.small)
      }
    }

  }
  
  # 使用 do.call 和 cbind 绑定列
  result <- do.call(cbind, Filter(Negate(is.null), list(product_intercept,sub_intercept,beta_i.mat,beta_j.mat)))
  
  columns_to_remove <- grep(paste0("product_",design_list$baseline_product_id,"|sub_",design_list$baseline_id), colnames(result))
  #colnames(result)
  # 删除这些列
  constraint_result <- result[, -columns_to_remove, with = FALSE]
  
  remain_idx_cols <- sub_res[,.( idx,Choice,Subject_ID,product_ID ,ChoiceSet)]
  data <- cbind(remain_idx_cols,constraint_result)
  
  sparse_data <- Matrix(as.matrix(data), sparse = TRUE)
  colnames(sparse_data) <- colnames(data)
  return(sparse_data)
}








Design_matrix_Generation.choice_model2 <- function(sub_res,design_list){
  # attr
  # sub_res: data.table with the following columns:
  ## idx, Choice, Subject_ID, product_ID, ChoiceSet, ...
  ## idx: key for each observation
  ## Choice: indicator of the chosen alternative
  ## Subject_ID: id of customer
  ## product_ID: id of the product
  ## ChoiceSet: indicator of the product in choice set
  # design_list: list with the settings for design matrix
  sub_idx_map <- sub_res[,.(Subject_ID,idx )] %>% unique() 
  product_idx_map <-  sub_res[,.(product_ID,idx )] %>% unique()
  
  sub_res_times=sub_idx_map[,.N,by=Subject_ID]
  product_num <- sub_res[,product_ID] %>% unique() %>% length()
  idx_num <-  sub_res[,idx] %>% unique() %>% length()
  dup_sub <- sub_res_times[N>1]
  
  print("start create product intercept")
  S <- Sys.time()
  product_intercept <- do.call(rbind, replicate(idx_num,   diag(1,product_num), simplify = FALSE))
  product_intercept <- product_intercept %>% as.data.table()
  
  colnames(product_intercept) <- paste0("product_",c(1:product_num))
  print("product intercept finished")
      print(round(Sys.time()-S,digit=2))
  
  print("start create subject intercept")
  S <- Sys.time()

  if(  nrow(dup_sub)>0){
    
    pb <- progress_bar$new(
      format = "  [:bar] :percent :elapsed",
      total = length(dup_sub$Subject_ID), clear = FALSE, width = 60
    )
    for (i in dup_sub$Subject_ID) {
      if (i==dup_sub$Subject_ID[1]){
        sub_intercept = copy(product_intercept)
        
        s_idx <- sub_idx_map[Subject_ID==i,idx]
        
        colnames(sub_intercept)=paste0(colnames(product_intercept),"_sub_",i)
        columns_to_update=colnames(sub_intercept)
        sub_intercept[!sub_res$idx %in% s_idx, (columns_to_update) := 0]
        pb$tick()
       # sub_intercept <- sub_intercept[, lapply(.SD, function(x) ifelse(row_to_keep, x, 0))]
      #  colnames(sub_intercept)=paste0(colnames(product_intercept),"_sub_",i)
        
      }else{
        s_inter <-  copy(product_intercept)
        
        s_idx <- sub_idx_map[Subject_ID==i,idx]
        #row_to_keep=sub_res$idx%in%s_idx
        # s_inter[sub_res$idx %in% s_idx, (paste0(colnames(product_intercept), "_sub_", i)) := lapply(.SD, function(x) x)]
        # s_inter[!sub_res$idx %in% s_idx, (paste0(colnames(product_intercept), "_sub_", i)) := 0]
        
        colnames(s_inter)=paste0(colnames(product_intercept),"_sub_",i)
      #  s_inter[!sub_res$idx %in% s_idx, colnames(s_inter) := 0, .SDcols = colnames(s_inter)]
        
        
        columns_to_update=colnames(s_inter)
        s_inter[!sub_res$idx %in% s_idx, (columns_to_update) := 0]
        
        
      #  s_inter[row_to_keep, (paste0(colnames(product_intercept), "_sub_", i)) := lapply(.SD, function(x) ifelse(.I %in% s_idx, x, 0))]
        
        # s_inter <- s_inter[, lapply(.SD, function(x) ifelse(row_to_keep, x, 0))]
        # 
        # colnames(s_inter)=paste0(colnames(product_intercept),"_sub_",i)
        sub_intercept=cbind(sub_intercept,s_inter)
        pb$tick()
      }
    }
  }else{
    sub_intercept <- NULL
  }
  print("subject intercept finished")
      print(round(Sys.time()-S,digit=2))
  
  print("start create beta_i.covariate")
  S <- Sys.time()
  
  if (is.null(design_list$beta_i.covariate)){
    beta_i.mat <- NULL
  }else{
    pb <- progress_bar$new(
      format = "  [:bar] :percent :elapsed",
      total = length(dup_sub$Subject_ID)*length(design_list$beta_i.covariate), clear = FALSE, width = 60
    )
    
    for (j in design_list$beta_i.covariate) {
      selected_cov <- sub_res[,j,with = FALSE]
      if(  nrow(dup_sub)>0){
        for (i in dup_sub$Subject_ID) {
          if (i==dup_sub$Subject_ID[1]){
            sub_selected_cov= copy(selected_cov)
            
            s_idx <- sub_idx_map[Subject_ID==i,idx]
            
            # sub_selected_cov[sub_res$idx %in% s_idx, (paste0(colnames(selected_cov), "_sub_", i)) := lapply(.SD, function(x) x)]
            # sub_selected_cov[!sub_res$idx %in% s_idx, (paste0(colnames(selected_cov), "_sub_", i)) := 0]
            
            colnames(sub_selected_cov)=paste0(colnames(selected_cov),"_sub_",i)
            
            
            columns_to_update=colnames(sub_selected_cov)
            sub_selected_cov[!sub_res$idx %in% s_idx, (columns_to_update) := 0]
            
           # sub_selected_cov[!sub_res$idx %in% s_idx, colnames(sub_selected_cov) := 0, .SDcols = colnames(sub_selected_cov)]
            
            
         #   row_to_keep=sub_res$idx%in%s_idx
            
          #  sub_selected_cov <- sub_selected_cov[, lapply(.SD, function(x) ifelse(row_to_keep, x, 0))]
          #  colnames(sub_selected_cov)=paste0(colnames(selected_cov),"_sub_",i)
            pb$tick()
          }else{
            s_inter <- copy(selected_cov)
            
            s_idx <- sub_idx_map[Subject_ID==i,idx]
            #row_to_keep=sub_res$idx%in%s_idx
            # 
            # s_inter[sub_res$idx %in% s_idx, (paste0(colnames(selected_cov), "_sub_", i)) := lapply(.SD, function(x) x)]
            # s_inter[!sub_res$idx %in% s_idx, (paste0(colnames(selected_cov), "_sub_", i)) := 0]
            
            colnames(s_inter)=paste0(colnames(selected_cov),"_sub_",i)
            
            
            columns_to_update=colnames(s_inter)
            s_inter[!sub_res$idx %in% s_idx, (columns_to_update) := 0]
            
            
            
           #$ s_inter[!sub_res$idx %in% s_idx, colnames(s_inter) := 0, .SDcols = colnames(s_inter)]
            
            # s_inter <- s_inter[, lapply(.SD, function(x) ifelse(row_to_keep, x, 0))]
            # 
            # colnames(s_inter)=paste0(colnames(selected_cov),"_sub_",i)
            sub_selected_cov=cbind(sub_selected_cov,s_inter)
            pb$tick()
          }
        }
        beta_i.mat.small <-  cbind(selected_cov,sub_selected_cov)
      }else{
        beta_i.mat.small <- selected_cov
      }
      if(j== design_list$beta_i.covariate[1]){
        beta_i.mat <- beta_i.mat.small
      }else{
        beta_i.mat <- cbind(beta_i.mat,beta_i.mat.small)
      }
    }
    
    
  }
  
  print("beta_i.covariate finished")
      print(round(Sys.time()-S,digit=2))
  
  
  print("start create beta_j.covariate")
  S <- Sys.time()
  
  
  if (is.null(design_list$beta_j.covariate)){
    beta_j.mat <- NULL
  }else{
    pb <- progress_bar$new(
      format = "  [:bar] :percent :elapsed",
      total = length(unique(product_idx_map$product_ID))*length(design_list$beta_j.covariate), clear = FALSE, width = 60
    )
    for (j in design_list$beta_j.covariate) {
      # j=design_list$beta_j.covariate
      selected_cov <- sub_res[,j,with = FALSE]
      for (i in unique(product_idx_map$product_ID)) {
        if(i==unique(product_idx_map$product_ID)[1]){
          
          product_selected_cov <- copy(selected_cov)
          
          # row_to_keep=sub_res$product_ID==i
          # product_selected_cov <- product_selected_cov[, lapply(.SD, function(x) ifelse(row_to_keep, x, 0))]
          # colnames(product_selected_cov)=paste0(colnames(selected_cov),"_product_",i)
          
          colnames(product_selected_cov)=paste0(colnames(selected_cov),"_product_",i)
          
          
          columns_to_update=colnames(product_selected_cov)
          product_selected_cov[!sub_res$product_ID==i, (columns_to_update) := 0]
          
      #    product_selected_cov[!sub_res$product_ID==i, (colnames(product_selected_cov) ):= 0, .SDcols = colnames(product_selected_cov)]
          
        #  product_selected_cov[sub_res$product_ID==i, (paste0(colnames(selected_cov), "_product_", i)) := lapply(.SD, function(x) x)]
          #product_selected_cov[!sub_res$product_ID==i, (paste0(colnames(selected_cov), "_product_", i)) := 0]
          
          pb$tick()
        }else{
          s_inter <- copy(selected_cov)
          # 
          # row_to_keep=sub_res$product_ID==i
          # s_inter <- s_inter[, lapply(.SD, function(x) ifelse(row_to_keep, x, 0))]
          # colnames(s_inter)=paste0(colnames(selected_cov),"_product_",i)
          # 
          # s_inter[sub_res$product_ID==i, (paste0(colnames(selected_cov), "_product_", i)) := lapply(.SD, function(x) x)]
          # s_inter[!sub_res$product_ID==i, (paste0(colnames(selected_cov), "_product_", i)) := 0]
          
          colnames(s_inter)=paste0(colnames(selected_cov),"_product_",i)
          
          columns_to_update=colnames(s_inter)
          s_inter[!sub_res$product_ID==i, (columns_to_update) := 0]
          
        #  s_inter[!sub_res$product_ID==i,( colnames(s_inter)) := 0, .SDcols = colnames(s_inter)]
          
          product_selected_cov=cbind(product_selected_cov,s_inter)
            pb$tick()
        }
      }
      beta_j.mat.small <-  cbind(product_selected_cov)
      if(j== design_list$beta_j.covariate[1]){
        beta_j.mat <- beta_j.mat.small
      }else{
        beta_j.mat <- cbind(beta_j.mat,beta_j.mat.small)
      }
    }
    
  }
  print("beta_j.covariate finished")
      print(round(Sys.time()-S,digit=2))
  # # 使用 do.call 和 cbind 绑定列
  # result <- do.call(cbind, Filter(Negate(is.null), list(product_intercept,sub_intercept,beta_i.mat,beta_j.mat)))
  # 
  # columns_to_remove <- grep(paste0("product_",design_list$baseline_product_id,"|sub_",design_list$baseline_id), colnames(result))
  # #colnames(result)
  # # 删除这些列
  # constraint_result <- result[, -columns_to_remove, with = FALSE]
  # 
  # remain_idx_cols <- sub_res[,.( idx,Choice,Subject_ID,product_ID ,ChoiceSet)]
  # data <- cbind(remain_idx_cols,constraint_result)
  # 
  # sparse_data <- Matrix(as.matrix(data), sparse = TRUE)
  # 
  # 
  
  # 假设 `result` 已经是之前操作得到的 data.table
  result <- do.call(cbind, Filter(Negate(is.null), list(product_intercept, sub_intercept, beta_i.mat, beta_j.mat)))
  
  # 使用 grep 找出需要删除的列
  columns_to_remove <- grep(paste0("product_", design_list$baseline_product_id, "|sub_", design_list$baseline_id), colnames(result))
  
  # 使用 data.table 效率地删除列
  constraint_result <- result[, -columns_to_remove, with = FALSE]
  
  # 将剩余的索引列与约束结果合并
  data <- cbind(sub_res[, .(idx, Choice, Subject_ID, product_ID, ChoiceSet)], constraint_result)
  a <- colnames(data)
  # 转换为稀疏矩阵
  # sparse_data <- as(as.matrix(data), "CsparseMatrix")
  # colnames(sparse_data) <-a
  
  # 创建一个空的稀疏矩阵
  sparse_data <- Matrix(0, nrow = nrow(data), ncol = ncol(data), sparse = TRUE)
  
  print("start transform to sparse matrix")
  S <- Sys.time()
  pb <- progress_bar$new(
    format = "  [:bar] :percent :elapsed",
    total = ncol(data), clear = FALSE, width = 60
  )
  # 逐列转换并赋值给稀疏矩阵
  for (col in seq_len(ncol(data))) {
    sparse_data[, col] <- as(data[[col]], "sparseVector")
    pb$tick()
  }
  print("transformation finished")
  print(round(Sys.time()-S,digit=2))
  colnames(sparse_data) <-a
  
  return(sparse_data)
}












Design_matrix_Generation.choice_model3 <- function(sub_res,design_list){
  # attr
  # sub_res: data.table with the following columns:
  ## idx, Choice, Subject_ID, product_ID, ChoiceSet, ...
  ## idx: key for each observation
  ## Choice: indicator of the chosen alternative
  ## Subject_ID: id of customer
  ## product_ID: id of the product
  ## ChoiceSet: indicator of the product in choice set
  # design_list: list with the settings for design matrix
  sub_idx_map <- sub_res[,.(Subject_ID,idx )] %>% unique() 
  product_idx_map <-  sub_res[,.(product_ID,idx )] %>% unique()
  
  sub_res_times=sub_idx_map[,.N,by=Subject_ID]
  product_num <- sub_res[,product_ID] %>% unique() %>% length()
  idx_num <-  sub_res[,idx] %>% unique() %>% length()
  dup_sub <- sub_res_times[N>1]


  
  print("start create product intercept")
  S <- Sys.time()

  
  product_intercept <- do.call(rbind, replicate(idx_num,   diag(1,product_num), simplify = FALSE))
  product_intercept <- product_intercept %>% as.data.table()
  
  colnames(product_intercept) <- paste0("product_",c(1:product_num))


  
  print("product intercept finished")
  print(round(Sys.time()-S,digit=2))
  
  print("start create subject intercept")
  S <- Sys.time()
  
  if(  nrow(dup_sub)>0){
    
    pb <- progress_bar$new(
      format = "  [:bar] :percent :elapsed",
      total = length(dup_sub$Subject_ID), clear = FALSE, width = 60
    )
    for (i in dup_sub$Subject_ID) {
      if (i==dup_sub$Subject_ID[1]){
        sub_intercept = copy(product_intercept)
        
        s_idx <- sub_idx_map[Subject_ID==i,idx]
        
        colnames(sub_intercept)=paste0(colnames(product_intercept),"_sub_",i)
        columns_to_update=colnames(sub_intercept)
        sub_intercept[!sub_res$idx %in% s_idx, (columns_to_update) := 0]
        sub_intercept <- Matrix(as.matrix(sub_intercept), sparse = TRUE)
        colnames(sub_intercept) <- columns_to_update
        pb$tick()
        # sub_intercept <- sub_intercept[, lapply(.SD, function(x) ifelse(row_to_keep, x, 0))]
        #  colnames(sub_intercept)=paste0(colnames(product_intercept),"_sub_",i)
        
      }else{
        s_inter <-  copy(product_intercept)
        
        s_idx <- sub_idx_map[Subject_ID==i,idx]
        #row_to_keep=sub_res$idx%in%s_idx
        # s_inter[sub_res$idx %in% s_idx, (paste0(colnames(product_intercept), "_sub_", i)) := lapply(.SD, function(x) x)]
        # s_inter[!sub_res$idx %in% s_idx, (paste0(colnames(product_intercept), "_sub_", i)) := 0]
        
        colnames(s_inter)=paste0(colnames(product_intercept),"_sub_",i)
        #  s_inter[!sub_res$idx %in% s_idx, colnames(s_inter) := 0, .SDcols = colnames(s_inter)]
        
        
        columns_to_update=colnames(s_inter)
        s_inter[!sub_res$idx %in% s_idx, (columns_to_update) := 0]
 
        
        #  s_inter[row_to_keep, (paste0(colnames(product_intercept), "_sub_", i)) := lapply(.SD, function(x) ifelse(.I %in% s_idx, x, 0))]
        
        # s_inter <- s_inter[, lapply(.SD, function(x) ifelse(row_to_keep, x, 0))]
        # 
        # colnames(s_inter)=paste0(colnames(product_intercept),"_sub_",i)
        s_inter <- Matrix(as.matrix(s_inter), sparse = TRUE)
        colnames(s_inter) <- columns_to_update
        sub_intercept=cbind(sub_intercept,s_inter)
        pb$tick()
      }
    }
  }else{
    sub_intercept <- NULL
  }
  print("subject intercept finished")
  print(round(Sys.time()-S,digit=2))
  
  print("start create beta_i.covariate")
  S <- Sys.time()
  
  if (is.null(design_list$beta_i.covariate)){
    beta_i.mat <- NULL
  }else{
    pb <- progress_bar$new(
      format = "  [:bar] :percent :elapsed",
      total = length(dup_sub$Subject_ID)*length(design_list$beta_i.covariate), clear = FALSE, width = 60
    )
    
    for (j in design_list$beta_i.covariate) {
      selected_cov <- sub_res[,j,with = FALSE]
      if(  nrow(dup_sub)>0){
        for (i in dup_sub$Subject_ID) {
          if (i==dup_sub$Subject_ID[1]){
            sub_selected_cov= copy(selected_cov)
            sub_selected_cov <- sub_selected_cov+0
            s_idx <- sub_idx_map[Subject_ID==i,idx]
            
            # sub_selected_cov[sub_res$idx %in% s_idx, (paste0(colnames(selected_cov), "_sub_", i)) := lapply(.SD, function(x) x)]
            # sub_selected_cov[!sub_res$idx %in% s_idx, (paste0(colnames(selected_cov), "_sub_", i)) := 0]
            
            colnames(sub_selected_cov)=paste0(colnames(selected_cov),"_sub_",i)
            
            
            columns_to_update=colnames(sub_selected_cov)
            sub_selected_cov[!sub_res$idx %in% s_idx, (columns_to_update) := 0]
            
            sub_selected_cov <- Matrix(as.matrix(sub_selected_cov), sparse = TRUE)
            colnames(sub_selected_cov) <- columns_to_update
    
            
            
            
            # sub_selected_cov[!sub_res$idx %in% s_idx, colnames(sub_selected_cov) := 0, .SDcols = colnames(sub_selected_cov)]
            
            
            #   row_to_keep=sub_res$idx%in%s_idx
            
            #  sub_selected_cov <- sub_selected_cov[, lapply(.SD, function(x) ifelse(row_to_keep, x, 0))]
            #  colnames(sub_selected_cov)=paste0(colnames(selected_cov),"_sub_",i)
            pb$tick()
          }else{
            s_inter <- copy(selected_cov)
            
            s_idx <- sub_idx_map[Subject_ID==i,idx]
            #row_to_keep=sub_res$idx%in%s_idx
            # 
            # s_inter[sub_res$idx %in% s_idx, (paste0(colnames(selected_cov), "_sub_", i)) := lapply(.SD, function(x) x)]
            # s_inter[!sub_res$idx %in% s_idx, (paste0(colnames(selected_cov), "_sub_", i)) := 0]
            
            colnames(s_inter)=paste0(colnames(selected_cov),"_sub_",i)
            
            
            columns_to_update=colnames(s_inter)
            s_inter[!sub_res$idx %in% s_idx, (columns_to_update) := 0]
            
            s_inter <- Matrix(as.matrix(s_inter), sparse = TRUE)
            colnames(s_inter) <- columns_to_update
            
            #$ s_inter[!sub_res$idx %in% s_idx, colnames(s_inter) := 0, .SDcols = colnames(s_inter)]
            
            # s_inter <- s_inter[, lapply(.SD, function(x) ifelse(row_to_keep, x, 0))]
            # 
            # colnames(s_inter)=paste0(colnames(selected_cov),"_sub_",i)
            sub_selected_cov=cbind(sub_selected_cov,s_inter)
            pb$tick()
          }
        }
        columns_to_update <- colnames(selected_cov)
        selected_cov <- Matrix(as.matrix(selected_cov), sparse = TRUE)
        colnames(selected_cov) <- columns_to_update
        beta_i.mat.small <-  cbind(selected_cov,sub_selected_cov)
      }else{
        columns_to_update <- colnames(selected_cov)
        selected_cov <- Matrix(as.matrix(selected_cov), sparse = TRUE)
        colnames(selected_cov) <- columns_to_update
        beta_i.mat.small <- selected_cov
      }
      if(j== design_list$beta_i.covariate[1]){
        beta_i.mat <- beta_i.mat.small
      }else{
        beta_i.mat <- cbind(beta_i.mat,beta_i.mat.small)
      }
    }
    
    
  }
  
  print("beta_i.covariate finished")
  print(round(Sys.time()-S,digit=2))
  
  
  print("start create beta_j.covariate")
  S <- Sys.time()
  
  
  if (is.null(design_list$beta_j.covariate)){
    beta_j.mat <- NULL
  }else{
    pb <- progress_bar$new(
      format = "  [:bar] :percent :elapsed",
      total = length(unique(product_idx_map$product_ID))*length(design_list$beta_j.covariate), clear = FALSE, width = 60
    )
    for (j in design_list$beta_j.covariate) {
      # j=design_list$beta_j.covariate
      selected_cov <- sub_res[,j,with = FALSE]
      for (i in unique(product_idx_map$product_ID)) {
        if(i==unique(product_idx_map$product_ID)[1]){
          
          product_selected_cov <- copy(selected_cov)
          
          colnames(product_selected_cov)=paste0(colnames(selected_cov),"_product_",i)
          
          columns_to_update=colnames(product_selected_cov)
          product_selected_cov[!sub_res$product_ID==i, (columns_to_update) := 0]
          
          product_selected_cov <- Matrix(as.matrix(product_selected_cov), sparse = TRUE)
          colnames(product_selected_cov) <- columns_to_update
          
          pb$tick()
        }else{
          s_inter <- copy(selected_cov)
          
          colnames(s_inter)=paste0(colnames(selected_cov),"_product_",i)
          
          columns_to_update=colnames(s_inter)
          s_inter[!sub_res$product_ID==i, (columns_to_update) := 0]
          s_inter <- Matrix(as.matrix(s_inter), sparse = TRUE)
          colnames(s_inter) <- columns_to_update
          
          product_selected_cov=cbind(product_selected_cov,s_inter)
          pb$tick()
        }
      }
      beta_j.mat.small <-  cbind(product_selected_cov)
      if(j== design_list$beta_j.covariate[1]){
        beta_j.mat <- beta_j.mat.small
      }else{
        beta_j.mat <- cbind(beta_j.mat,beta_j.mat.small)
      }
    }
    
  }
  print("beta_j.covariate finished")
  print(round(Sys.time()-S,digit=2))
  
  columns_to_update <- colnames(product_intercept)
  product_intercept <- Matrix(as.matrix(product_intercept), sparse = TRUE)
  colnames(product_intercept) <- columns_to_update
  # 假设 `result` 已经是之前操作得到的 data.table
  
  
  
  print("start transform to sparse matrix")
  S <- Sys.time()
  
  
  result <- do.call(cbind, Filter(Negate(is.null), list(product_intercept, sub_intercept, beta_i.mat, beta_j.mat)))
  
  
  # 使用 grep 找出需要删除的列
  columns_to_remove <- grep(paste0("product_", design_list$baseline_product_id, "|sub_", design_list$baseline_id), colnames(result))
  
  # 检查列名是否存在
  if (length(columns_to_remove) > 0) {
    # 移除指定列
    constraint_result <- result[, -columns_to_remove]
  }
  sub_sparse <- sub_res[, .(idx, Choice, Subject_ID, product_ID, ChoiceSet)]
  columns_to_update <-  colnames(sub_sparse)
  sub_sparse <- as.matrix(sub_sparse)
  sub_sparse <-  Matrix(sub_sparse, sparse = TRUE) 
  colnames(sub_sparse) <- columns_to_update
  # 将剩余的索引列与约束结果合并
  data <- cbind(sub_sparse, constraint_result)




  print("transformation finished")
  print(round(Sys.time()-S,digit=2))

  
  return(data)
}




# To use the likelihood and the SGD function, 
# you need to confirm that the data is correctly constructed.
# That is, your data should be numeric spars matrix after regularization. 
# The recommended regularization is min-max normalization, 
# while other methods are acceptable.
#Also, you have to check the required column is correct.
## idx: key for each observation
## Choice: indicator of the chosen alternative
## Subject_ID: id of customer
## product_ID: id of the product
## ChoiceSet: indicator of the product in choice set
# If any of the column has semantic error, the result of the optimization should be doubtful.






# design

choice_llik <- function(data,parameter,lambda=0,alpha=0){
  idx_loc <- which(colnames(data)== "idx")
  sid_loc <- which(colnames(data)== "Subject_ID")
  c_loc <- which(colnames(data)== "Choice")
  product_loc <- which(colnames(data)== "product_ID")
  c_set_loc <- which(colnames(data)== "ChoiceSet")
  
  # 1. obs 2. choice  3. Subject_ID 4. product_ID 5. ... 
  
  product_length=length(unique(data[,product_loc]))
  
  product_mat <-data[,-c(idx_loc,sid_loc,c_loc,product_loc,c_set_loc)]
  #product_mat <- product_mat[c(1:product_length),]
  
  obs_num <- length(unique(data[,idx_loc]))
  deno <- product_mat%*%parameter
  c_set <- data[,c_set_loc]
  
  #e_deno <- log(sum(exp(deno)))
  # 如果要有product indicator 加在這邊
  e_deno <- tapply(exp(deno)*c_set, (seq_along(deno) - 1) %/% product_length, sum)
  e_deno <- mean(log(e_deno))
  
  # ch_mat <- data[,-c(1,2,3)]
  ch <- data[,c_loc]
  num <- t(ch) %*%product_mat%*% parameter
  #ori_form <- num/obs_num-e_deno
  
  
  ori_form <- num/obs_num-e_deno
  
  
  #ori_form <- mean(deno[ch==1]-log(e_deno))
  
  
  penal_lik <- ori_form-lambda*(t(parameter)%*%parameter )
  return(as.numeric(penal_lik))
}

choice_score <- function(data,parameter,lambda=0,alpha=0){
  # data=data_mat
  # parameter=par
  idx_loc <- which(colnames(data)== "idx")
  sid_loc <- which(colnames(data)== "Subject_ID")
  c_loc <- which(colnames(data)== "Choice")
  product_loc <- which(colnames(data)== "product_ID")
  c_set_loc <- which(colnames(data)== "ChoiceSet")
  c_set <- data[,c_set_loc]
  
  product_length=length(unique(data[,product_loc]))
  product_mat <-data[,-c(idx_loc,sid_loc,c_loc,product_loc,c_set_loc)]
  
  # product_mat <-data[,-c(1,2,3)]
  #  product_mat <- product_mat[c(1:product_length),]
  obs_num <- length(unique(data[,idx_loc]))
  deno <- product_mat%*%parameter
  
  #  e_deno <- log(sum(exp(deno)))
  # e_deno <- mean(log(e_deno))
  e_deno <- tapply(exp(deno)*c_set, (seq_along(deno) - 1) %/% product_length, sum) # new ver
  
  
  #ch_mat <- data[,-c(1,2,3)]
  ch <- data[,c_loc]
  num <- t(ch) %*%product_mat
  
  e_deno2 <- log(rep(e_deno,each=product_length)) # new ver
  # length(e_deno2) 
  ori_form <- (t(num)-t(t(exp(deno-e_deno2)*c_set)%*%product_mat))/obs_num # new ver
  
  penal_lik <- ori_form-2*lambda*(parameter )
  return( (penal_lik))
}


# par <- rnorm(11)
# choice_llik(data_mat,par)
# num_score <- pracma::grad(function(x){
#   choice_llik(data_mat,x,lambda=1)
# },x0=par)
# 
# ana_score <- choice_score(data_mat,par,lambda = 1)
# num_score-ana_score
# 
# colnames(data_mat)


predict_acc <- function(data,parameter,lambda=0,alpha=0){
  idx_loc <- which(colnames(data)== "idx")
  sid_loc <- which(colnames(data)== "Subject_ID")
  c_loc <- which(colnames(data)== "Choice")
  product_loc <- which(colnames(data)== "product_ID")
  c_set_loc <- which(colnames(data)== "ChoiceSet")
  
  # 1. obs 2. choice  3. Subject_ID 4. product_ID 5. ... 
  
  product_length=length(unique(data[,product_loc]))
  
  product_mat <-data[,-c(idx_loc,sid_loc,c_loc,product_loc,c_set_loc)]
  #product_mat <- product_mat[c(1:product_length),]
  
  obs_num <- length(unique(data[,idx_loc]))
  deno <- product_mat%*%parameter
  c_set <- data[,c_set_loc]
  
  #e_deno <- log(sum(exp(deno)))
  # 如果要有product indicator 加在這邊
  # e_deno <- tapply(exp(deno)*c_set, (seq_along(deno) - 1) %/% product_length, sum)
  # e_deno <- mean(log(e_deno))
  
  # ch_mat <- data[,-c(1,2,3)]
  ch <- data[,c_loc]

  pred <- tapply( deno, (seq_along(deno) - 1) %/% product_length, which.is.max)
  act <- tapply(ch, (seq_along(ch) - 1) %/% product_length, which.is.max)


  return(  mean(pred==act))
}



predict_acc2 <- function(data,parameter,lambda=0,alpha=0,range=1){
  idx_loc <- which(colnames(data)== "idx")
  sid_loc <- which(colnames(data)== "Subject_ID")
  c_loc <- which(colnames(data)== "Choice")
  product_loc <- which(colnames(data)== "product_ID")
  c_set_loc <- which(colnames(data)== "ChoiceSet")
  
  # 1. obs 2. choice  3. Subject_ID 4. product_ID 5. ... 
  
  product_length=length(unique(data[,product_loc]))
  
  product_mat <-data[,-c(idx_loc,sid_loc,c_loc,product_loc,c_set_loc)]
  #product_mat <- product_mat[c(1:product_length),]
  
  obs_num <- length(unique(data[,idx_loc]))
  deno <- product_mat%*%parameter
  c_set <- data[,c_set_loc]
  
  #e_deno <- log(sum(exp(deno)))
  # 如果要有product indicator 加在這邊
  # e_deno <- tapply(exp(deno)*c_set, (seq_along(deno) - 1) %/% product_length, sum)
  # e_deno <- mean(log(e_deno))
  
  # ch_mat <- data[,-c(1,2,3)]
  ch <- data[,c_loc]
  
  
  get_top_k_positions <- function(vec, k) {
    # 获取排序后的索引，降序排列
    sorted_indices <- order(vec, decreasing = TRUE)
    # 返回前k大的元素的原始位置（索引）
    top_k_indices <- sorted_indices[1:k]
    return(top_k_indices)
  }
  result <- tapply(deno, (seq_along(deno) - 1) %/% product_length, get_top_k_positions, k = range)
  
 # pred <- tapply( deno, (seq_along(deno) - 1) %/% product_length, which.is.max)
  act <- tapply(ch, (seq_along(ch) - 1) %/% product_length, which.is.max)
  is_in_top_k_result <- sapply(seq_along(result), function(i) {
    group_pos <- act[i]
    if (i <= length(act) && !is.null(result[[i]])) {
      return(group_pos %in% result[[i]])
    } else {
      return(FALSE)
    }
  })

  return(    mean(is_in_top_k_result))
}


base_line_get <- function(data){
  product_loc <- which(colnames(data)== "product_ID")
  product_length <- length(unique(data[,product_loc]))
  act <- tapply(data[,which(colnames(data)=="Choice")], (seq_along(data[,which(colnames(data)=="Choice")]) - 1) %/% product_length, which.is.max)
  act_tab <- table(act)
  sorted_indices <- order(act_tab, decreasing = TRUE)
  ordering_prop <- act_tab[sorted_indices]/sum(act_tab)
  base_line_acc <- cumsum(ordering_prop)
  return(base_line_acc)
}


# 
# 
# sub_res <- data.table(idx=rep(1:9,each=3),
#                       Choice=c(1,0,0,
#                                0,1,0,
#                                1,0,0,
#                                1,0,0,
#                                rep(c(1,0,0),5)),
#                       Subject_ID=c(rep(1,6),
#                                    rep(2,3),
#                                    rep(3,9),
#                                    rep(4,3),
#                                    rep(5,6)),
#                       product_ID =rep(1:3,9),
#                       ChoiceSet=rep(1,27),
#                       x.1j=c(rep(4:6,9)),
#                       x.2i=c(rep(0,6),
#                              rep(1,3),
#                              rep(2,9),
#                              rep(3,3),
#                              rep(2,6))
#                       
# )
# 
# design_list <- list(baseline_id=5,
#                     baseline_product_id=3,
#                     beta_i.covariate=c("x.1j"),
#                     beta_j.covariate=c("x.2i")
# )
# 
# data_mat <- Design_matrix_Generation.choice_model(sub_res = sub_res,design_list = design_list)
# 
# data_mat2 <- Design_matrix_Generation.choice_model2(sub_res = sub_res,design_list = design_list)
# library(pryr)
# mem_change(data_mat <- Design_matrix_Generation.choice_model(sub_res = sub_res,design_list = design_list)
# )
# mem_change(data_mat2 <- Design_matrix_Generation.choice_model2(sub_res = sub_res,design_list = design_list)
# )
