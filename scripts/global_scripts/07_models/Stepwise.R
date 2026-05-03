library(nnet)

stepwise_selection <- function(data, response_var,weight=NULL,MAX_IT=500) {
  # 初始化变量
  if(is.null(weight)){
    forward_vars <- setdiff(names(data), c(response_var))
    forward_vars <- sapply(forward_vars, function(x) if(grepl("[(]|[)]", x)) paste0("`", x, "`") else x)
    forward_vars <- sapply(forward_vars, function(x) if(grepl("-", x)) paste0("`", x, "`") else x)
    
    model_formula <- as.formula(paste(response_var, "~ 1"))
    current_bic <- BIC(multinom(model_formula, data = data, trace = FALSE,maxit=MAX_IT))
    improved <- TRUE
    
    while(improved) {
      improved <- FALSE
      
      # 向前选择
      for (var in forward_vars) {
        temp_formula <- update(model_formula, paste(". ~ . +", var))
        temp_model <- tryCatch({ multinom(temp_formula, data = data, trace = FALSE,maxit=MAX_IT) }, error = function(e) NULL)
        if (!is.null(temp_model)) {
          temp_bic <- BIC(temp_model)
          if (temp_bic < current_bic) {
            current_bic <- temp_bic
            model_formula <- temp_formula
            forward_vars <- setdiff(forward_vars, var)
            improved <- TRUE
            break
          }
        }
      }
      
      # 向后剔除
      backward_vars <- setdiff(all.vars(model_formula), c(response_var))
      for (var in backward_vars) {
        temp_formula <- update(model_formula, paste(". ~ . -", var))
        temp_model <- tryCatch({ multinom(temp_formula, data = data, trace = FALSE,maxit=MAX_IT) }, error = function(e) NULL)
        if (!is.null(temp_model)) {
          temp_bic <- BIC(temp_model)
          if (temp_bic < current_bic) {
            current_bic <- temp_bic
            model_formula <- temp_formula
            improved <- TRUE
            break
          }
        }
      }
      cat(" New BIC:", current_bic, "\n")
      cat("Current Model Formula:",revert_chinese_chars(deparse(model_formula)), "\n\n")
    }
    final_model <- multinom(model_formula, data = data, trace = FALSE,maxit=MAX_IT)
  }else{
    W <- data[[weight]]
    forward_vars <- setdiff(names(data), c(response_var,weight))
    forward_vars <- sapply(forward_vars, function(x) if(grepl("[(]|[)]", x)) paste0("`", x, "`") else x)
    forward_vars <- sapply(forward_vars, function(x) if(grepl("-", x)) paste0("`", x, "`") else x)
    
    model_formula <- as.formula(paste(response_var, "~ 1"))
    current_bic <- BIC(multinom(model_formula, data = data, trace = FALSE,maxit=MAX_IT,weights =W))
    improved <- TRUE
    
    while(improved) {
      improved <- FALSE
      
      # 向前选择
      for (var in forward_vars) {
        temp_formula <- update(model_formula, paste(". ~ . +", var))
        temp_model <- tryCatch({ multinom(temp_formula, data = data, trace = FALSE,maxit=MAX_IT,weights =W) }, error = function(e) NULL)
        if (!is.null(temp_model)) {
          temp_bic <- BIC(temp_model)
          if (temp_bic < current_bic) {
            current_bic <- temp_bic
            model_formula <- temp_formula
            forward_vars <- setdiff(forward_vars, var)
            improved <- TRUE
            break
          }
        }
      }
      
      # 向后剔除
      backward_vars <- setdiff(all.vars(model_formula), c(response_var,weight))
      for (var in backward_vars) {
        temp_formula <- update(model_formula, paste(". ~ . -", var))
        temp_model <- tryCatch({ multinom(temp_formula, data = data, trace = FALSE,maxit=MAX_IT,weights =W) }, error = function(e) NULL)
        if (!is.null(temp_model)) {
          temp_bic <- BIC(temp_model)
          if (temp_bic < current_bic) {
            current_bic <- temp_bic
            model_formula <- temp_formula
            improved <- TRUE
            break
          }
        }
      }
      cat(" New BIC:", current_bic, "\n")
      cat("Current Model Formula:",revert_chinese_chars(deparse(model_formula)), "\n\n")
    }
    final_model <- multinom(model_formula, data = data, trace = FALSE,maxit=MAX_IT,weights = W)
  }

  
  

  return(list(final_model = final_model, final_formula = model_formula, final_bic = current_bic))
}


