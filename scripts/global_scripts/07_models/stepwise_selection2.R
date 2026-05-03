library(nnet)
stepwise_selection2 <- function(data, response_var,model=NULL,...) {
  acceptable_model <- c("multinom","poisson")
  if (is.null(model)){
    stop( "Please specify the type of model." )
  }else if(!  tolower(model) %in% acceptable_model){
    stop("Model is not acceptable, please specify again.")
  }
  # if u want to add more model, u could rewrite the function
  # and add more argument in otherarg
  selecction_model <- function(option,model_formula,data,otherarg) {
    if(is.null(otherarg$W)){
      result <- switch(option,,
                       "multinom" = multinom(model_formula, data = data, trace = FALSE,maxit=otherarg$MAX_IT),
                       "poisson" =  glm(model_formula, data = data, family = poisson()))
    }else{
      result <- switch(option,,
                       "multinom" = multinom(model_formula, data = data, trace = FALSE,
                                             maxit=otherarg$MAX_IT,
                                             weights =otherarg$W),
                       "poisson" =  glm(model_formula, data = data, family = poisson()))
    }
    
    return(result)
  }
  
  

  otherarg <- list(...)
  # 初始化变量
  if(is.null(otherarg$weight)){
    forward_vars <- setdiff(names(data), c(response_var))
    forward_vars <- sapply(forward_vars, function(x) if(grepl("[(]|[)]", x)) paste0("`", x, "`") else x)
    forward_vars <- sapply(forward_vars, function(x) if(grepl("-", x)) paste0("`", x, "`") else x)
    
    model_formula <- as.formula(paste(response_var, "~ 1"))
    current_bic <- BIC(selecction_model(option=tolower(model),model_formula=model_formula,data,otherarg))
    improved <- TRUE
    
    while(improved) {
      improved <- FALSE
      
      # 向前选择
      for (var in forward_vars) {
        temp_formula <- update(model_formula, paste(". ~ . +", var))
        temp_model <- tryCatch({ 
          selecction_model(option=tolower(model),model_formula=temp_formula,data,otherarg)
          #multinom(temp_formula, data = data, trace = FALSE,maxit=MAX_IT)
          }, error = function(e) NULL)
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
        temp_model <- tryCatch({ 
          selecction_model(option=tolower(model),model_formula=temp_formula,data,otherarg)
          #multinom(temp_formula, data = data, trace = FALSE,maxit=MAX_IT) 
          
          }, error = function(e) NULL)
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

   #   cat("Current Model Formula:",revert_chinese_chars(deparse(model_formula)), "\n\n")
      cat("Current Model Formula:",deparse(model_formula), "\n\n")
    }
    #final_model <- multinom(model_formula, data = data, trace = FALSE,maxit=MAX_IT)
    #
    final_model <- selecction_model(option=tolower(model),model_formula=model_formula,data=data,otherarg)
  }else{
    weight <- otherarg$weight
    W <- data[[weight]]
    otherarg$W <- W
 #   print(otherarg)

    
    forward_vars <- setdiff(names(data), c(response_var,weight))
    forward_vars <- sapply(forward_vars, function(x) if(grepl("[(]|[)]", x)) paste0("`", x, "`") else x)
    forward_vars <- sapply(forward_vars, function(x) if(grepl("-", x)) paste0("`", x, "`") else x)
    
    model_formula <- as.formula(paste(response_var, "~ 1"))

    current_bic <- BIC(
     # multinom(model_formula, data = data, trace = FALSE,maxit=MAX_IT,weights =W)

      selecction_model(option=tolower(model),model_formula=model_formula,data,otherarg)
      )
    improved <- TRUE
    
    while(improved) {
      improved <- FALSE
      
      # 向前选择
      for (var in forward_vars) {
        temp_formula <- update(model_formula, paste(". ~ . +", var))
        temp_model <- tryCatch({
          #multinom(temp_formula, data = data, trace = FALSE,maxit=MAX_IT,weights =W) 
          selecction_model(option=tolower(model),model_formula=temp_formula,data,otherarg)
          }, error = function(e) NULL)
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
        temp_model <- tryCatch({
        #  multinom(temp_formula, data = data, trace = FALSE,maxit=MAX_IT,weights =W)
          selecction_model(option=tolower(model),model_formula=temp_formula,data,otherarg)
          }, error = function(e) NULL)
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
#      print(otherarg$W)
      #cat("Current Model Formula:",revert_chinese_chars(deparse(model_formula)), "\n\n")
      cat("Current Model Formula:",deparse(model_formula), "\n\n")
    }
 #   final_model <- multinom(model_formula, data = data, trace = FALSE,maxit=MAX_IT,weights = W)
    final_model <- selecction_model(option=tolower(model),model_formula=model_formula,data,otherarg)
  }
  
  
  
  
  return(list(final_model = final_model, final_formula = model_formula, final_bic = current_bic))
  
}
# data <- data.frame(
#   count = c(3, 1, 4, 1, 2, 7, 4, 6, 3, 2),
#   time = c(1, 2, 3, 4, 5, 1, 2, 3, 4, 5),
#   group = factor(c('A', 'A', 'A', 'B', 'B', 'A', 'A', 'B', 'B', 'B'))
# )
# 
# stepwise_selection2(data=data,response_var = "count",model = "Poisson")
# stepwise_selection2(data=data,response_var = "group",model = "multinom")
# stepwise_selection2(data=data,response_var = "group",model = "multinom",MAX_IT=500)
# stepwise_selection2(data=data,response_var = "group",model = "multinom",MAX_IT=500,weight="time")
# 
