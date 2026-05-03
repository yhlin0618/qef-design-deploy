library(nnet)
stepwise_selection3 <- function(data, response_var, important_vars = NULL, model=NULL, ...) {
  acceptable_model <- c("multinom","poisson")
  if(!is.null(important_vars )){
    if(!all(important_vars %in% colnames(data))){
      stop("Error! Not all important variables are in the data. Please check your input again.")
    }
  }
  if (is.null(model)){
    stop("Please specify the type of model.")
  } else if(!tolower(model) %in% acceptable_model){
    stop("Model is not acceptable, please specify again.")
  }
  
  selecction_model <- function(option, model_formula, data, otherarg) {
    if (is.null(otherarg$W)) {
      result <- switch(option,
                       "multinom" = multinom(model_formula, data = data, trace = FALSE, maxit=otherarg$MAX_IT),
                       "poisson" = glm(model_formula, data = data, family = poisson()))
    } else {
      result <- switch(option,
                       "multinom" = multinom(model_formula, data = data, trace = FALSE,
                                             maxit=otherarg$MAX_IT,
                                             weights = otherarg$W),
                       "poisson" = glm(model_formula, data = data, family = poisson()))
    }
    return(result)
  }
  
  otherarg <- list(...)
  if (is.null(otherarg$weight)) {
    # if(is.null(important_vars)){
    #   important_vars <- character()
    # }
   # important_vars <- ifelse(is.null(important_vars), character(), important_vars)
    forward_vars <- setdiff(names(data), c(response_var, important_vars))
    forward_vars <- sapply(forward_vars, function(x) if(grepl("[(]|[)]", x)) paste0("`", x, "`") else x)
    forward_vars <- sapply(forward_vars, function(x) if(grepl("-", x)) paste0("`", x, "`") else x)
    
    model_formula <-formula(paste(response_var, 
                                     ifelse(is.null(important_vars),
                                            "~ 1",
                                            paste0("~ 1+",paste0(important_vars,collapse = "+"))
                                     )
                                     ,colllapse="")
    )
    current_bic <- BIC(selecction_model(option=tolower(model), model_formula=model_formula, data, otherarg))
    improved <- TRUE
    
    while (improved) {
      improved <- FALSE
      
      for (var in forward_vars) {
        temp_formula <- update(model_formula, paste(". ~ . +", var))
        temp_model <- tryCatch({ 
          selecction_model(option=tolower(model), model_formula=temp_formula, data, otherarg)
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
      
      backward_vars <- setdiff(all.vars(model_formula), c(response_var, important_vars))
      for (var in backward_vars) {
        temp_formula <- update(model_formula, paste(". ~ . -", var))
        temp_model <- tryCatch({ 
          selecction_model(option=tolower(model), model_formula=temp_formula, data, otherarg)
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
      cat("Current Model Formula:", deparse(model_formula), "\n\n")
    }
    final_model <- selecction_model(option=tolower(model), model_formula=model_formula, data=data, otherarg)
  } else {
    weight <- otherarg$weight
    W <- data[[weight]]
    otherarg$W <- W
    
    important_vars <- ifelse(is.null(important_vars), character(0), important_vars)
    forward_vars <- setdiff(names(data), c(response_var, important_vars, weight))
    forward_vars <- sapply(forward_vars, function(x) if(grepl("[(]|[)]", x)) paste0("`", x, "`") else x)
    forward_vars <- sapply(forward_vars, function(x) if(grepl("-", x)) paste0("`", x, "`") else x)
    
    #model_formula <- as.formula(paste(response_var, "~ 1 +", paste(important_vars, collapse = " + ")))
    model_formula <- as.formula(paste(response_var, 
                                      ifelse(is.null(important_vars),
                                             "~ 1",
                                             paste0("~ 1+",paste0(important_vars,collapse = "+"))
                                             )
                                      ,colllapse="")
                                )
    current_bic <- BIC(selecction_model(option=tolower(model), model_formula=model_formula, data, otherarg))
    improved <- TRUE
    
    while (improved) {
      improved <- FALSE
      
      for (var in forward_vars) {
        temp_formula <- update(model_formula, paste(". ~ . +", var))
        temp_model <- tryCatch({
          selecction_model(option=tolower(model), model_formula=temp_formula, data, otherarg)
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
      
      backward_vars <- setdiff(all.vars(model_formula), c(response_var, important_vars, weight))
      for (var in backward_vars) {
        temp_formula <- update(model_formula, paste(". ~ . -", var))
        temp_model <- tryCatch({
          selecction_model(option=tolower(model), model_formula=temp_formula, data, otherarg)
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
      cat("Current Model Formula:", deparse(model_formula), "\n\n")
    }
    final_model <- selecction_model(option=tolower(model), model_formula=model_formula, data=data, otherarg)
  }
  
  return(list(final_model = final_model, final_formula = model_formula, final_bic = current_bic))
}


# 
# 
# 
# # Set seed for reproducibility
# set.seed(123)
# 
# # Generate synthetic data
# n <- 200  # Number of observations
# 
# # Response variable (categorical with 3 levels)
# response_var <- factor(sample(c("Class1", "Class2", "Class3"), n, replace = TRUE))
# 
# # Predictor variables
# predictor1 <- rnorm(n) # Continuous variable
# predictor2 <- rnorm(n, mean = 5, sd = 2) # Continuous variable
# predictor3 <- factor(sample(c("Level1", "Level2", "Level3"), n, replace = TRUE)) # Categorical variable
# 
# # Combine into a data frame
# data <- data.frame(response_var, predictor1, predictor2, predictor3)
# # Run the stepwise selection function
# results <- stepwise_selection3(data, response_var = "response_var", important_vars = c("predictor1","predictor2"),
#                                model = "multinom", MAX_IT = 100)
# results <- stepwise_selection3(data, response_var = "response_var", model = "multinom", MAX_IT = 100)
# 
# 
# # Load the iris dataset
# data(iris)
# 
# # Convert the Species column to a factor (response variable)
# iris$Species <- as.factor(iris$Species)
# iris$Sepal.Width <- round(iris$Sepal.Width)
# # Run the stepwise selection function
# results <- stepwise_selection3(data = iris,
#                                response_var = "Sepal.Width",
#                                model = "poisson",important_vars = c("Species","Sepal.Length"))
# 
# 
# # View the results
# print(results$final_formula)
# summary(results$final_model)

# 
# 
