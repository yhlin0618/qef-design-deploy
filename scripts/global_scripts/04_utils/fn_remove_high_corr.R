#' Remove Highly Correlated Variables
#' 
#' This function removes highly correlated numeric variables from a dataframe based on
#' a specified correlation threshold. When two variables exceed the correlation threshold,
#' the variable with more high correlations to other variables is removed.
#' 
#' @param df A dataframe containing variables to check for high correlation
#' @param cutoff Numeric correlation threshold (default: 0.8). Variables with absolute
#'   correlation exceeding this value will be considered for removal.
#' @param method Character string specifying the correlation method. Options are "pearson" 
#'   (default), "kendall", or "spearman".
#' @param verbose Logical indicating whether to print removal messages (default: TRUE)
#' 
#' @return A dataframe with highly correlated numeric variables removed. Non-numeric 
#'   variables are retained without modification.
#' 
#' @details 
#' The function works by:
#' 1. Identifying all numeric columns in the dataframe
#' 2. Calculating the absolute correlation matrix for numeric columns
#' 3. Iteratively identifying pairs of variables exceeding the correlation threshold
#' 4. For each pair, removing the variable that has more high correlations with other variables
#' 5. If both variables have equal numbers of high correlations, the second variable is removed
#' 
#' @examples
#' # Example with iris dataset
#' iris_clean <- remove_high_corr(iris[, 1:4], cutoff = 0.8)
#' names(iris_clean)
#' 
#' # Example with synthetic data
#' set.seed(123)
#' n  <- 100
#' x1 <- rnorm(n)
#' x2 <- x1 + rnorm(n, sd = 0.1)  # Highly correlated with x1
#' x3 <- rnorm(n)
#' x4 <- x3 + rnorm(n, sd = 0.1)  # Highly correlated with x3
#' test_df <- data.frame(x1, x2, x3, x4)
#' 
#' clean_df <- remove_high_corr(test_df, cutoff = 0.8)
#' # Will remove x2 and x4, keeping x1 and x3
#' 
#' @export
remove_high_corr <- function(df, cutoff = 0.8, method = "pearson",
                             verbose = TRUE) {
  ## 1. Identify numeric columns only
  df <- as.data.frame(df)
  num_idx   <- sapply(df, is.numeric)
  num_names <- names(df)[num_idx]
  keep_flag <- rep(TRUE, length(num_names))
  
  ## 2. Calculate absolute correlation matrix (diagonal set to 0 for easier max search)
  cor_mat <- abs(cor(df[, num_idx], use = "pairwise.complete.obs",
                     method = method))
  diag(cor_mat) <- 0
  
  ## 3. Iteratively find and remove variables with highest correlations
  while (any(cor_mat > cutoff, na.rm = TRUE)) {
    # Find the pair with highest correlation
    pair <- which(cor_mat == max(cor_mat), arr.ind = TRUE)[1, ]
    i <- pair[1];  j <- pair[2]
    
    # Get variable names and correlation value
    var_i <- num_names[i]
    var_j <- num_names[j]
    r_ij  <- cor_mat[i, j]
    
    # Count how many times each variable exceeds cutoff with others
    cnt_i <- sum(cor_mat[i, ] > cutoff)
    cnt_j <- sum(cor_mat[j, ] > cutoff)
    
    # Decision: remove the variable with more high correlations
    # If tied, remove the second variable (j)
    drop_idx  <- if (cnt_i >= cnt_j) i else j
    keep_idx  <- if (drop_idx == i) j else i
    drop_var  <- num_names[drop_idx]
    keep_var  <- num_names[keep_idx]
    
    # Print removal message if verbose
    if (isTRUE(verbose)) {
      message(
        sprintf("Remove %-10s (|r| = %.3f with %s)",
                drop_var, r_ij, keep_var)
      )
    }
    
    # Mark for removal and zero out corresponding rows/columns
    keep_flag[drop_idx] <- FALSE
    cor_mat[drop_idx, ] <- 0
    cor_mat[, drop_idx] <- 0
  }
  
  ## 4. Construct output: retained numeric columns + all non-numeric columns
  kept_num <- num_names[keep_flag]
  df_out   <- df[, c(setdiff(names(df), num_names), kept_num), drop = FALSE]
  return(df_out)
}
