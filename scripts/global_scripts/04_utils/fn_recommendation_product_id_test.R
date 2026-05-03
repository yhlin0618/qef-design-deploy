#' Test script for recommend_next_products function
#'
#' This script demonstrates the usage of the recommendation system with synthetic data.
#' It creates a test dataset, runs the recommendation algorithm, and visualizes the results.
#'
#' To run this test:
#' 1. Make sure the required packages are installed (dplyr, randomForest, tidyr, ggplot2)
#' 2. Source or run this file after fn_recommendation_product_id.R has been loaded
#'

# Load required packages
if (!require("dplyr")) install.packages("dplyr")
if (!require("tidyr")) install.packages("tidyr")
if (!require("randomForest")) install.packages("randomForest")
if (!require("ggplot2")) install.packages("ggplot2")
library(dplyr)
library(tidyr)
library(randomForest)
library(ggplot2)

# Check if the recommendation function is available
if (!exists("recommend_next_products")) {
  if (file.exists("fn_recommendation_product_id.R")) {
    source("fn_recommendation_product_id.R")
  } else {
    stop("Please source fn_recommendation_product_id.R first")
  }
}

# Set random seed for reproducibility
set.seed(123)

# Create synthetic purchase data
generate_synthetic_purchase_data <- function(n_customers = 100, 
                                            n_products = 50, 
                                            min_purchases = 3, 
                                            max_purchases = 15,
                                            n_categories = 5,
                                            date_range = c("2023-01-01", "2023-12-31")) {
  
  # Generate customer IDs
  customer_ids <- paste0("C", sprintf("%04d", 1:n_customers))
  
  # Generate product IDs and properties
  product_ids <- paste0("P", sprintf("%04d", 1:n_products))
  
  # Create product properties
  product_properties <- data.frame(
    product_id = product_ids,
    category = sample(paste0("Cat", 1:n_categories), n_products, replace = TRUE),
    price = round(runif(n_products, 10, 200), 2),
    rating = round(runif(n_products, 1, 5), 1),
    stock_level = sample(c("Low", "Medium", "High"), n_products, replace = TRUE),
    stringsAsFactors = FALSE
  )
  
  # Generate purchase records
  purchases_list <- list()
  
  for (i in 1:n_customers) {
    # Number of purchases for this customer
    n_purchases <- sample(min_purchases:max_purchases, 1)
    
    # Generate purchase dates
    date_range_numeric <- as.numeric(as.Date(date_range))
    purchase_dates <- sort(as.Date(sample(date_range_numeric[1]:date_range_numeric[2], n_purchases), origin = "1970-01-01"))
    
    # Generate a preference profile for this customer
    preferred_category <- sample(paste0("Cat", 1:n_categories), 1)
    price_sensitivity <- runif(1, 0, 1)  # Higher means more price sensitive
    
    # Filter products by preference
    preferred_products <- product_properties %>%
      mutate(preference_score = ifelse(category == preferred_category, 2, 1) *
               (1 - price_sensitivity * scale(price)))
    
    # Add some randomness to preferences
    preferred_products$preference_score <- preferred_products$preference_score + rnorm(n_products, 0, 0.5)
    
    # Sort by preference score
    preferred_products <- preferred_products %>%
      arrange(desc(preference_score))
    
    # Generate purchase sequence with some repeat purchases and category affinity
    purchased_products <- character(n_purchases)
    
    for (j in 1:n_purchases) {
      if (j == 1) {
        # First purchase - choose from preferred products with higher probability
        purchased_products[j] <- sample(
          preferred_products$product_id, 
          1, 
          prob = exp(preferred_products$preference_score) / sum(exp(preferred_products$preference_score))
        )
      } else {
        # Subsequent purchases - influence by previous purchase
        prev_product <- purchased_products[j-1]
        prev_category <- product_properties$category[product_properties$product_id == prev_product]
        
        # Increase probability for products in same category
        category_boost <- ifelse(product_properties$category == prev_category, 1.5, 1)
        
        # Avoid immediate duplicates but allow later repeats
        prev_products <- purchased_products[1:(j-1)]
        repeat_penalty <- ifelse(product_properties$product_id %in% prev_products, 0.5, 1)
        
        # Combine factors
        adjusted_scores <- preferred_products$preference_score * category_boost * repeat_penalty
        
        # Ensure probability is positive
        probs <- exp(adjusted_scores) / sum(exp(adjusted_scores))
        
        purchased_products[j] <- sample(
          preferred_products$product_id, 
          1, 
          prob = probs
        )
      }
    }
    
    # Create purchase records for this customer
    customer_purchases <- data.frame(
      customer_id = rep(customer_ids[i], n_purchases),
      product_id = purchased_products,
      purchase_date = purchase_dates,
      stringsAsFactors = FALSE
    )
    
    purchases_list[[i]] <- customer_purchases
  }
  
  # Combine all purchases
  all_purchases <- do.call(rbind, purchases_list)
  
  # Add some random additional features
  all_purchases$quantity <- sample(1:5, nrow(all_purchases), replace = TRUE)
  all_purchases$discount_applied <- sample(c(TRUE, FALSE), nrow(all_purchases), 
                                         replace = TRUE, prob = c(0.2, 0.8))
  all_purchases$purchase_channel <- sample(c("Online", "Store", "Mobile"), 
                                         nrow(all_purchases), replace = TRUE)
  
  # Join with product properties
  purchase_data <- all_purchases %>%
    left_join(product_properties, by = "product_id")
  
  return(purchase_data)
}

# Generate test data
cat("Generating synthetic purchase data...\n")
purchase_data <- generate_synthetic_purchase_data()

# Display data summary
cat("\nData Summary:\n")
cat("Number of customers:", length(unique(purchase_data$customer_id)), "\n")
cat("Number of products:", length(unique(purchase_data$product_id)), "\n")
cat("Number of purchases:", nrow(purchase_data), "\n")
cat("Date range:", min(purchase_data$purchase_date), "to", max(purchase_data$purchase_date), "\n\n")

# Show data structure 
cat("Data structure:\n")
str(purchase_data)

# Show a few rows of the data
cat("\nSample data rows:\n")
print(head(purchase_data, 10))

# Run the recommendation algorithm
cat("\nRunning recommendation algorithm...\n")

# Select covariates
selected_covariates <- c("category", "price", "rating", "purchase_channel", "discount_applied")

# Run recommendations
recommended_products <- recommend_next_products(
  df = purchase_data,
  customer_id_col = "customer_id",
  product_id_col = "product_id",
  time_col = "purchase_date",
  covariate_cols = selected_covariates,
  recommended_number = 5,  # Recommend 5 products for simplicity
  seed = 456,
  rf_trees = 50  # Use fewer trees for faster execution in the test
)

# Display recommendations
cat("\nRecommendation Results:\n")
cat("Recommendations generated for", nrow(recommended_products), "customers\n\n")
print(head(recommended_products, 10))

# Analyze recommendation diversity
cat("\nAnalyzing recommendation diversity...\n")

# Reshape recommendations for analysis
recommendation_long <- recommended_products %>%
  tidyr::pivot_longer(
    cols = starts_with("recommended_next_product_id"),
    names_to = "rank",
    values_to = "product_id"
  )

# Count frequency of recommended products
product_frequency <- recommendation_long %>%
  group_by(product_id) %>%
  summarize(count = n()) %>%
  arrange(desc(count))

# Show top recommended products
cat("\nTop 10 most frequently recommended products:\n")
print(head(product_frequency, 10))

# Calculate how many unique products were recommended
unique_recommended <- length(unique(recommendation_long$product_id))
total_possible <- length(unique(purchase_data$product_id))
cat("\nRecommendation coverage:", 
    unique_recommended, "unique products recommended out of", 
    total_possible, "possible products (", 
    round(100 * unique_recommended / total_possible, 1), "%)\n")

# Create a visualization of recommendation distribution
if (requireNamespace("ggplot2", quietly = TRUE)) {
  # Top 20 products by recommendation frequency
  top_products <- head(product_frequency, 20)
  
  # Create plot
  p <- ggplot(top_products, aes(x = reorder(product_id, count), y = count)) +
    geom_bar(stat = "identity", fill = "steelblue") +
    coord_flip() +
    labs(title = "Top 20 Most Frequently Recommended products",
         x = "Product ID",
         y = "Frequency of Recommendation") +
    theme_minimal()
  
  # Display plot
  print(p)
  
  # Save plot to file
  ggsave("recommendation_frequency.png", p, width = 10, height = 6)
  cat("\nRecommendation frequency plot saved to 'recommendation_frequency.png'\n")
}

# Evaluate recommendation relevance
cat("\nEvaluating recommendation relevance...\n")

# Define a simple relevance metric: 
# Are recommended products in the same category as previously purchased products?

# Get the last purchased product for each customer
last_purchases <- purchase_data %>%
  group_by(customer_id) %>%
  filter(purchase_date == max(purchase_date)) %>%
  select(customer_id, product_id, category) %>%
  rename(last_purchase_id = product_id,
         last_purchase_category = category)

# Get categories for recommended products
recommendation_categories <- recommendation_long %>%
  left_join(
    purchase_data %>% select(product_id, category) %>% distinct(),
    by = "product_id"
  )

# Join with last purchase data
recommendation_relevance <- recommendation_categories %>%
  left_join(last_purchases, by = "customer_id") %>%
  mutate(same_category = category == last_purchase_category)

# Calculate overall category relevance
category_relevance <- recommendation_relevance %>%
  summarize(
    total_recommendations = n(),
    same_category_count = sum(same_category, na.rm = TRUE),
    relevance_rate = mean(same_category, na.rm = TRUE)
  )

cat("\nCategory relevance analysis:\n")
cat(sprintf("%.1f%% of recommendations are in the same category as the customer's last purchase\n",
            100 * category_relevance$relevance_rate))

cat("\nTest completed successfully!\n")