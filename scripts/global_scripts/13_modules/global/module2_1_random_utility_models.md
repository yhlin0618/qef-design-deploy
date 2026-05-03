# Module 2.1: Random Utility Models (RUM)

## Purpose

The Random Utility Models Module implements discrete choice models based on random utility theory for analyzing and predicting customer preferences, choice behavior, and demand across multiple alternatives. This module provides a framework to estimate coefficients that quantify the impact of product attributes and customer characteristics on choice probabilities.

## Key Principles

1. **Utility Maximization**: Models are based on the assumption that customers choose alternatives that maximize their utility
2. **Preference Heterogeneity**: Accounts for variation in preferences across different customer segments
3. **Probabilistic Choice**: Treats customer decisions as probabilistic rather than deterministic
4. **Attribute-Based Analysis**: Focuses on how product attributes influence customer choices
5. **Maximum Likelihood Estimation**: Uses MLE techniques to estimate model parameters

## Module Inputs

- Choice data with customer IDs, product IDs, choice indicators, and choice sets
- Product attributes (price, features, etc.)
- Customer characteristics (demographics, purchase history, etc.)
- Model specification parameters

## Module Outputs

- Estimated preference coefficients
- Choice probability predictions
- Customer preference segments
- Model fit statistics and validation metrics
- Derived insights for marketing strategies

## Process Flow

```
                     ┌──────────────────────────────────────────────────┐
                     │                                                  │
Customer & Product   │                                                  │
     Data      ──────→  Design Matrix   →  Estimation   →  Evaluation   ├───→ Preference
                     │   Generation       (MLE/SGD)       & Prediction  │     Insights
                     │                                                  │
                     │          Random Utility Models Module            │
                     └──────────────────────────────────────────────────┘
                                              │
                                              ↓
                               Marketing Strategy Recommendations
                                   (Pricing, Targeting, etc.)
```

## Module Components

### 1. Design Matrix Generation
- Creates appropriate design matrices for choice models
- Handles product-specific and customer-specific variables
- Implements proper identification constraints
- Supports various model specifications (MNL, Nested Logit, etc.)

### 2. Model Estimation
- Implements maximum likelihood estimation
- Supports regularization techniques (L1, L2)
- Provides optimization algorithms for large datasets
- Includes convergence diagnostics

### 3. Model Evaluation
- Calculates various fit statistics (log-likelihood, AIC, BIC)
- Implements prediction accuracy metrics
- Performs cross-validation
- Generates choice probability predictions

### 4. Preference Analysis
- Identifies key drivers of choice
- Segments customers based on preferences
- Calculates willingness to pay for attributes
- Simulates market shares under different scenarios

## Implementation Details

### Design Matrix Generation
```r
# The module provides functions to transform raw choice data into appropriate design matrices
Design_matrix_Generation.choice_model <- function(sub_res, design_list) {
  # sub_res: data.table with columns:
  #   idx - observation key
  #   Choice - binary indicator of chosen alternative
  #   Subject_ID - customer identifier
  #   product_ID - product identifier
  #   ChoiceSet - indicator of products in choice set
  #   ... (plus attribute variables)
  
  # design_list: specification for the model structure
  #   baseline_id - reference customer segment
  #   baseline_product_id - reference product
  #   beta_i.covariate - customer-specific variables
  #   beta_j.covariate - product-specific variables
  
  # Implementation creates a sparse matrix with:
  # - product-specific intercepts
  # - Customer-specific parameters
  # - Variables interacted with customer segments
  # - Product-specific attribute effects
  
  # Returns a sparse matrix ready for estimation
}
```

### Model Estimation
```r
# Maximum likelihood estimation for choice models
choice_model_MLE <- function(data, Ini, Lambda = 0, printlevel = 1) {
  # data: design matrix from Design_matrix_Generation
  # Ini: initial parameter values
  # Lambda: regularization parameter (L2 penalty)
  
  # Uses maxLik package for optimization
  # Returns maximum likelihood estimates and standard errors
}

# Log-likelihood function for choice models
choice_llik <- function(data, parameter, lambda = 0) {
  # Calculates multinomial logit log-likelihood
  # Supports regularization with L2 penalty
}

# Score function (gradient of log-likelihood)
choice_score <- function(data, parameter, lambda = 0) {
  # Analytical gradient for efficient optimization
}
```

### Model Evaluation
```r
# Prediction accuracy calculation
predict_acc <- function(data, parameter) {
  # Calculates hit rate (% of correctly predicted choices)
}

# Top-K prediction accuracy
predict_acc2 <- function(data, parameter, range = 1) {
  # Calculates accuracy when considering top K choices
}

# Baseline accuracy benchmark
base_line_get <- function(data) {
  # Calculates naive baseline predictions for comparison
}
```

## Usage Example

```r
# 1. Prepare data
choice_data <- data.table(
  idx = rep(1:100, each = 3),            # 100 choice occasions with 3 options each
  Choice = sample(c(1,0,0), 300, replace = TRUE, prob = c(0.6, 0.2, 0.2)), # Observed choices
  Subject_ID = rep(1:20, each = 15),     # 20 customers
  product_ID = rep(1:3, 100),               # 3 product alternatives
  ChoiceSet = rep(1, 300),               # All options are available
  price = runif(300, 10, 30),            # Product price
  quality = sample(1:5, 300, replace = TRUE), # Product quality
  customer_age = rep(sample(20:60, 20), each = 15) # Customer characteristic
)

# 2. Define model specification
design_list <- list(
  baseline_id = 1,              # Reference customer
  baseline_product_id = 1,         # Reference product
  beta_i.covariate = c("price", "quality"), # Product attributes
  beta_j.covariate = c("customer_age")      # Customer characteristics
)

# 3. Generate design matrix
design_matrix <- Design_matrix_Generation.choice_model(
  sub_res = choice_data,
  design_list = design_list
)

# 4. Set initial parameter values
initial_params <- rep(0, ncol(design_matrix) - 5) # Exclude idx, Choice, etc.

# 5. Estimate model
model_results <- choice_model_MLE(
  data = design_matrix,
  Ini = initial_params,
  Lambda = 0.01 # L2 regularization
)

# 6. Evaluate predictions
accuracy <- predict_acc(design_matrix, model_results$estimate)
top2_accuracy <- predict_acc2(design_matrix, model_results$estimate, range = 2)
baseline <- base_line_get(design_matrix)

# 7. Analyze results
parameter_estimates <- data.table(
  Parameter = colnames(design_matrix)[-c(1:5)],
  Estimate = model_results$estimate,
  StdErr = sqrt(diag(solve(-model_results$hessian)))
)
```

## Integration with Other Modules

- Receives processed customer data from Module 1.2 (Data Cleansing)
- Provides preference insights to Module 3.1 (Customer Segmentation)
- Supplies choice probabilities to Module 3.2 (Marketing Response)
- Feeds parameters to Module 4.1 (Pricing Optimization)

## Theoretical Foundation

The module implements random utility theory, which posits that:

1. A customer chooses the alternative that maximizes their utility
2. Utility consists of:
   - Observable systematic utility based on attributes (V)
   - Random unobserved utility (ε)
3. Total utility: U = V + ε
4. Assuming Type I extreme value distribution for ε leads to the multinomial logit model

For a multinomial logit model, the probability of choosing alternative j from set C is:
P(j|C) = exp(Vj) / Σk∈C exp(Vk)

## Limitations and Considerations

- IIA (Independence of Irrelevant Alternatives) assumption
- Potential for endogeneity in attribute variables
- Need for sufficient variation in choices
- Computational complexity for large datasets
- Importance of proper identification constraints

## References

- McFadden, D. (1974). "Conditional logit analysis of qualitative choice behavior"
- Train, K. (2009). "Discrete Choice Methods with Simulation"

## Maintainers

- Data Science Team
- Marketing Analytics Team

## Documentation References

- [Data Integrity Principles](../../00_principles/data_integrity_principles.md)
- [Function Reference](../../00_principles/function_reference.md)
- [Project Principles](../../00_principles/project_principles.md)