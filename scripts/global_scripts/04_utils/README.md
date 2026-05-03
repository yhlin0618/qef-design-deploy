# Utility Functions

This directory contains utility functions for the Precision Marketing application.

## Available Functions

### analysis_dna

Customer DNA analysis function that examines customer behavior patterns and calculates various metrics.

**File:** `fn_analysis_dna.R`

**Description:**
Analyzes customer transaction data to compute various customer metrics including:
- RFM (Recency, Frequency, Monetary value)
- Customer Activity Index (CAI)
- Past Customer Value (PCV)
- Customer Lifetime Value (CLV)
- Next Expected Shopping (NES) status
- Churn prediction

**Usage:**
```r
# Source the function
source(file.path("update_scripts", "global_scripts", "utils", "fn_analysis_dna.R"))

# Basic usage
results <- analysis_dna(df_sales)

# With aggregated data
results <- analysis_dna(df_sales, df_sales_by_customer_id)

# Skip within-subject analysis for better performance
results <- analysis_dna(df_sales, skip_within_subject = TRUE)

# Turn off verbose messages
results <- analysis_dna(df_sales, verbose = FALSE)
```

**Parameters:**
- `df_sales`: Transaction data containing customer_id, time, total, etc.
- `df_sales_by_customer_id`: Optional aggregated data by customer_id
- `skip_within_subject`: Logical; if TRUE, skips within-subject calculations (faster)
- `verbose`: Logical; if TRUE, outputs progress messages

**Returns:**
A list containing:
- `Data_byCustomer`: A tibble with customer DNA metrics
- `NrecAccu`: Accuracy metrics for the churn prediction model

**Example:**
```r
# Load data
df_ebay_sales <- tbl(processed_data, "df_ebay_sales") %>% collect()
df_ebay_sales_by_customer_id <- tbl(processed_data, "df_ebay_sales_by_customer_id") %>% collect()

# Run DNA analysis
dna_results <- analysis_dna(df_ebay_sales, df_ebay_sales_by_customer_id)

# Store results in database
dbWriteTable(processed_data, "customer_dna_analysis", dna_results$Data_byCustomer, overwrite = TRUE)

# Check accuracy of churn prediction
print(dna_results$NrecAccu)
```

**Notes:**
- The function requires global parameters defined in `global_parameters.R`
- This function is a replacement for the legacy `DNA_Function_dplyr2` function
- Performance optimized with data.table for faster processing