# Module 1.1: Establishing Customer DNA

This document outlines the general process of creating a comprehensive customer DNA analysis system for precision marketing. It covers the data preparation, processing, and application of customer DNA metrics.

**IMPORTANT NOTE**: The specific implementation details may vary between different projects. The examples provided here are general templates that should be adapted based on the specific data structures and requirements of each project. Always refer to the project-specific scripts in your update_scripts directory for the actual implementation details.

## Overview

Customer DNA is a holistic profile of each customer derived from their purchasing behavior and interactions. It consists of multiple metrics that together create a complete picture of customer value, behavior patterns, and future potential.

## Data Requirements

Customer transaction data must include at least:
- `customer_id`: Unique identifier for each customer
- `time`: Purchase timestamp
- `total`: Purchase amount
- Additional fields such as product details, location data are useful but optional

## Process Flow

### Step 1: Data Preparation

```r
# Source required scripts
source("update_scripts/global_scripts/03_config/global_parameters.R")
source("update_scripts/global_scripts/04_utils/left_join_remove_duplicate.R")
source("update_scripts/global_scripts/05_data_processing/common/DNA_Function_dplyr.R")

# Connect to databases
raw_data <- dbConnect_from_list("raw_data")
cleansed_data <- dbConnect_from_list("cleansed_data")
processed_data <- dbConnect_from_list("processed_data")

# Import raw data if necessary
import_amazon_sales_dta(
  folder_path = "path/to/amazon_data",
  connection = raw_data,
  clean_columns = TRUE,
  verbose = TRUE
)

# Process raw data into cleansed format
process_amazon_sales(raw_data, cleansed_data)
```

### Step 2: Prepare Customer Time Series Data

```r
# Transform sales data into customer time series with IPT (Inter-Purchase Time)
Data_byCustomertime <- tbl(cleansed_data, "amazon_sales_dta") %>%
  arrange(customer_id, time) %>%
  group_by(customer_id) %>%
  mutate(
    times = row_number(),                      # Purchase sequence
    ni = n(),                                  # Total purchases per customer
    IPT = case_when(
      times == 1 ~ as.numeric(NA),            # No IPT for first purchase
      TRUE ~ as.numeric(difftime(time, lag(time), units = "days"))
    )
  ) %>%
  collect()
```

### Step 3: Generate Customer DNA

```r
# Apply the DNA function (dplyr version for readability)
customer_dna_results <- DNA_Function_dplyr(
  Data_byCustomertime, 
  Skip.within.Subject = FALSE,  # Set to TRUE to skip within-subject analysis
  replace_NESmedian = TRUE      # Set to TRUE to recalculate NES median
)

# Extract results
Data_byCustomer <- customer_dna_results$Data_byCustomer  # DNA metrics
NrecAccu <- customer_dna_results$NrecAccu               # Churn prediction accuracy

# For larger datasets, use the data.table version for performance
# customer_dna_results <- analyze_customer_behavior_dt(Data_byCustomertime)
```

### Step 4: Save and Use Results

```r
# Save customer DNA to database
dbWriteTable(processed_data, "customer_dna", Data_byCustomer, overwrite = TRUE)

# Create indexes for performance
dbExecute(processed_data, "CREATE INDEX IF NOT EXISTS idx_customer_id ON customer_dna (customer_id)")
dbExecute(processed_data, "CREATE INDEX IF NOT EXISTS idx_nesstatus ON customer_dna (NESstatus)")

# Clean up
dbDisconnect_all()
```

## DNA Components and Their Meaning

The customer DNA includes the following components:

### 1. RFM Analysis

| Metric | Description | Business Use |
|--------|-------------|--------------|
| `Rvalue` | Days since last purchase | Identify recent vs. dormant customers |
| `Rlabel` | Recency category (Recent/Mid-term/Long-term) | Segment customers by recency |
| `Fvalue` | Number of purchases | Measure purchase frequency |
| `Flabel` | Frequency category (Low/Medium/High) | Segment by purchase frequency |
| `Mvalue` | Average purchase amount | Measure monetary value |
| `Mlabel` | Monetary value category (Low/Medium/High) | Segment by spend level |

### 2. NES Status

| Metric | Description | Business Use |
|--------|-------------|--------------|
| `NESratio` | Normalized time since last purchase (relative to mean inter-purchase time) | Standardized measure of customer dormancy |
| `NESstatus` | Customer lifecycle status: <br> - N: New customer <br> - E0: Active customer <br> - S1-S3: Increasingly dormant | Lifecycle marketing, reactivation campaigns |

### 3. Customer Activity Index (CAI)

| Metric | Description | Business Use |
|--------|-------------|--------------|
| `CAI` | Trend in purchase frequency (negative = slowing, positive = accelerating) | Identify changing behavior patterns |
| `CAIlabel` | Activity trend category (Declining/Stable/Increasing) | Target customers based on activity trends |

### 4. Value Metrics

| Metric | Description | Business Use |
|--------|-------------|--------------|
| `PCV` | Past Customer Value (weighted sum of historical purchases) | Measure historical value |
| `CLV` | Customer Lifetime Value (predicted future value) | Prioritize high-value customers |
| `E0T` | Average purchase amount during active periods | Set expectations for future purchases |

### 5. Stability and Prediction

| Metric | Description | Business Use |
|--------|-------------|--------------|
| `CRI` | Customer Regularity Index (consistency of purchase intervals) | Identify consistent vs. sporadic buyers |
| `IPT_mean` | Average time between purchases | Predict next purchase window |
| `Nrec` | Churn prediction (binary: will/won't repurchase) | Target potential churners |
| `Nrec_prob` | Probability of churn | Prioritize high-risk customers |

### 6. History and Tenure

| Metric | Description | Business Use |
|--------|-------------|--------------|
| `time_First` | First purchase date | Track customer cohorts |
| `time_FirstTonow` | Days since first purchase | Measure customer tenure |
| `NT` | First purchase amount | Analyze acquisition quality |

## Applications

### 1. Customer Segmentation

```r
# Segment customers by NES status
nes_segments <- tbl(processed_data, "customer_dna") %>%
  group_by(NESstatus) %>%
  summarise(
    count = n(),
    avg_clv = mean(CLV, na.rm = TRUE),
    avg_pcv = mean(PCV, na.rm = TRUE)
  ) %>%
  collect()

# Create RFM segments
rfm_segments <- tbl(processed_data, "customer_dna") %>%
  group_by(Rlabel, Flabel, Mlabel) %>%
  summarise(
    count = n(),
    avg_clv = mean(CLV, na.rm = TRUE)
  ) %>%
  collect()
```

### 2. Campaign Targeting

```r
# Target sleeping customers with reactivation offers
sleeping_customers <- tbl(processed_data, "customer_dna") %>%
  filter(
    NESstatus %in% c("S1", "S2"),  # Moderately dormant customers
    Mlabel == "高購買買家",          # High monetary value
    Nrec_prob < 0.7                # Not likely to reactivate naturally
  ) %>%
  collect()

# Target at-risk customers for retention
at_risk_customers <- tbl(processed_data, "customer_dna") %>%
  filter(
    NESstatus == "E0",             # Currently active
    CAIlabel == "漸趨靜止客戶",       # Declining activity
    Nrec_prob > 0.4                # Some risk of churn
  ) %>%
  collect()
```

### 3. CLV Optimization

```r
# Identify high-value customers for premium services
premium_customers <- tbl(processed_data, "customer_dna") %>%
  filter(
    CLV > quantile(CLV, 0.9),      # Top 10% by CLV
    NESstatus %in% c("N", "E0")    # Active customers
  ) %>%
  collect()

# Find customers with high growth potential
growth_potential <- tbl(processed_data, "customer_dna") %>%
  filter(
    CAIlabel == "漸趨活躍客戶",       # Increasing activity
    Flabel == "中頻買家（兩次）",     # Medium frequency
    ni >= 2                        # At least 2 purchases
  ) %>%
  collect()
```

## Validation and Monitoring

Regularly validate the DNA metrics:

```r
# Check NES median drift
nes_median_history <- tbl(processed_data, "dna_tracking") %>%
  select(date, nes_median) %>%
  arrange(date) %>%
  collect()

# Monitor churn prediction accuracy
churn_accuracy <- tbl(processed_data, "dna_tracking") %>%
  select(date, churn_accuracy) %>%
  arrange(date) %>%
  collect()

# Plot customer status distribution over time
nes_distribution <- tbl(processed_data, "dna_trend") %>%
  group_by(date, NESstatus) %>%
  summarise(count = n()) %>%
  collect()
```

## Best Practices

1. **Regular Updates**: Recalculate customer DNA weekly or monthly
2. **Cohort Analysis**: Compare DNA metrics across acquisition cohorts
3. **A/B Testing**: Test different interventions for each DNA segment
4. **Integration**: Connect DNA metrics to marketing automation systems
5. **Visualization**: Create dashboards to monitor DNA metrics over time
6. **Customization**: Adjust thresholds based on industry and business model

## Parameters to Adjust

Key parameters in `global_parameters.R` that affect DNA calculations:

```r
# Interest rate for CLV calculation
r = 1.085      # Annual interest rate
delta = (r/365)*0.01  # Daily discount rate

# NES breakpoints
NESbreaks = c(0, 1, 2, 2.5, Inf)  # Relative to median IPT
textNESlabel = c("E0","S1","S2","S3")  # Status labels

# Customer Activity Index thresholds
CAIbreaks = c(0, 0.1, 0.9, 1)
textCAIlabel = c("漸趨靜止客戶", "穩定消費客戶", "漸趨活躍客戶")

# Frequency breakpoints
Fbreaks = c(0-dq, 1.1, 2.1, Inf)  # Based on purchase count
textFlabel = c("低頻買家（一次）","中頻買家（兩次）","高頻買家（>三次）")

# Recency and Monetary value breakpoints (percentile-based)
Rbreaks = c(0-dq, 0.1-dq, 0.9+dq, 1+dq)
Mbreaks = c(0-dq, 0.1-dq, 0.9+dq, 1+dq)
```

## Troubleshooting

Common issues and solutions:

1. **Missing IPT values**: Ensure first purchases have NA for IPT
2. **NaN in CAI calculation**: Check for customers with only one purchase
3. **Extreme CLV values**: Review discount rate parameters
4. **Poor churn prediction**: Ensure sufficient historical data for training

## References

- Common utilities in global_scripts:
  - `/update_scripts/global_scripts/05_data_processing/common/DNA_Function_dplyr.R`
  - `/update_scripts/global_scripts/03_config/global_parameters.R`
  - `/update_scripts/global_scripts/04_utils/left_join_remove_duplicate.R`

- Project-specific implementations:
  - Check your project's specific update_scripts directory for customized DNA analysis functions
  - The implementation details, parameter values, and field names may vary between projects
  - Always consult the project documentation for exact specifications