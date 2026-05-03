# KitchenMAMA-Specific Implementation Guide

This document outlines specific considerations and implementation details for the KitchenMAMA kitchen product line. It focuses on the unique aspects of KitchenMAMA's data structure, customer base, and analytical requirements.

## Data Import Considerations

### Product Categories
KitchenMAMA products are categorized into these main groups:
1. Electric Can Openers
2. Manual Can Openers
3. Kitchen Gadgets
4. Utensil Sets
5. Specialty Tools

### Amazon Data Structure
- ASIN format: B0XXXXXXXX
- Review structure includes verified purchase status
- Rating scale 1-5 stars
- Kitchen category on Amazon has specific seasonality patterns

### Website Data Structure
- Order IDs follow format: KM-YYMMDD-XXXXX
- Direct customer contact information available
- Detailed demographic data including cooking preferences
- Gift purchases flagged specifically

## Customer DNA Components

### Specific to KitchenMAMA
The Customer DNA for KitchenMAMA shoppers includes these specific components:

1. **Cooking Frequency Vector**
   - Daily home cooking (HF-5)
   - Several times weekly (HF-4)
   - Weekend cooking only (HF-3)
   - Occasional cooking (HF-2)
   - Rare cooking (HF-1)

2. **Kitchen Equipment Level**
   - Professional kitchen (KE-5)
   - Well-equipped home kitchen (KE-4)
   - Standard kitchen (KE-3)
   - Basic kitchen (KE-2)
   - Minimal kitchen (KE-1)

3. **Specialty Diet Indicators**
   - Vegetarian/Vegan
   - Gluten-Free
   - Keto
   - Paleo
   - No special diet

4. **Purchasing Behavior**
   - Gift buyer
   - Self-use buyer
   - Replacement buyer
   - Upgrade buyer
   - First-time buyer

## Modeling Approach

### Sales Prediction
For KitchenMAMA products, we use a modified Poisson model that accounts for:
- Holiday-specific cooking peaks (Thanksgiving, Christmas)
- Wedding season gift purchasing
- New Year resolution cooking uptick
- Summer BBQ season complementary products

### Pricing Strategy
Electric can openers follow a distinct pricing pattern:
- Premium tier: $35-45
- Mid-tier: $25-34
- Budget tier: $15-24
- Basic tier: Under $15

Competition analysis should account for these tiers when calculating optimal pricing.

### Marketing Campaign Effectiveness
KitchenMAMA-specific campaign metrics:
- Recipe inclusion lift
- Bundle purchase rate
- Warranty registration rate
- Accessory attachment rate

## Implementation Examples

### Customer Segmentation Code
```r
# KitchenMAMA-specific segmentation
segment_kitchenmama_customers <- function(customer_data) {
  customer_data %>%
    mutate(
      segment = case_when(
        cooking_frequency %in% c("HF-5", "HF-4") & 
          equipment_level %in% c("KE-5", "KE-4") ~ "Cooking Enthusiast",
        gift_purchase_rate > 0.5 ~ "Gift Shopper",
        replacement_frequency < 365 ~ "Quality Seeker",
        first_purchase == TRUE & product_price_tier %in% c("Budget", "Basic") ~ "Price Explorer",
        TRUE ~ "General Customer"
      )
    )
}
```

### Campaign Effectiveness Analysis
```r
# KitchenMAMA-specific campaign analysis
analyze_kitchenmama_campaign <- function(campaign_data, sales_data, campaign_name) {
  # Join campaign exposure with sales data
  combined_data <- campaign_data %>%
    inner_join(sales_data, by = "customer_id")
  
  # KitchenMAMA-specific metrics
  metrics <- combined_data %>%
    group_by(exposed_to_campaign) %>%
    summarize(
      avg_order_value = mean(order_value),
      bundle_rate = sum(is_bundle) / n(),
      recipe_download_rate = sum(downloaded_recipe) / n(),
      warranty_reg_rate = sum(registered_warranty) / n(),
      accessory_attach_rate = sum(purchased_accessory) / n()
    )
  
  return(list(
    campaign_name = campaign_name,
    metrics = metrics,
    lift_calculations = calculate_kitchenmama_lift(metrics)
  ))
}
```

## Data Visualization Guidelines

For KitchenMAMA reports and dashboards:
- Use kitchen-themed color palette: 
  - Primary: #E83F3F (Apple Red)
  - Secondary: #2B4B34 (Basil Green)
  - Tertiary: #F7CB45 (Golden Yellow)
  - Background: #F5F1E4 (Parchment)
  
- Utilize kitchen-themed iconography for categories
- Segment sales data by meal occasions when relevant
- Include recipe correlation analysis in product performance reports

## Recommended Claude Prompts

When working on KitchenMAMA-specific analyses, use these prompt starters:

1. "Analyze the KitchenMAMA Electric Can Opener sales data with special attention to the holiday cooking season patterns..."

2. "Develop a customer segmentation approach for KitchenMAMA that accounts for cooking frequency, equipment level, and dietary preferences..."

3. "Create a competitive analysis dashboard for KitchenMAMA products that highlights price positioning within tiers and feature comparisons..."

---

This implementation guide should be used in conjunction with the general precision marketing framework. It provides KitchenMAMA-specific context to ensure analyses and models reflect the unique characteristics of kitchen product purchasing patterns and customer behavior.

Last Updated: April 1, 2025