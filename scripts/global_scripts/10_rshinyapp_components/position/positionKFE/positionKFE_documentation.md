# positionKFE Component Documentation

## Overview
The `positionKFE` (Key Factor Evaluation) component performs automated critical success factor analysis for product positioning. It identifies which product attributes are most important for achieving ideal performance and provides benchmarking insights.

## Core Functions

### 1. `perform_kfe_analysis()`
**Purpose**: Main analysis function that identifies key factors and creates performance benchmarks.

**Parameters**:
- `data`: Position data with numerical attributes
- `exclude_vars`: Variables to exclude from analysis (default: NULL)
- `threshold_multiplier`: Sensitivity adjustment (default: 1.0)

**Process**:
1. **Data Preparation**:
   - Checks for `product_id` column, falls back to platform-specific columns (`asin`, `ebay_item_number`)
   - Removes special rows ("Rating", "Revenue")
   - Filters out excluded variables

2. **Ideal Row Extraction**:
   - Finds the "Ideal" row which contains target values for each attribute
   - Returns empty results if no Ideal row exists

3. **Indicator Matrix Creation**:
   - For each numeric column and each product:
     - Compares product value to ideal value
     - Sets indicator to 1 if product >= ideal, 0 otherwise
     - NA values are set to 0

4. **Key Factor Identification**:
   - Calculates "gate" threshold: average indicator score × threshold_multiplier
   - Key factors = columns where sum of indicators > mean(gate)
   - These are the attributes where many products meet/exceed ideal

5. **Benchmark Creation**:
   - For each key factor, identifies which products achieve the ideal value
   - Creates a list of benchmark products for each factor

6. **Scoring**:
   - Creates ideal_analysis dataframe with:
     - Each product's values for key factors only
     - Score = sum of key factor achievements
     - Sorted by score (descending)

**Returns**:
```r
list(
  key_factors = character(),      # Names of identified key factors
  indicators = data.frame(),      # Full indicator matrix
  benchmarks = list(),           # Benchmark products by factor
  ideal_analysis = data.frame(), # Scored and ranked products
  gate_threshold = numeric()     # Calculated threshold value
)
```

### 2. `format_key_factors()`
**Purpose**: Formats factor names for user-friendly display.

**Process**:
- Replaces underscores with spaces
- Converts to title case
- Joins multiple factors with commas

**Example**:
- Input: `c("price_per_unit", "customer_rating")`
- Output: `"Price Per Unit, Customer Rating"`

## UI Components

### `positionKFEFilterUI()`
**Controls**:
1. **Analysis Sensitivity** (threshold_multiplier):
   - Range: 0.5 to 2.0
   - Lower = more factors identified (less selective)
   - Higher = fewer factors identified (more selective)

2. **Display Options**:
   - Show detailed factor analysis (checkbox)
   - Show benchmark products (checkbox)
   - Maximum factors to display (3-15)

3. **Reset Settings**: Returns all settings to defaults

### `positionKFEDisplayUI()`
**Display Modes**:
- **Full Mode**: Complete analysis with header and details
- **Compact Mode**: Simplified view for integration with other components

**Outputs**:
- Key factors list (formatted text)
- Benchmark products (conditional, based on checkbox)

## Server Logic

### Data Flow:
1. **Configuration Extraction**:
   - Extracts `platform_id` and `product_line_id` from config
   - Handles both direct config and nested filter config

2. **Data Loading**:
   - Uses `fn_get_position_complete_case()` with `include_special_rows = TRUE`
   - Requires "Ideal" row for analysis
   - Handles platform-specific column naming

3. **Analysis Execution**:
   - Triggered when data changes or sensitivity adjusts
   - Updates component status throughout process

4. **Output Rendering**:
   - Formats key factors with count limits
   - Shows benchmark details with product limits (max 5 per factor)
   - Provides informative status messages

## Component Status States
- `idle`: Ready for analysis or no product line selected
- `loading`: Fetching position data
- `computing`: Running KFE analysis
- `ready`: Analysis complete with results
- `error`: Problem occurred during processing

## Key Business Logic

### What Makes a "Key Factor"?
A factor becomes "key" when:
1. Many products achieve the ideal value for that attribute
2. The sum of achievements exceeds the calculated threshold
3. It represents a common strength across successful products

### Interpretation:
- **High Score Products**: Excel at most key factors
- **Low Score Products**: Need improvement in key areas
- **Benchmark Products**: Examples that achieve ideal in specific factors

### Use Cases:
1. **Product Development**: Identify which attributes to prioritize
2. **Competitive Analysis**: See which competitors excel at key factors
3. **Performance Gaps**: Find where products fall short of ideal

## Integration Example
```r
# Create component
kfe_comp <- positionKFEComponent(
  id = "kfe_analysis",
  app_data_connection = conn,
  config = reactive({ 
    list(
      platform_id = input$platform,
      product_line_id = input$product_line
    )
  })
)

# Add to UI
kfe_comp$ui$filter  # Filter controls
kfe_comp$ui$display # Analysis results

# Initialize server
kfe_server <- kfe_comp$server(input, output, session)

# Access results
kfe_results <- kfe_server$kfe_result()
```

## Technical Notes

### Performance Considerations:
- Analysis complexity: O(n × m) where n = products, m = attributes
- Handles NA values gracefully (treated as not achieving ideal)
- Efficient column-wise operations using vectorized R

### Data Requirements:
- Must have "Ideal" row in position data
- Numeric columns for analysis (non-numeric ignored)
- Product identifier column (product_id, asin, or ebay_item_number)

### Error Handling:
- Missing Ideal row → empty results with warning
- No numeric columns → empty results with warning
- Missing product ID → attempts platform-specific columns
- All errors logged with informative messages