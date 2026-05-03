# Position Table Filtering Strategy

## Overview

This document describes the filtering strategy used in the `positionTable` component to handle sparse data with many NA values while preserving meaningful information for analysis.

## Problem Context

Position table data typically contains:
- **Essential columns**: `item_id`, `brand`, `product_line_id`, `rating`, `sales`
- **Attribute columns**: Product characteristics (e.g., "配送快速", "品質優良", "價格實惠")
- **High sparsity**: Many attribute columns have significant NA values
- **Cross-contamination risk**: Different product lines cannot be meaningfully compared

## Filtering Philosophy

### Design Principles
1. **Preserve over Filter**: Better to show more data than to over-filter
2. **Simplicity over Complexity**: Avoid compound filtering effects
3. **Predictable Results**: Users should understand what gets filtered and why
4. **Essential Data Protection**: Never remove core business columns

### Strategy: Simple Two-Step Filtering (No Loops)

#### Step 1: Remove Completely Empty Rows
- **Target**: Products with NO attribute information
- **Logic**: Remove rows where ALL attribute columns are NA
- **Rationale**: If a product has no attribute data, it cannot contribute to positioning analysis

#### Step 2: Remove Mostly Empty Columns
- **Target**: Attributes that are largely missing
- **Logic**: Remove columns where non-NA ratio < threshold (default: 30%)
- **Rationale**: Attributes with too little data cannot provide reliable insights

## Implementation Details

### Function: `simple_filter_position_table()`

```r
simple_filter_position_table <- function(data, threshold = 0.3)
```

#### Parameters
- `data`: Raw position table data
- `threshold`: Minimum non-NA ratio for column retention (default: 0.3 = 30%)

#### Process Flow

1. **Initialization**
   - Identify essential columns (always preserved)
   - Identify filterable columns (numeric attributes only)
   - Log initial data dimensions

2. **Step 1: Row Filtering**
   ```r
   # Count non-NA values per row in attribute columns
   row_non_na_count <- apply(data[filterable_cols], 1, function(row) {
     sum(!is.na(row))
   })
   
   # Keep rows with at least one non-NA attribute
   non_empty_rows <- row_non_na_count > 0
   ```

3. **Step 2: Column Filtering**
   ```r
   # Calculate non-NA ratio per column
   col_valid_ratio <- sapply(data[filterable_cols], function(col) {
     sum(!is.na(col)) / length(col)
   })
   
   # Keep columns meeting threshold
   valid_cols <- names(col_valid_ratio)[col_valid_ratio >= threshold]
   ```

## Historical Context

### Previous Approach: Iterative Filtering
- **Method**: Alternating row-column filtering until convergence
- **Issues**:
  - Over-aggressive filtering
  - Compound effects leading to empty results
  - Unpredictable convergence behavior
  - Complex debugging

### Why We Changed
1. **Data Loss**: Iterative approach removed all attributes
2. **Complexity**: Loop logic was hard to predict and debug
3. **Performance**: Multiple iterations were unnecessary
4. **User Experience**: Empty tables provided no analytical value

## Configuration Guidelines

### Threshold Selection
- **0.1 (10%)**: Very lenient, keeps almost all columns
- **0.3 (30%)**: Balanced approach (current default)
- **0.5 (50%)**: Stricter filtering, fewer but higher-quality columns
- **0.7 (70%)**: Very strict, only well-populated attributes

### Recommended Thresholds by Use Case
- **Exploratory Analysis**: 0.1-0.2 (see more attributes)
- **Standard Reporting**: 0.3-0.4 (balanced view)
- **High-Quality Analysis**: 0.5-0.6 (reliable data only)

## Monitoring and Validation

### Log Messages
The function provides clear logging:
```
🔧 開始簡單篩選 Position Table...
  📊 原始資料：50 行 × 40 列
  🔧 可篩選欄位：35 個
  🛡️  必保留欄位：item_id, brand, product_line_id, rating, sales
  ✂️  步驟1 - 移除空行：保留 45/50 行
  ✂️  步驟2 - 移除空列：保留 20/35 屬性欄位（比例 >= 0.3）
🎯 簡單篩選完成：45 行 × 25 列
```

### Quality Checks
- Monitor retention rates (rows and columns)
- Validate essential columns are preserved
- Check for completely empty results
- Review attribute diversity in final dataset

## Integration Points

### Component Integration
- Called in `positionTable.R` line 661
- Applied after data retrieval but before display formatting
- Works with both complete and demonstrate case data

### Data Flow
```
Raw Data → Product Line Filter → Type Filter → Simple Filter → Display Formatting
```

### Error Handling
- Returns original data if input is null or empty
- Preserves essential columns regardless of threshold
- Logs warnings for extreme filtering results

## Future Considerations

### Potential Enhancements
1. **Adaptive Thresholds**: Adjust based on data sparsity
2. **User Configuration**: Allow threshold adjustment in UI
3. **Smart Grouping**: Group related attributes before filtering
4. **Quality Metrics**: Add data quality indicators to output

### Maintenance Notes
- Monitor user feedback on attribute availability
- Adjust default threshold based on usage patterns
- Consider dataset-specific configurations
- Review effectiveness with different product lines

---

**Last Updated**: 2025-06-20  
**Version**: 1.0  
**Author**: Claude Code Assistant  
**Review Status**: Initial Implementation