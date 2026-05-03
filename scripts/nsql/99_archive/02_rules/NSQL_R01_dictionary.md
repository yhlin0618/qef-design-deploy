---
id: "NSQL_R01"
title: "NSQL Dictionary Rule"
type: "rule"
date_created: "2025-04-03"
date_modified: "2025-12-24"
author: "Claude"
previous_id: "R21"
implements:
  - "MP24": "Natural SQL Language"
related_to:
  - "P13": "Language Standard Adherence Principle"
  - "MP23": "Documentation Language Preferences"
---

# NSQL_R01: NSQL Dictionary Rule

> **Note**: This rule was previously R21 in the MAMBA principles system.

## Core Requirement

The NSQL Dictionary must be maintained as a centralized, extensible reference that defines all terms, functions, and translations for the Natural SQL Language in the precision marketing system. This dictionary serves as the authoritative standard for NSQL syntax, semantics, and implementation.

## Implementation Requirements

### 1. Dictionary Structure and Content

The NSQL Dictionary must include:

1. **Structural Keywords**: transform, to, as, grouped by, where, ordered by, etc.
2. **Aggregate Functions**: sum, count, average, min, max, etc.
3. **Logical Operators**: and, or, not, etc.
4. **Comparison Operators**: equals, greater than, less than, etc.
5. **Date Functions**: date_part, date_diff, date_trunc, etc.
6. **Window Functions**: running_sum, rank, lag, lead, etc.

Each entry must include:
- Clear definition and purpose
- Translation to implementation languages (SQL, dplyr, etc.)
- Usage examples
- Restrictions or limitations
- Related terms

### 2. Dictionary Update Process

Updates to the NSQL Dictionary must follow this process:

1. **Proposal Phase**:
   - Submit formal proposal with rationale
   - Include definition, translations, and examples
   - Document potential impacts

2. **Review Phase**:
   - Assess necessity and utility
   - Verify consistency with existing terms
   - Validate translations
   - Check for ambiguity or overlap

3. **Implementation Phase**:
   - Update dictionary documentation
   - Update translation implementations
   - Create supporting examples
   - Version the changes

4. **Notification Phase**:
   - Communicate changes to stakeholders
   - Provide migration guidance if needed
   - Update related documentation

### 3. Dictionary Versioning

The NSQL Dictionary must be versioned according to:

1. **Major Version**: Incompatible changes
2. **Minor Version**: Backward-compatible additions
3. **Patch Version**: Corrections without functional change

Version history must be maintained with dates and summaries of changes.

### 4. Implementation Language Support

The NSQL Dictionary must support translations to:

1. **SQL**: Standard SQL dialect
2. **dplyr/R**: R data manipulation
3. **pandas/Python**: Python data manipulation

Additional languages may be added as needed.

## Current Dictionary State (v1.0.0)

### Structural Keywords

| Term | Description | SQL Translation | dplyr Translation | Example |
|------|-------------|----------------|-------------------|---------|
| transform | Defines the source dataset | FROM | dataset %>% | transform Sales to Summary |
| to | Defines the target dataset | INTO/CREATE TABLE | assign() | transform Sales to Summary |
| as | Defines output columns | SELECT ... AS | mutate()/summarize() | sum(revenue) as total_rev |
| grouped by | Defines grouping dimensions | GROUP BY | group_by() | grouped by region, date |
| where | Defines filtering conditions | WHERE | filter() | where date > "2025-01-01" |
| ordered by | Defines sort order | ORDER BY | arrange() | ordered by revenue desc |

### Aggregate Functions

| Function | Description | SQL Translation | dplyr Translation | Example |
|----------|-------------|----------------|-------------------|---------|
| sum(field) | Sums values in field | SUM(field) | sum(field) | sum(revenue) as total_rev |
| count(field) | Counts non-null values | COUNT(field) | sum(!is.na(field)) | count(order_id) as orders |
| count(distinct field) | Counts unique values | COUNT(DISTINCT field) | n_distinct(field) | count(distinct customer_id) |
| average(field) | Calculates average | AVG(field) | mean(field) | average(price) as avg_price |
| min(field) | Finds minimum value | MIN(field) | min(field) | min(price) as min_price |
| max(field) | Finds maximum value | MAX(field) | max(field) | max(price) as max_price |

### Logical Operators

| Operator | Description | SQL Translation | dplyr Translation | Example |
|----------|-------------|----------------|-------------------|---------|
| and | Logical AND | AND | & | region = "NA" and year = 2025 |
| or | Logical OR | OR | \| | status = "new" or status = "pending" |
| not | Logical negation | NOT | ! | not is_deleted |
| in | Value in list | IN | %in% | region in ["NA", "EU"] |

## Extension Process Example

### Adding a New Aggregate Function

To add "median" to the dictionary:

1. **Proposal**:
   ```
   Term: median
   Category: aggregate_function
   Description: Finds the median value of a field
   Parameters: field (The field to calculate median for)
   SQL Translation: PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {field})
   dplyr Translation: median({field}, na.rm = TRUE)
   Example: median(price) as med_price
   Rationale: Important for analyzing distributions where mean is skewed
   ```

2. **Review Checklist**:
   - ✓ Fills a genuine need (central tendency resistant to outliers)
   - ✓ No existing term with same meaning
   - ✓ Translations are accurate and efficient
   - ✓ Example demonstrates proper usage
   - ✓ Name follows NSQL conventions

3. **Implementation**:
   - Add to dictionary documentation
   - Update parser to recognize new function
   - Add translation rules
   - Create test cases

4. **Notification**:
   - Document in change log
   - Include in release notes
   - Update examples to demonstrate usage

## Translation Examples

### Example 1: Simple Aggregation

NSQL:
```
transform Sales to RegionalSummary as
  sum(revenue) as total_revenue,
  count(distinct customer_id) as customer_count,
  average(order_value) as avg_order
  grouped by region
  where status = "completed"
  ordered by total_revenue desc
```

SQL:
```sql
SELECT 
  region,
  SUM(revenue) AS total_revenue,
  COUNT(DISTINCT customer_id) AS customer_count,
  AVG(order_value) AS avg_order
FROM Sales
WHERE status = 'completed'
GROUP BY region
ORDER BY total_revenue DESC
```

dplyr:
```r
Sales %>%
  filter(status == "completed") %>%
  group_by(region) %>%
  summarize(
    total_revenue = sum(revenue),
    customer_count = n_distinct(customer_id),
    avg_order = mean(order_value)
  ) %>%
  arrange(desc(total_revenue))
```

### Example 2: Time-Based Analysis

NSQL:
```
transform CustomerActivity to MonthlyActivity as
  date_trunc(activity_date, "month") as month,
  count(distinct customer_id) as active_customers,
  sum(activity_count) as total_activities,
  sum(activity_count) / count(distinct customer_id) as activities_per_customer
  grouped by date_trunc(activity_date, "month")
  ordered by month desc
```

SQL:
```sql
SELECT 
  DATE_TRUNC('month', activity_date) AS month,
  COUNT(DISTINCT customer_id) AS active_customers,
  SUM(activity_count) AS total_activities,
  SUM(activity_count) / COUNT(DISTINCT customer_id) AS activities_per_customer
FROM CustomerActivity
GROUP BY DATE_TRUNC('month', activity_date)
ORDER BY month DESC
```

dplyr:
```r
CustomerActivity %>%
  mutate(month = floor_date(activity_date, "month")) %>%
  group_by(month) %>%
  summarize(
    active_customers = n_distinct(customer_id),
    total_activities = sum(activity_count),
    activities_per_customer = sum(activity_count) / n_distinct(customer_id)
  ) %>%
  arrange(desc(month))
```

## Dictionary Maintenance

### 1. Regular Review

The NSQL Dictionary must undergo:

1. **Quarterly Review**: Assess for gaps, inconsistencies, or improvements
2. **Usage Analysis**: Evaluate actual usage patterns
3. **Translation Validation**: Verify translations are optimal

### 2. Backward Compatibility

When updating the dictionary:

1. **Deprecation Process**: Mark terms as deprecated before removal
2. **Deprecation Period**: Maintain deprecated terms for at least 6 months
3. **Alternative Documentation**: Provide alternatives for deprecated terms
4. **Automated Detection**: Implement tools to detect usage of deprecated terms

### 3. Documentation Requirements

Dictionary documentation must include:

1. **Reference Documentation**: Comprehensive reference of all terms
2. **Translation Examples**: Examples of translations to all supported languages
3. **Best Practices**: Guidelines for effective NSQL usage
4. **Change Log**: Record of all changes with version numbers

## Relationship to Other Principles

### Relation to Natural SQL Language (MP24)

This rule implements MP24 by:
1. **Standardization**: Providing a concrete standard dictionary
2. **Evolution**: Establishing a process for language evolution
3. **Translation**: Enabling consistent translation to implementation languages

### Relation to Language Standard Adherence (P13)

This rule supports P13 by:
1. **Standard Definition**: Providing the reference standard for NSQL
2. **Validation**: Enabling validation against a definitive reference
3. **Consistency**: Ensuring consistent application of NSQL

## Benefits

1. **Consistency**: Creates consistent NSQL expressions across all documentation
2. **Clarity**: Reduces ambiguity through standard definitions
3. **Extensibility**: Provides a structured process for extending NSQL
4. **Automation**: Facilitates automated translation to implementation languages
5. **Learning**: Supports learning and adoption of NSQL
6. **Evolution**: Enables controlled evolution of the language

## Conclusion

The NSQL Dictionary Rule establishes a living, extensible dictionary as the authoritative reference for the Natural SQL Language. By defining a standard dictionary and a structured process for its maintenance and extension, this rule ensures that NSQL remains a valuable, consistent, and evolving tool for expressing data transformation operations in the precision marketing system.

By adhering to this rule, the system will maintain a clear, comprehensive, and up-to-date dictionary that serves as both reference and standard, supporting effective communication and implementation of data operations across the organization.