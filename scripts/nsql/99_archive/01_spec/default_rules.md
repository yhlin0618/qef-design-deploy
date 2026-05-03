# NSQL Default Rules

While NSQL requires unambiguous references, these default rules provide standardized interpretations for specific patterns. These defaults are explicit, documented, and consistent to ensure predictability without introducing ambiguity.

## Import Operation Defaults

### Import Source Typing

When using type annotations, the following defaults apply:

```
import A:directory to destination
```

**Default Behavior**: All files in directory A are imported without duplication or modification to `destination.A`

**Translation**:
```sql
-- For each file f in directory A:
CREATE TABLE IF NOT EXISTS destination.A.f AS
SELECT * FROM external_file('A/f')
```

### Import Destination Inheritance

When importing to a destination without sub-path:

```
import source to destination
```

**Default Behavior**: Creates a sub-container in destination named after the source base name

**Example**:
```
import customer_data.csv to raw_data
```

Is equivalent to:
```
import customer_data.csv to raw_data.customer_data
```

### Import Transformation Defaults

When no transformation is specified:

**Default Behavior**: Data is imported as-is with minimal modification required only for compatibility

Specifically:
- Data types are preserved when possible
- Only required conversions for compatibility are performed
- No deduplication, filtering, or aggregation is applied
- Original column names are preserved unless they conflict with system requirements

## Query Pattern Defaults

### Default Time Ranges

When a time period is mentioned without specific bounds:

```
show sales for [time-period]
```

| Term | Default Range |
|------|---------------|
| `today` | 00:00:00 to 23:59:59 of current day |
| `yesterday` | 00:00:00 to 23:59:59 of previous day |
| `this week` | Monday 00:00:00 to Sunday 23:59:59 of current week |
| `last week` | Monday 00:00:00 to Sunday 23:59:59 of previous week |
| `this month` | 1st 00:00:00 to last day 23:59:59 of current month |
| `last month` | 1st 00:00:00 to last day 23:59:59 of previous month |
| `this quarter` | First day 00:00:00 to last day 23:59:59 of current quarter |
| `last quarter` | First day 00:00:00 to last day 23:59:59 of previous quarter |
| `this year` | Jan 1 00:00:00 to Dec 31 23:59:59 of current year |
| `last year` | Jan 1 00:00:00 to Dec 31 23:59:59 of previous year |

**Example**:
```
show sales for this month
```

Is equivalent to:
```
show sales where date >= first_day_of_current_month and date <= last_day_of_current_month
```

### Default Metrics

When a general metric category is referenced without specification:

| General Term | Default Metric |
|--------------|----------------|
| `sales` | Sum of monetary transaction values |
| `revenue` | Sum of monetary transaction values |
| `customers` | Count of distinct customer IDs |
| `orders` | Count of distinct order IDs |
| `products` | Count of distinct product IDs |

**Example**:
```
show sales by region
```

Is equivalent to:
```
transform Transactions to Results as
  sum(amount) as sales
  grouped by region
```

### Default Grouping

When a grouping dimension is not specified, but aggregation is implied:

**Default Behavior**: No grouping is applied, and the aggregation is performed on the entire dataset

**Example**:
```
calculate average order value
```

Is equivalent to:
```
transform Orders to Results as
  avg(total_amount) as average_order_value
```

## Working Context Commands

### Set Working Directory

The `setwd` command establishes the current working directory context:

```
setwd /path/to/directory
```

**Default Behavior**: Sets the current working context for file operations and relative path resolution

**Example**:
```
setwd /data/customer_files/
import transactions.csv to raw_data
```

Is equivalent to:
```
import /data/customer_files/transactions.csv to raw_data
```

If path ambiguity exists, the system will prompt:
```
What directory are you looking at?
1. /data/customer_files/ (current user directory)
2. /shared/customer_files/ (shared directory)
3. Specify another path...
```

### Use Schema

The `use schema` command establishes the current schema context:

```
use schema Sales
```

**Default Behavior**: Sets the current schema context for table resolution

## Reference Resolution Defaults

### Schema Resolution

When a table is referenced without schema qualification:

**Default Behavior**: System will look in the following order, but will still prompt if multiple matches are found:
1. Current working schema (set by `use schema X`)
2. User's default schema
3. Public schema

**Example**:
If current schema is "Sales":
```
transform Orders to OrderSummary
```

Is equivalent to:
```
transform Sales.Orders to OrderSummary
```

### Column Resolution

When a column is referenced that exists in multiple joined tables:

**Default Behavior**: The system MUST still prompt for clarification, as this ambiguity cannot have a safe default

## Date Format Defaults

When dates are provided as strings:

**Default Behavior**: ISO format (YYYY-MM-DD) is assumed

**Example**:
```
show sales where date >= "2025-01-01"
```

## Parenthetical Clarifications

Parentheses can be used for clarification without changing the meaning:

```
import A:folder to raw_data (without modification, preserving original structure)
```

**Default Behavior**: Parenthetical notes serve as documentation only and don't affect execution

## Implementation Notes

1. These defaults provide predictable behaviors but DO NOT override the requirement to resolve ambiguity
2. If a reference matches multiple objects, the system MUST still prompt for clarification
3. Default rules apply only after reference resolution is complete
4. Documentation should clearly indicate when defaults are being applied

## Examples with Default Rules Applied

### Import Examples

```
import customer_data.csv to raw_data
```

Resolved to:
```
import customer_data.csv to raw_data.customer_data
```

Translation:
```sql
CREATE TABLE IF NOT EXISTS raw_data.customer_data AS
SELECT * FROM external_file('customer_data.csv')
```

### Query Examples

```
show sales by region for last quarter
```

Resolved to:
```
transform Transactions to Results as
  sum(amount) as sales
  grouped by region
  where date >= '2025-01-01' and date <= '2025-03-31'
```

Translation:
```sql
SELECT
  region,
  SUM(amount) AS sales
FROM Transactions
WHERE date >= '2025-01-01' AND date <= '2025-03-31'
GROUP BY region
```