---
id: "NSQL_R08"
title: "Index Variability Theory"
type: "rule"
date_created: "2025-04-03"
date_modified: "2025-12-24"
author: "Claude"
previous_id: "R63"
---

# NSQL_R08: Index Variability Theory

> **Note**: This rule was previously R63 in the MAMBA principles system.

## Definition
Index Variability Theory is a formal system for classifying and reasoning about how values change across different dimensions using precise index notation. Each value is explicitly indexed by its variability dimensions, allowing for rigorous analysis of value persistence, scope, and changeability.

## Formal Index Notation

Value expressions are written in the form:

V[i₁, i₂, ..., iₙ]

Where:
- V is the value or variable name
- [i₁, i₂, ..., iₙ] are the indices representing dimensions of variability
- The absence of an index on a dimension implies constancy across that dimension

## Core Variability Dimensions

### 1. Constants (No Indices)

**Notation**: `V`

**Definition**: Values that remain fixed across all possible dimensions and contexts.

**Example**:
```
π = 3.14159...  # Mathematical constant, invariant across all dimensions
```

### 2. Company-Indexed Values

**Notation**: `V[company]`

**Definition**: Values that vary by company but remain fixed within a company context.

**Example**:
```
BrandColor[KitchenMAMA] = "#00A5E3"
BrandColor[WISER] = "#7700FF"
```

### 3. Config-Indexed Values

**Notation**: `V[config]`

**Definition**: Values that vary by configuration but remain fixed for a given configuration.

**Example**:
```
MaxResults[development] = 100
MaxResults[production] = 20
```

### 4. Config-Company-Indexed Values

**Notation**: `V[config, company]`

**Definition**: Values that vary by both configuration and company.

**Example**:
```
ApiEndpoint[development, KitchenMAMA] = "https://dev-api.kitchenmama.com"
ApiEndpoint[production, KitchenMAMA] = "https://api.kitchenmama.com"
ApiEndpoint[development, WISER] = "https://dev-api.wiser.com"
```

### 5. User-Indexed Values

**Notation**: `V[user]`

**Definition**: Values that vary by user but remain constant for a given user.

**Example**:
```
LanguagePreference[user123] = "en_US"
LanguagePreference[user456] = "zh_TW"
```

### 6. Time-Indexed Values

**Notation**: `V[t]`

**Definition**: Values that vary over time.

**Example**:
```
ExchangeRate[2023-04-07] = 1.08
ExchangeRate[2023-04-08] = 1.09
```

### 7. Multi-Dimensional Indices

**Notation**: `V[i₁, i₂, ..., iₙ]`

**Definition**: Values that vary across multiple dimensions simultaneously.

**Example**:
```
UserPreferences[user123, device="mobile"] = {compact: true, darkMode: true}
UserPreferences[user123, device="desktop"] = {compact: false, darkMode: false}
```

## Index Operations

### 1. Index Fixing

When an index is fixed to a specific value, that dimension of variability is eliminated:

```
# Original multi-dimensional value
ApiEndpoint[config, company]

# Fixed company dimension
ApiEndpoint[config, company=KitchenMAMA]

# Fixed both dimensions
ApiEndpoint[config=production, company=KitchenMAMA] = "https://api.kitchenmama.com"
```

### 2. Index Quantification

Expressing properties that hold across all values of an index:

```
# A property that holds for all companies
∀company: MaxUsers[company] > 0

# A property that holds for at least one configuration
∃config: DebugMode[config] = true
```

### 3. Index Dependency

When one index depends on another:

```
# The language depends on both user and company policy
Language[user, company] = 
  if (UserPreference[user].hasLanguage) 
    then UserPreference[user].language 
    else DefaultLanguage[company]
```

## Application in Code

### Variable Declarations with Index Types

```r
# Declare a constant
declare_variable("PI", index_type = "constant", value = 3.14159)

# Declare a company-indexed value
declare_variable("BRAND_COLOR", index_type = "company", 
                default_value = "#000000")

# Declare a config-company-indexed value
declare_variable("API_ENDPOINT", index_type = c("config", "company"),
                default_value = "https://api.default.com")
```

### Value Access

```r
# Access a value with explicit indices
get_value("API_ENDPOINT", config = "production", company = "KitchenMAMA")

# Access with current context (implicit indices)
get_value("BRAND_COLOR")  # Uses current company context
```

### Index Validation

```r
# Validate that a variable has certain indices
validate_indices("USER_PREFERENCES", required_indices = c("user", "device"))

# Validate an index assignment is valid
validate_index_assignment("API_TIMEOUT", 
                         index_values = list(config = "production"),
                         value = 30)
```

## Implications for System Design

### 1. Config File Organization

Config files should be organized by index dimensions:

```yaml
# Constants (global.yaml)
constants:
  MAX_RETRIES: 5
  TIMEOUT_SECONDS: 30

# Company-indexed values (company_values.yaml)
companies:
  KitchenMAMA:
    BRAND_COLOR: "#00A5E3"
    DEFAULT_CURRENCY: "USD"
  WISER:
    BRAND_COLOR: "#7700FF"
    DEFAULT_CURRENCY: "EUR"

# Config-indexed values (configurations.yaml)
configs:
  development:
    LOG_LEVEL: "DEBUG"
    MOCK_SERVICES: true
  production:
    LOG_LEVEL: "ERROR"
    MOCK_SERVICES: false
```

### 2. UI Component Variability

UI components should declare their index dependencies:

```r
# Component with explicit index dependencies
register_component("BrandHeader", 
                  index_dependencies = c("company"),
                  render_function = function(company) {
                    div(
                      style = paste0("background-color: ", BRAND_COLOR[company]),
                      img(src = LOGO_PATH[company])
                    )
                  })

# Component with multi-dimensional index dependencies
register_component("UserDashboard",
                  index_dependencies = c("user", "config", "company"),
                  render_function = function(user, config, company) {
                    # Render based on all indices
                  })
```

### 3. Change Impact Analysis

Index theory enables precise impact analysis for changes:

```r
# Analyze impact of changing a value
analyze_change_impact("API_ENDPOINT", 
                     indices = list(config = "production", company = "KitchenMAMA"),
                     new_value = "https://new-api.kitchenmama.com")
# Result: Affects only KitchenMAMA users in production environment
```

## Examples in NSQL

### Querying with Index Awareness

```nsql
SELECT 
  product_name,
  sales_amount,
  sales_amount * ExchangeRate[current_date, Currency[company]]
FROM sales
WHERE region IN AuthorizedRegions[user]
LIMIT MaxResults[config]
```

### Transformations with Index Constraints

```nsql
# Transform that explicitly specifies which indices to use
TRANSFORM Sales 
USING RollupMethod[company]
WITH {
  dimensions: GroupingDimensions[user, report_type],
  filters: CurrentFilters[user, session],
  limit: ResultLimit[config, company]
}
```

## Mathematical Foundation

Index Variability Theory can be formalized using partial functions and dependent type theory:

1. A value with indices i₁, i₂, ..., iₙ is a partial function:
   V : I₁ × I₂ × ... × Iₙ → ValueDomain

2. Fixing an index corresponds to partial application:
   V[i₁=v₁] : I₂ × ... × Iₙ → ValueDomain

3. Index dependencies can be expressed through dependent types:
   V : (i₁:I₁) → (i₂:I₂(i₁)) → ... → ValueDomain

## Relationship to Other Principles

1. **R61 (Extensionality)**: Two indexed values are equal if they yield the same results for all possible index values
2. **R59 (Component Effect Propagation)**: Changes to an indexed value propagate to all components that depend on that specific index combination
3. **MP41 (Configuration-Driven UI)**: Clarifies exactly how UI depends on configuration through precise indexing

## Related Principles and Rules

- MP41: Configuration-Driven UI Composition
- R59: Component Effect Propagation
- R61: NSQL Extensionality Principle 
- R62: NSQL Similarity Principle