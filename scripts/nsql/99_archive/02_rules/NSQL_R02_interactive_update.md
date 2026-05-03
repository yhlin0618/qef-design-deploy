---
id: "NSQL_R02"
title: "NSQL Interactive Update Rule"
type: "rule"
date_created: "2025-04-03"
date_modified: "2025-12-24"
author: "Claude"
previous_id: "R22"
implements:
  - "MP24": "Natural SQL Language"
related_to:
  - "NSQL_R01": "NSQL Dictionary Rule"
  - "P13": "Language Standard Adherence Principle"
---

# NSQL_R02: NSQL Interactive Update Rule

> **Note**: This rule was previously R22 in the MAMBA principles system.

## Core Requirement

When a user identifies a statement as potential NSQL, AI systems must analyze the statement, disambiguate it if necessary, and if unambiguous, add this usage pattern to the NSQL dictionary. This process ensures NSQL evolves based on natural usage patterns while maintaining precision.

## Implementation Requirements

### 1. User-Initiated NSQL Identification

When a user identifies a statement as potential NSQL:

1. **Explicit Identification**: The user must explicitly indicate the statement is intended as NSQL
2. **Statement Isolation**: The statement must be clearly isolated from surrounding text
3. **Intent Declaration**: The user should indicate whether they want the statement evaluated for addition to the dictionary

### 2. AI Ambiguity Analysis

Upon receiving a potential NSQL statement, AI systems must:

1. **Parse Statement**: Analyze the statement structure
2. **Identify Ambiguities**: Determine if the statement has multiple possible interpretations
3. **Reference Dictionary**: Check if similar patterns exist in the current NSQL dictionary
4. **Assess Translatability**: Determine if the statement can be systematically translated to implementation languages

### 3. Disambiguation Process

If ambiguities are found, the AI must engage in disambiguation:

1. **Identify Specific Ambiguities**: Clearly communicate which parts of the statement are ambiguous
2. **Provide Options**: Offer alternative interpretations
3. **Suggest Clarifications**: Recommend specific clarifications to resolve ambiguities
4. **Learn from Resolution**: Record how ambiguities were resolved for future reference

### 4. Unambiguous Statement Processing

For statements determined to be unambiguous:

1. **Translate to Standard NSQL**: Convert to formal NSQL if in a non-standard pattern
2. **Generate Implementations**: Produce translations to SQL, dplyr, etc.
3. **Verify Correctness**: Ensure the translations maintain the original intent
4. **Document Usage Pattern**: Record the pattern with its translations

### 5. Dictionary Update Decision

To determine if a statement should be added to the dictionary:

1. **Novelty Assessment**: Determine if the pattern represents a new usage not in the dictionary
2. **Utility Evaluation**: Assess if the pattern has broad applicability
3. **Consistency Check**: Ensure the pattern aligns with existing NSQL principles
4. **Formal Proposal**: Generate a formal dictionary addition proposal

### 6. User Feedback Integration

Throughout the process, user feedback must be integrated:

1. **Interpretation Confirmation**: Confirm the interpretation matches user intent
2. **Translation Validation**: Verify translations represent the intended operation
3. **Usage Context**: Gather information about the contexts where this pattern is useful
4. **Preference Tracking**: Note user preferences for specific expression patterns

## Disambiguation Examples

### Example 1: Ambiguous Time Reference

**User Statement**:
```
show me sales by region for last quarter
```

**Ambiguity Analysis**:
1. "Last quarter" is ambiguous:
   - Calendar quarter (Q1, Q2, etc.)?
   - Fiscal quarter?
   - Rolling 3-month period?
2. "Sales" is ambiguous:
   - Total revenue?
   - Unit sales?
   - Net sales after returns?

**Disambiguation Process**:
```
This NSQL statement contains ambiguities:

1. "Last quarter" could mean:
   - Calendar quarter (Jan-Mar, Apr-Jun, etc.)
   - Fiscal quarter
   - Previous 3 months from today

2. "Sales" could mean:
   - Total revenue
   - Number of units sold
   - Net sales (after returns/discounts)

Suggested clarification:
"show me total revenue by region for calendar Q1 2025"
```

### Example 2: Unambiguous Statement

**User Statement**:
```
calculate average order value by customer segment for active customers
```

**Ambiguity Analysis**:
- "Average order value" is a clear metric
- "Customer segment" refers to a specific dimension
- "Active customers" has a standard definition in the system

**Processing**:
```
This statement is unambiguous and can be formalized as NSQL:

transform Orders to SegmentAnalysis as
  average(order_value) as avg_order_value
  grouped by customer_segment
  where customer_status = "active"
```

## Dictionary Update Examples

### Example 1: Adding a New Query Pattern

**User Statement**:
```
show top 5 products by revenue for northeast region
```

**Analysis**: This statement uses a "show [top N] [dimension] by [metric] for [filter]" pattern that isn't in the dictionary.

**Dictionary Update Proposal**:
```
Pattern: "show [top N] [dimension] by [metric] for [filter]"

Translation:
transform [implicit_table] to Results as
  [dimension],
  [metric] as [metric_name]
  where [filter_field] = [filter_value]
  ordered by [metric_name] desc
  limit [N]

Example:
"show top 5 products by revenue for northeast region"

Translates to:
transform Sales to Results as
  product,
  sum(revenue) as total_revenue
  where region = "northeast"
  ordered by total_revenue desc
  limit 5
```

### Example 2: Adding a Time-Based Pattern

**User Statement**:
```
compare monthly sales vs last year
```

**Analysis**: This statement introduces a year-over-year comparison pattern not currently in the dictionary.

**Dictionary Update Proposal**:
```
Pattern: "compare [time_period] [metric] vs last year"

Translation:
transform [implicit_table] to YoyComparison as
  date_trunc(date, "[time_period]") as period,
  sum([metric]) as current_value,
  sum([metric]) over (date between date_add(period, -1, "year") and date_add(period, -1, "year") + interval 1 [time_period]) as previous_value,
  (current_value - previous_value) / previous_value as percent_change
  grouped by date_trunc(date, "[time_period]")
  ordered by period

Example:
"compare monthly sales vs last year"

Translates to:
transform Sales to YoyComparison as
  date_trunc(date, "month") as month,
  sum(revenue) as current_month_sales,
  sum(revenue) over (date between date_add(month, -1, "year") and date_add(month, -1, "year") + interval 1 month) as previous_year_sales,
  (current_month_sales - previous_year_sales) / previous_year_sales as percent_change
  grouped by date_trunc(date, "month")
  ordered by month
```

## Implementation Process

### 1. Initial User Interaction

When a user identifies a potential NSQL statement:

1. **Explicit Marker**: User marks the statement with a tag like "NSQL:" or "[NSQL]"
2. **Optional Directive**: User may add directives like "analyze", "disambiguate", or "add to dictionary"
3. **System Acknowledgment**: System acknowledges receipt of potential NSQL statement

### 2. Ambiguity Resolution Workflow

The ambiguity resolution follows this workflow:

1. **Initial Analysis**: System analyzes statement and reports ambiguity level (none, low, medium, high)
2. **Ambiguity Details**: System explains specific ambiguities if any
3. **Resolution Options**: System provides options to resolve each ambiguity
4. **User Selection**: User selects preferred interpretation or provides clarification
5. **Verification**: System presents final interpretation for confirmation
6. **Translation**: System produces translations to implementation languages

### 3. Dictionary Update Workflow

The dictionary update process follows this workflow:

1. **Pattern Extraction**: System extracts the general pattern from the specific statement
2. **Pattern Analysis**: System compares with existing patterns in dictionary
3. **Novelty Assessment**: System determines if pattern is novel enough to warrant addition
4. **Translation Rules**: System generates rules for translating pattern to implementation languages
5. **Formal Proposal**: System generates formal dictionary addition proposal
6. **Governance Process**: Proposal enters the formal governance process defined in R21

### 4. User Feedback Loop

Throughout the process, user feedback is incorporated:

1. **Interpretation Feedback**: Users confirm or correct interpretations
2. **Translation Feedback**: Users validate translations
3. **Pattern Refinement**: Users suggest refinements to extracted patterns
4. **Usage Documentation**: Users provide context for when/how pattern should be used

## Integration with NSQL Dictionary Rule (R21)

This rule complements R21 (NSQL Dictionary Rule) by:

1. **Interactive Approach**: Adding an interactive path to dictionary updates
2. **User-Driven Evolution**: Allowing evolution based on actual usage patterns
3. **Ambiguity Management**: Providing a framework for managing ambiguity
4. **Pattern Discovery**: Enabling discovery of natural expression patterns

Updates to the dictionary through this process must still:
1. Follow the formal proposal structure defined in R21
2. Undergo the review process defined in R21
3. Be properly documented and versioned
4. Be communicated to users

## Natural Expression Patterns

The following types of natural expressions should be considered as potential NSQL patterns:

### 1. Imperative Queries

- "show [dimension] by [metric]"
- "calculate [metric] for [filter]"
- "compare [dimension1] vs [dimension2] by [metric]"
- "rank [dimension] by [metric]"
- "find top [N] [dimension] by [metric]"

### 2. Time-Based Queries

- "trend of [metric] over [time_period]"
- "compare [metric] this [time_period] vs last [time_period]"
- "year-to-date [metric] by [dimension]"
- "forecast [metric] for next [time_period]"

### 3. Comparative Queries

- "[metric] comparison between [dimension1] and [dimension2]"
- "difference in [metric] between [value1] and [value2]"
- "percentage of [metric] by [dimension]"
- "contribution of [dimension] to total [metric]"

### 4. Analysis Queries

- "segment [dimension] by [metric] threshold"
- "correlate [metric1] with [metric2]"
- "distribution of [metric] across [dimension]"
- "outliers in [dimension] by [metric]"

## Relationship to Other Principles

### Relation to Natural SQL Language (MP24)

This rule extends MP24 by:
1. **User-Driven Evolution**: Enabling NSQL to evolve based on actual usage
2. **Natural Patterns**: Formally incorporating natural language patterns into NSQL
3. **Ambiguity Management**: Adding a framework for resolving ambiguities
4. **Interactive Clarification**: Supporting interactive refinement of expressions

### Relation to NSQL Dictionary Rule (R21)

This rule complements R21 by:
1. **Alternative Update Path**: Providing an interactive path to dictionary updates
2. **Pattern Discovery**: Supporting discovery of useful expression patterns
3. **User Validation**: Incorporating user validation in the update process
4. **Ambiguity Resolution**: Adding a process for resolving ambiguities

## Benefits

1. **User-Centered Evolution**: Allows NSQL to evolve based on actual user preferences
2. **Natural Expression**: Encourages more natural expression patterns
3. **Ambiguity Management**: Provides a framework for managing and resolving ambiguities
4. **Learning System**: Creates a learning system that improves over time
5. **Documentation**: Automatically documents common query patterns
6. **Standardization**: Standardizes natural expressions without artificial constraints
7. **Accessibility**: Makes NSQL more accessible to non-technical users

## Conclusion

The NSQL Interactive Update Rule establishes a process for analyzing, disambiguating, and incorporating user-identified NSQL statements into the NSQL dictionary. By enabling NSQL to evolve based on actual usage patterns while maintaining precision and consistency, this rule makes NSQL more accessible and natural while preserving its formal benefits.

This approach recognizes that the boundary between natural language and formal query languages is fluid, and that user-preferred expression patterns can enhance the utility and adoption of NSQL. Through structured ambiguity resolution and pattern extraction, this rule ensures that NSQL remains both natural and precise, optimizing for both human readability and machine interpretability.