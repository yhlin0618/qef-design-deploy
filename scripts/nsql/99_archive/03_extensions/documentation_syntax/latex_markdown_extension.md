# NSQL Documentation Syntax Extension

## Overview

This extension enhances NSQL with structured documentation syntax inspired by LaTeX, Markdown, and Roxygen2. It provides a comprehensive framework for documenting code, data flows, and component structures with clear hierarchical organization and metadata.

## LaTeX-Inspired Document Structure Elements

| NSQL Term | LaTeX Equivalent | Description | Usage |
|-----------|------------------|-------------|-------|
| `####name####` | `\section{name}` | Defines a major section in a script | Used to delineate function definitions |
| `###name###` | `\subsection{name}` | Defines a sub-section in a script | Used for components within functions |
| `##name##` | `\subsubsection{name}` | Defines a tertiary section | Used for detailed components |
| `#~name~#` | `\paragraph{name}` | Defines a minor section or block | Used for specific blocks of code |
| `%% name %%` | `\comment{name}` | Structured comment block | Used for extended documentation |

## Document Metadata

| NSQL Term | LaTeX Equivalent | Description | Usage |
|-----------|------------------|-------------|-------|
| `@file` | `\documentclass` | Defines the file type | Used at the top of files |
| `@principle` | `\usepackage` | References principles applied | Lists principles implemented |
| `@author` | `\author` | Specifies the author | Identifies creator/modifier |
| `@date` | `\date` | Creation date | When the file was created |
| `@modified` | `\versiondate` | Modification date | When the file was modified |
| `@related_to` | `\input` or `\include` | References related files | Identifies related files |

## Environment Blocks

| NSQL Term | LaTeX Equivalent | Description | Usage |
|-----------|------------------|-------------|-------|
| `DATA_FLOW {...}` | `\begin{environment}...\end{environment}` | Data flow documentation block | Documents data flows |
| `FUNCTION_DEP {...}` | `\begin{environment}...\end{environment}` | Function dependency block | Documents dependencies |
| `TEST_CASE {...}` | `\begin{environment}...\end{environment}` | Test case definition block | Defines test cases |
| `EXAMPLE {...}` | `\begin{environment}...\end{environment}` | Example usage block | Shows usage examples |
| `VALIDATION {...}` | `\begin{environment}...\end{environment}` | Input validation block | Defines validation rules |

## Markdown-Inspired Elements

| NSQL Term | Markdown Equivalent | Description | Usage |
|-----------|---------------------|-------------|-------|
| `# Title` | `# Heading 1` | Primary heading | File title and divisions |
| `## Subtitle` | `## Heading 2` | Secondary heading | Major sections |
| `- Item` | `- Bullet point` | Unordered list item | Listing items |
| `1. Step` | `1. Numbered item` | Ordered list item | Sequential steps |
| `> Note` | `> Blockquote` | Note or callout | Important notes |
| `` `code` `` | `` `inline code` `` | Inline code | Short code snippets |
| `**Important**` | `**Bold**` | Emphasis | Important terms |
| `*Variable*` | `*Italic*` | Variable or term | Parameter names |

## Roxygen2-Inspired Annotations

### Standard Documentation Tags

| NSQL Term | Roxygen2 Equivalent | Description | Usage |
|-----------|---------------------|-------------|-------|
| `@title` | `@title` | Function title | Defines function title |
| `@description` | `@description` | Brief description | Describes function purpose |
| `@details` | `@details` | Detailed information | In-depth information |
| `@param` | `@param` | Parameter documentation | Documents a parameter |
| `@return` | `@return` | Return value | Documents return value |
| `@export` | `@export` | Export flag | Indicates export |
| `@examples` | `@examples` | Usage examples | Provides examples |

### Extended Documentation Tags

| NSQL Term | Description | Usage |
|-----------|-------------|-------|
| `@implements` | Principle implementation | Indicates implemented principles |
| `@requires` | Dependencies | Lists required packages or functions |
| `@throws` | Error conditions | Documents conditions causing errors |
| `@performance` | Performance notes | Documents performance characteristics |
| `@validation` | Input validation | Documents validation procedures |

## Example Usage

```r
#' @file fn_analyze_customer_cohort.R
#' @principle R067 Functional Encapsulation
#' @principle P077 Performance Optimization
#' @author Analytics Team
#' @date 2025-04-12
#' @modified 2025-04-15
#' @related_to fn_customer_segmentation.R

# Load required libraries
library(dplyr)
library(tidyr)

####extract_cohort_date####

#' Extract Cohort Date from Customer Data
#'
#' Extracts the cohort date (first purchase date) for each customer.
#'
#' @param transaction_data Transaction data frame
#' @return Data frame with customer_id and cohort_date
#'
extract_cohort_date <- function(transaction_data) {
  ###prepare_data###
  
  # Implementation...
  
  return(cohort_dates)
}

####compute_retention_rates####

#' Compute Retention Rates by Cohort
#'
#' @param transaction_data Transaction data with cohort dates
#' @return Retention rates matrix
#'
#' TEST: "Retention Computation Validation" {
#'   INPUTS: {
#'     transaction_data = sample_transactions
#'   }
#'   EXPECT: all(result$retention_rate >= 0, result$retention_rate <= 1)
#'   EXPECT: all(result$cohort_size > 0)
#' }
#'
compute_retention_rates <- function(transaction_data) {
  ###prepare_data###
  
  # Data preparation steps...
  
  ###calculate_rates###
  
  # Rate calculation steps...
  
  return(retention_matrix)
}

####analyze_customer_cohort####

#' Analyze Customer Cohort Retention
#'
#' Performs cohort analysis on customer transaction data.
#'
#' @param customer_data Customer transaction data
#' @param cohort_period Period for cohort grouping
#' @return Cohort analysis results
#' @export
#'
#' VALIDATION {
#'   REQUIRE: customer_data is data.frame
#'   REQUIRE: contains(customer_data, ["customer_id", "transaction_date", "amount"])
#'   REQUIRE: cohort_period in ["day", "week", "month", "quarter", "year"]
#' }
#'
#' FUNCTION_DEP {
#'   MAIN: analyze_customer_cohort
#'   AUXILIARIES: [
#'     extract_cohort_date,
#'     compute_retention_rates
#'   ]
#'   CALL_GRAPH: {
#'     analyze_customer_cohort → extract_cohort_date
#'     analyze_customer_cohort → compute_retention_rates
#'   }
#' }
#'
analyze_customer_cohort <- function(customer_data, cohort_period = "month") {
  # Implementation...
}

# [EOF]
```

## Benefits of Documentation Syntax

1. **Hierarchical Organization**: Creates clear structural hierarchy for improved readability
2. **Enhanced Navigability**: Facilitates easy navigation through consistent section markers
3. **Standard Metadata**: Provides consistent format for file and function metadata
4. **Structured Validation**: Defines clear validation expectations for functions
5. **Function Dependencies**: Documents relationships between functions and components
6. **Integrated Testing**: Embeds test cases directly with function definitions
7. **Integration with Tools**: Enables tooling support for navigation and documentation generation

## Implementation Mapping

The documentation syntax elements can be used with various implementation approaches:

| Element | R Implementation | Python Implementation | JavaScript Implementation |
|---------|------------------|----------------------|---------------------------|
| Section Markers | roxygen/comment blocks | docstrings/comment blocks | JSDoc/comment blocks |
| Function Dependencies | roxygen + custom parser | dependency decorators | module exports/imports |
| Test Cases | testthat integration | pytest fixtures | Jest test blocks |
| Validation | assertthat integration | type hints + validation | PropTypes/TypeScript |

## Related Principles

- MP024: Natural SQL Language
- MP025: AI Communication Meta-Language
- MP026: R Statistical Query Language
- MP027: Integrated Natural SQL Language (NSQL)
- P090: Documentation Standards
- R095: Code Comments Rule