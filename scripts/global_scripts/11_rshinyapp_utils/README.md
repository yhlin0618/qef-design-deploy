# Shiny Application Utility Functions

This directory contains utility functions specifically designed for use in the Precision Marketing KitchenMAMA Shiny application. Each file contains a single utility function with thorough documentation.

## Structure

- One function per file
- File names match function names
- Each file includes roxygen-style documentation

## Usage

You can either source individual utility functions:

```r
source("./scripts/global_scripts/rshinyapp_utils/formattime.R")
source("./scripts/global_scripts/rshinyapp_utils/safe_get.R")
```

Or load all utilities at once using the index file:

```r
source("./scripts/global_scripts/rshinyapp_utils/utils.R")
```

## Available Utilities

The following utility functions are available:

### Data Retrieval

- `safe_get.R` - Safely load RDS files with error handling
- `getDynamicOptions.R` - Get unique values from a data frame based on filters
- `CreateChoices.R` - Create a list of unique values from a data frame column

### Data Formatting

- `formattime.R` - Format date objects based on time scale (year, quarter, month)
- `make_names.R` - Clean column names for R compatibility
- `clean_column_names_remove_english.R` - Remove English text from Chinese column names

### Data Processing

- `process_sales_data.R` - Process and summarize sales data by time interval
- `remove_elements.R` - Remove specified elements from a vector
- `Recode_time_TraceBack.R` - Convert time scales for historical comparisons

### Deployment

- `fn_deploy_shiny_app.R` - Deploy a Shiny application to shinyapps.io

## Relationship to Other Utilities

These utilities are specifically designed for the Shiny application and differ from the general utilities in the `utils/` directory:

1. They focus on UI-specific data transformation
2. They may have dependencies on Shiny-related packages
3. They are optimized for reactive contexts

## Naming Conventions

Function names follow mixed conventions:
- Most use `lower_case_with_underscores` format
- Some use `camelCase` for historical reasons
- New functions should use the `lower_case_with_underscores` convention

## Adding New Functions

When adding a new utility function:

1. Create a new file named after the function
2. Include thorough roxygen documentation
3. Keep functions small and focused on a single task
4. Add examples to demonstrate usage
5. Update this README to include the new function
