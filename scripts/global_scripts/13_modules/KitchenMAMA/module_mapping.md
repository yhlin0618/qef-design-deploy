# KitchenMAMA Module Mapping

This document provides a mapping between analytical modules, their descriptions, and the implemented functions in the KitchenMAMA precision marketing platform.

## Naming Convention

Update scripts should follow the structured naming convention:

```
AABB_C_D_E_description.R
```

Where:
- **AA**: Bundle group identifier (00-99)
- **BB**: Serial number within the bundle (00-99)
- **C**: Sub-script identifier (0-9)
- **D_E**: Module reference (e.g., 0_1)
- **description**: Brief descriptive text

For example: `0000_0_0_1_connect_databases.R` (First script in bundle 00, main script, related to module 0.1)

## Module 0: Initialization

| Module | Description | Suggested Update Script | Current Implementation | Proposed Implementation |
|--------|-------------|------------------------|-------------------|-------------------|
| 0_1 Database Connection | Connect to DuckDB databases | `0000_0_0_1_connect_databases.R` | `100g_dbConnect_from_list.R` | `fn_dbConnect_from_list.R` |
| 0_2 Database Copy Operations | Copy data between databases | `0100_0_0_2_copy_database_data.R` | `101g_dbCopyorReadTemp.R`, `102g_dbCopyTable.R` | `fn_dbCopyorReadTemp.R`, `fn_dbCopyTable.R` |
| 0_3 Database Disconnect | Safely disconnect from all databases | `0200_0_0_3_disconnect_databases.R` | `103g_dbDisconnect_all.R` | `fn_dbDisconnect_all.R` |
| 0_4 Database Overwrite | Safely overwrite database tables | `0300_0_0_4_overwrite_tables.R` | `104g_dbOverwrite.R` | `fn_dbOverwrite.R` |
| 0_5 Database Delete | Delete database files | `0400_0_0_5_delete_databases.R` | `105g_dbDeletedb.R` | `fn_dbDeletedb.R` |

## Module 1: Customer DNA

| Module | Description | Suggested Update Script | Current Implementation | Proposed Implementation |
|--------|-------------|------------------------|-------------------|-------------------|
| 1_1 DNA Calculation | Calculate customer DNA patterns | `1000_0_1_1_calculate_customer_dna.R` | `DNA_Function_dplyr2.R`, `DNA_Function_data.table.R` | `fn_customer_dna_dplyr.R`, `fn_customer_dna_data_table.R` |
| 1_2 Time Partitioning | Partition data by time periods | `1100_0_1_2_partition_by_time.R` | `Day_time_partition.R`, `Time_Recode.R` | `fn_time_partition.R`, `fn_time_recode.R` |
| 1_3 Processing by Customer | Process and aggregate by customer and time | `1200_0_1_3_process_customer_time.R` | `process_by_customer_time.R` | `sc_process_customer_time.R` |
| 1_4 Geographical Processing | Process and analyze geographical data | `1300_0_1_4_process_geo_data.R` | `StateNameTranformation.R`, `209g_process_zip.R` | `fn_state_transformation.R`, `sc_process_zip.R` |

## Module 2: Data Import & Processing

| Module | Description | Suggested Update Script | Implementation |
|--------|-------------|------------------------|----------------|
| 2_1 Amazon Sales | Import and process Amazon sales data | `2000_0_2_1_import_amazon_sales.R` | `205g_process_amazon_sales.R`, `211g_process_address_and_time_amazon_sales.R` | 
| 2_2 Website Sales | Import and process official website sales | `2100_0_2_2_import_website_sales.R` | `212g_process_address_and_time_officialwebsite_sales.R` |
| 2_3 Amazon Reviews | Process Amazon review data | `2200_0_2_3_process_amazon_reviews.R` | `203g_process_amazon_review.R`, `403g_patch_amazon_reviews.R` |
| 2_4 Competitor Analysis | Process competitor sales data | `2300_0_2_4_process_competitor_data.R` | `101g_create_or_replace_amazon_competitor_sales_dta.R` |

## Module 3: AI Integration

| Module | Description | Suggested Update Script | Implementation |
|--------|-------------|------------------------|----------------|
| 3_1 Python Setup | Set up Python environment for AI functions | `3000_0_3_1_setup_python_env.R` | `510g_setup_python.R` |
| 3_2 Review Rating | AI-based analysis of product reviews | `3100_0_3_2_analyze_reviews_ai.R` | `601g_ai_review_rating.R` |
| 3_3 Review Estimation | Estimate sentiment and features from reviews | `3200_0_3_3_estimate_review_features.R` | `603g_ai_review_ratings_estimate.R` |
| 3_4 Decoding Results | Process and decode AI analysis results | `3300_0_3_4_decode_ai_results.R` | `701g_decode_ai_review_ratings.R` |

## Module 4: Statistical Models

| Module | Description | Suggested Update Script | Implementation |
|--------|-------------|------------------------|----------------|
| 4_1 Design Matrix | Generate design matrices for statistical models | `4000_0_4_1_generate_design_matrix.R` | `Design_matrix_Generation_Poisson.R`, `Design_matrix_Generation_Poisson2.R` |
| 4_2 Poisson Models | Implement Poisson regression for sales prediction | `4100_0_4_2_run_poisson_models.R` | `Poisson_Regression.R` |
| 4_3 Optimal Pricing | Calculate optimal pricing models | `4200_0_4_3_calculate_optimal_pricing.R` | `Optimal Pricing.R`, `Sort_Optimal_Pricing_Forluma.R` |
| 4_4 Variable Selection | Perform stepwise variable selection | `4300_0_4_4_select_variables.R` | `Stepwise.R`, `stepwise_selection2.R`, `stepwise_selection3.R` |
| 4_5 Choice Models | Implement customer choice models | `4400_0_4_5_build_choice_models.R` | `choice_model_lik.R`, `choice_model_optimization.R` |

## Module 5: Shiny App Integration

| Module | Description | Suggested Update Script | Implementation |
|--------|-------------|------------------------|----------------|
| 5_1 Data Source | Configure data sources for Shiny | `5000_0_5_1_configure_shiny_data.R` | `data_source.R` in `10_rshinyapp_modules/data/` |
| 5_2 Macro Analysis | Implement macro-level analysis views | `5100_0_5_2_build_macro_views.R` | `macro_overview.R`, `sales_analysis.R` |
| 5_3 Micro Analysis | Implement customer-level analysis | `5200_0_5_3_build_micro_views.R` | `micro_customer.R` |
| 5_4 Marketing Campaign | Analyze marketing campaign effectiveness | `5300_0_5_4_analyze_campaigns.R` | `campaign_analysis.R`, `target_profiling.R` |

## Implementation Notes

This mapping is maintained by the KitchenMAMA analytics team and will be updated as new modules and implementations are added. For questions about specific implementation details, please refer to the function documentation or contact the development team.

### Update Script Naming Convention

The new naming convention `AABB_C_D_E_description.R` for update scripts provides these benefits:
1. Scripts will appear in correct execution order in file listings
2. Bundle groupings keep related scripts together
3. Module references ensure traceability to documentation
4. Sub-script capabilities allow complex processes to be broken down
5. Clear, structured naming improves maintainability

To implement, existing scripts should be gradually renamed following this pattern during regular maintenance cycles or when making significant changes.

### Global Script Naming Convention

Global scripts should use descriptive names with prefixes that distinguish between function libraries, execution scripts, and Shiny modules:

```
[prefix]_[name].R
```

Where the prefix indicates the file type:
- `fn_` - Function libraries that contain reusable functions
- `sc_` - Scripts that execute processes or workflows
- `ui_` - Shiny UI module components
- `server_` - Shiny server module components

#### Function Library Rules

Function libraries (`fn_` files) must follow the "one function per file" principle:

```
fn_[function_name].R  # Contains function_name() as its primary export
```

1. Each file should export exactly one primary function
2. The filename should match the primary exported function name
3. Helper functions can be included but should be internal (not exported)

For example, a file named `fn_dbConnect_from_list.R` should contain and export only the `dbConnect_from_list()` function. This ensures clear one-to-one mapping between files and their exported functions.

#### Shiny Module Rules

Shiny modules should be split into UI and server files:

```
ui_[module_name].R      # Contains the UI component function
server_[module_name].R  # Contains the server component function
```

Each file should export a single function following Shiny module conventions.

#### General Naming Rules

Follow these rules for all global scripts:
1. Use lowercase with underscores for word separation
2. Name should clearly describe the function's purpose
3. Replace the "g" suffix and numeric prefixes (like "100g_") with the appropriate prefix
4. Maintain the domain-based directory structure
5. For function libraries, the name after the prefix should match the exported function name

For example:
- Function libraries:
  - `fn_dbConnect_from_list.R` instead of `100g_dbConnect_from_list.R` (contains `dbConnect_from_list()`)
  - `fn_dbCopyTable.R` instead of `102g_dbCopyTable.R` (contains `dbCopyTable()`)

- Execution scripts:
  - `sc_process_amazon_sales.R` instead of `205g_process_amazon_sales.R`
  - `sc_import_website_sales.R` instead of `300g_import_website_km_sales.R`

This approach:
1. Clarifies the purpose of each file at a glance
2. Creates a clear one-to-one mapping between function files and their exported functions
3. Maintains organization within directories
4. Aligns with standard R package development practices
5. Improves code clarity and maintainability

Last Updated: April 1, 2025 (Updated to include fn/sc prefixes)