# Precision Marketing Global Scripts

This directory contains the utility scripts, functions, and principles for the Precision Marketing projects, organized according to the MP-P-R (Meta-Principles, Principles, Rules) framework.

## Directory Structure

The project follows a numbered directory structure for clear organization and dependency management:

- **00_principles/** - Axiomatic system of principles and rules
  - **MP** - Meta-Principles (governing principles)
  - **P** - Principles (implementation guidelines)
  - **R** - Rules (specific implementation details)
  - **M** - Modules (functor implementations following MP44)
    - **M01_summarizing_database/** - Database documentation module

- **01_db/** - Database table definitions
  - Table creation scripts
  - Table schema definitions
  - Database structure setup

- **02_db_utils/** - Database utility functions
  - Database connection handling
  - Database operations and transactions
  - Data copying and backup utilities

- **03_config/** - Configuration and parameter files
  - Global parameters and settings
  - Path configurations
  - Environment setups
  - Initialization and deinitialization scripts

- **04_utils/** - General utility functions
  - String manipulation
  - Data cleaning
  - File operations
  - Data structure conversions

- **05_data_processing/** - Data transformation and processing scripts
  - **amazon/** - Amazon-specific data processing
  - **officialwebsite/** - Official website-specific data processing
  - **common/** - Shared data processing utilities

- **05_etl_utils/** - ETL (Extract, Transform, Load) utility functions
  - **all/** - Cross-platform ETL utilities
    - **import/** - Import phase functions (0IM)
      - `fn_import_competitor_products.R` - Import competitor products from Google Sheets
    - **stage/** - Staging phase functions (1ST)
      - `fn_stage_competitor_products.R` - Stage competitor products with validation
    - **transform/** - Transform phase functions (2TR)
      - `fn_transform_competitor_products.R` - Transform competitor products for business use
  - **amz/** - Amazon-specific ETL utilities
  - **eby/** - eBay-specific ETL utilities

- **06_queries/** - Query functions
  - Standardized query operations
  - Data retrieval patterns

- **07_models/** - Statistical models and analysis
  - Choice modeling
  - Design matrix generation
  - Regression models
  - Optimization algorithms

- **08_ai/** - AI and ML components
  - Review rating prediction
  - AI integration utilities
  - Decode/process AI outputs

- **09_python_scripts/** - Python integrations
  - Comment rating scripts
  - Gender prediction models

- **10_rshinyapp_components/** - Reusable Shiny components
  - **data/** - Data source components
  - **macro/** - Macro-level analysis components
  - **micro/** - Micro-level analysis components
  - **target/** - Target analysis components

- **11_rshinyapp_utils/** - Shiny application utilities
  - Helper functions for the Shiny app
  - YAML configuration utilities
  - Data processing functions

- **12_bash/** - Bash scripts for system operations
  - Git-Dropbox integration utilities
  - Automation scripts

- **13_modules/** - Company-specific module implementations
  - **COMPANY_NAME/** - Directories for each company
    - **M** - Modules (minimal purpose functionality)
    - **S** - Sequences (multi-purpose processes)
    - **D** - Derivations (complete transformation processes)

- **../nsql/** - Natural SQL Language implementation (standalone git repo in shared/)
  - Grammar definitions
  - Dictionary and rules
  - Extensions and translators
  - Example NSQL commands
  - Note: NSQL is a standalone git repo in `shared/nsql/`, symlinked into company projects as `scripts/nsql/`

- **98_debug/** - Debugging utilities
  - Debug logging functions
  - Test harnesses
  - Performance profiling tools

- **99_archive/** - Archived code and documentation
  - Previous versions of restructured code
  - Historical implementations

## Database Documentation

The project includes automated database documentation capabilities implemented through:

1. **Module**: `00_principles/M01_summarizing_database/`
2. **Command**: `summarize database "app_data.duckdb"` (SNSQL)
3. **Output**: Documentation is generated in `docs/database/`

This functionality follows the Functor-Module Correspondence Principle (MP44), treating database documentation as a functor that maps from database structures to documentation artifacts.

To generate database documentation, run:

```r
source("update_scripts/global_scripts/00_principles/M01_summarizing_database/sc_document_database.R")
```

Documentation outputs include:
- Table listings
- Column definitions
- Data samples
- Relationship information

## ETL Pipeline Documentation

The project includes comprehensive ETL pipeline documentation in `00_principles/principles_qmd/`:

### ETL04 - Competitor Analysis Pipeline
- **Document**: `ETL04_competitor_analysis.qmd`
- **Purpose**: Three-phase competitor analysis pipeline (0IM→1ST→2TR)
- **Migration**: Replaces legacy D03_01 with modern ETL architecture
- **Implementation**: `amz_ETL04_0IM.R`, `amz_ETL04_1ST.R`, `amz_ETL04_2TR.R`

### ETL03 - product Profiles Pipeline
- **Document**: `ETL03_product_profiles.qmd`
- **Purpose**: Amazon product profile processing pipeline
- **Implementation**: Multiple scripts for different data sources

### Key ETL Features
- **Seven-layer architecture**: raw → staged → transformed → cleansed → integrated → processed → reporting
- **Platform-specific processing**: Amazon (amz), eBay (eby) ready
- **Robust error handling**: Retry mechanisms, validation layers
- **Performance optimization**: Optional validation skipping, type optimization

## Axiomatic System (MP-P-R Framework)

This project is organized according to the axiomatic system defined in our MP-P-R framework:

### Meta-Principles (MP)

Meta-principles govern how principles themselves are structured and related. They are abstract, conceptual, and foundational, covering system-wide architecture and organizational concepts.

Key meta-principles include:
- **MP00**: Axiomatization System
- **MP01**: Primitive Terms and Definitions
- **MP05**: Instance vs. Principle
- **MP13**: Principle of Analogy of Statute Law
- **MP14**: Change Tracking Principle
- **MP15**: Currency Principle
- **MP16**: Modularity Principle
- **MP17**: Separation of Concerns Principle
- **MP18**: Don't Repeat Yourself Principle

### Principles (P)

Principles provide core guidance for implementation. They are conceptual but practical, offering actionable guidelines for broad implementation patterns and approaches.

Key principles include:
- **P00**: Project Principles
- **P02**: Data Integrity
- **P04**: App Construction Principles
- **P07**: App Bottom-Up Construction
- **P10**: Documentation Update Principle
- **P11**: Similar Functionality Management Principle

### Rules (R)

Rules define specific implementation details. They are concrete, specific, and directly applicable, covering narrow implementation techniques and specific patterns.

Key rules include:
- **R04**: App YAML Configuration
- **R09**: Global Scripts Synchronization
- **R10**: Database Table Naming and Creation
- **R11**: UI-Server-Defaults Triple Rule

For full documentation of all principles, see the `00_principles` directory.

## Change Tracking Requirements

As specified in MP14 (Change Tracking Principle) and R09 (Global Scripts Synchronization), all changes to global_scripts must be tracked through Git with the following workflow:

1. Commit changes immediately after completion
2. Pull from the remote repository before pushing
3. Push committed changes without delay

Commit messages should follow the format:
```
[MP/P/R Reference] Brief description of change

Detailed explanation of what was changed and why.
Additional context or considerations if needed.
```

## YAML Configuration

Application configuration is managed through YAML files following the R04 (App YAML Configuration) rule. The standard location is:

```
precision_marketing_app/app_configs.yaml
```

The configuration follows six standard patterns:
1. Simple String Format
2. Array Format
3. Object Format with Roles
4. Single/Multiple Data Sources with Parameters
5. Environment Configuration Pattern
6. External Parameter Files Pattern

Example configuration:
```yaml
components:
  macro:
    overview: sales_summary_view
    
    trends:
      data_source: sales_trends
      parameters:
        show_kpi: true
        refresh_interval: 300
  
  analytics:
    predictive_models:
      data_source: customer_data
      parameters:
        prediction_horizon: 90
        model_configurations:
          file: "app_data/parameters/model_config.xlsx"
          sheet: "PredictiveModels"
          refresh_on_startup: true
```

## Cross-Computer Setup

### Setting Up Root Path

To handle running on different computers, the repository includes a dynamic root path configuration:

1. In R scripts, use the `fn_root_path_config.R` utility:

```r
# At the beginning of your R script
source("path/to/00_principles/fn_root_path_config.R")
paths <- setup_project_paths("YOUR_COMPANY_NAME")  # e.g., "WISER", "KitchenMAMA"

# Now you can use:
data_dir <- paths$data_path
```

2. For bash scripts, the `exclude_git_from_dropbox.sh` script will automatically detect the appropriate root path.

### Git-Dropbox Integration

To prevent Dropbox sync issues with Git repositories:

1. Run the `exclude_git_from_dropbox.sh` script:

```bash
cd /path/to/precision_marketing/precision_marketing_COMPANY/precision_marketing_app/update_scripts/global_scripts
bash 12_bash/exclude_git_from_dropbox.sh
```

This script:
- Replaces .git directories with symbolic links to .git.nosync directories
- Adds appropriate .dropbox.ignore files
- Adds Dropbox conflict patterns to .gitignore files

## Organizational Frameworks

The system is organized according to two conceptual frameworks:

### 1. Principle Framework (MP/P/R) - How to Implement

- **Meta-Principles (MP)**: Govern principles themselves
- **Principles (P)**: Guide implementation
- **Rules (R)**: Define specific implementations

### 2. Organizational Framework (M/S/D) - What to Implement

- **Modules (M)**: Organize minimal purpose functionality
- **Sequences (S)**: Organize multi-purpose processes
- **Derivations (D)**: Document complete transformation processes

Each company has its own implementation in the `13_modules` directory.

## File Naming Conventions

### Snake Case File Names

All files in the global_scripts repository should follow the snake_case naming convention:

- **Use lowercase letters**: All filenames should be lowercase
- **Use underscores (_) to separate words**: For example, `process_amazon_data.R` not `processAmazonData.R`
- **Prefix function files with fn_**: For reusable functions, use the `fn_` prefix
- **Prefix script files with sc_**: For executable scripts, use the `sc_` prefix
- **Use descriptive names**: Names should clearly indicate the purpose of the file

Examples of proper file naming:
```
fn_root_path_config.R
sc_initialization_update_mode.R
fn_dbConnect_from_list.R
fn_process_data_source.R
```

## Git Structure and Version Control

**IMPORTANT: The git repository is set up in the global_scripts directory, not at the project root.**

This means that git commands should be run from:
```
/path/to/precision_marketing/precision_marketing_COMPANY/precision_marketing_app/update_scripts/global_scripts
```

This repository uses Git for version control and is designed with a structure that minimizes merge conflicts:

- Each subdirectory contains logically related files
- Keeping related files in separate directories helps with:
  - Selective commits and branching
  - Code reviews (changes to specific functional areas are grouped)
  - History tracking by functional area

### Git-Dropbox Integration

This repository is designed to work smoothly with Dropbox. To prevent sync conflicts:

1. The `.git` directory is renamed to `.git.nosync` with a symbolic link
2. A `.dropbox.ignore` file is added to prevent Dropbox from syncing Git files
3. The `.gitignore` file includes patterns to ignore Dropbox conflict files

When cloning this repository to a new location within Dropbox, run the `exclude_git_from_dropbox.sh` script in the `12_bash` directory to set up proper integration.

### Recommended Git Practices

1. **Commit Messages**: Include relevant principle references in commit messages
   ```
   [MP14, R09] Update database connection handling
   [P02, R04] Fix YAML configuration parsing for nested parameters
   [MP06] Implement new data source hierarchy for streaming data
   ```

2. **Branching Strategy**: Create branches based on functional areas when possible
   ```
   feature/db-optimization
   bugfix/amazon-data-processing
   feature/shiny-dashboard-improvements
   ```

3. **Pull Requests**: Group changes by functional area when possible to simplify reviews

## Project Structure

```
precision_marketing/
└── precision_marketing_COMPANY/
    └── precision_marketing_app/
        ├── app_configs.yaml
        ├── data/
        └── update_scripts/
            └── global_scripts/ (this repository)
                ├── 00_principles/
                ├── 01_db/
                ├── 02_db_utils/
                ├── 03_config/
                ├── 04_utils/
                ├── 05_data_processing/
                ├── 06_queries/
                ├── 07_models/
                ├── 08_ai/
                ├── 09_python_scripts/
                ├── 10_rshinyapp_components/
                ├── 11_rshinyapp_utils/
                ├── 12_bash/
                ├── 13_modules/
                ├── 98_debug/
                └── 99_archive/
```

For any issues or questions, please contact the development team.

Last updated: April 2025