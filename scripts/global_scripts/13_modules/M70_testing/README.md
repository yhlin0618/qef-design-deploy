# M70 Testing Module

This module provides testing functionality for the precision marketing application.

## Purpose

The testing module enables automated verification and testing of the application to ensure:
1. Configuration files are valid and complete
2. The application starts correctly
3. Core functionality works as expected

## Components

### M70_fn_verify_config.R

Verifies that the application configuration is valid and complete. It checks for:
- Required fields presence
- Valid values and structure
- Proper formatting

Usage:
```r
source("modules/M70_testing/M70_fn_verify_config.R")
results <- M70_verify_config("app_config.yaml", verbose = TRUE)
```

### M70_fn_test_app.R

Runs the application in test mode and verifies its functionality. Features:
- Configurable test modes: basic, comprehensive
- Timeout control to prevent hanging tests
- Detailed error reporting
- Test report generation

Usage:
```r
source("modules/M70_testing/M70_fn_test_app.R")
results <- M70_test_app("app_config.yaml", test_mode = "basic", timeout = 30)
```

### run_tests.R

A convenience script that runs both verification and testing in sequence:
1. Verifies the configuration
2. Only runs the app test if verification passes
3. Generates comprehensive reports

Usage:
```r
Rscript modules/M70_testing/run_tests.R
```

## Reports

The module generates the following report files in the `modules/M70_testing/` directory:
- `config_verification_YYYYMMDD_HHMMSS.md`: Configuration verification report
- `app_test_report_YYYYMMDD_HHMMSS.md`: Application test report

## Module Relationships

This module follows the M70 Testing category in the Module Naming Convention (R07) and depends on:
- M80_utilities (for common utility functions)

## Compliance

This module adheres to:
- R07_module_naming_convention.md
- P09_authentic_context_testing.md