# Testing the microCustomer Module

This document outlines the testing approach for the microCustomer module following the principles of P16 (Component-wise Testing), R74 (Shiny Test Data), and R75 (Test Script Initialization).

## Available Test Scripts

The module includes two test implementations:

1. **Standard Implementation Test** (`microCustomer_test.R`):
   - Tests the standard implementation with direct data frame inputs
   - Provides various test scenarios (complete, incomplete, empty data)
   - Includes debugging tools and visualization

2. **Universal Implementation Test** (`microCustomer_test_universal.R`):
   - Tests the Universal Data Access Pattern implementation
   - Tests multiple connection types (list, function, mock DBI, mixed)
   - Demonstrates how the module works with different data sources

## Running the Tests

### Standard Implementation Test

```r
# Navigate to the project root directory
setwd("/path/to/precision_marketing_app")

# Run the standard test
source("update_scripts/global_scripts/10_rshinyapp_components/micro/microCustomer/microCustomer_test.R")
```

### Universal Data Access Test

```r
# Navigate to the project root directory
setwd("/path/to/precision_marketing_app")

# Run the universal test
source("update_scripts/global_scripts/10_rshinyapp_components/micro/microCustomer/microCustomer_test_universal.R")
```

## Test Data

The test data follows R74 (Shiny Test Data) with these characteristics:

| Feature | Description |
|---------|-------------|
| Complete Data | Full customer profiles and DNA data for comprehensive testing |
| Incomplete Data | Partial DNA data coverage (some customers lack DNA data) |
| Empty Data | Test case with empty datasets to verify error handling |
| Realistic Values | Sample data with realistic RFM metrics and customer values |
| Edge Cases | Includes both high-value and low-value customer examples |

## Test Scenarios

Each test script includes multiple test scenarios:

1. **Complete Data Scenario**:
   - 5 customer profiles
   - 3 customers with DNA data (60% coverage)
   - Full range of metrics and values

2. **Incomplete Data Scenario**:
   - 5 customer profiles
   - 2 customers with DNA data (40% coverage)
   - Tests handling of partially available data

3. **Empty Data Scenario**:
   - Empty data frames
   - Tests error handling and default value display

## What to Test

When running the test applications, verify that:

1. **Data Loading**:
   - Data loads correctly for all scenarios
   - Error handling works for invalid data

2. **Filter Functionality**:
   - Customer dropdown populates correctly
   - Selection changes update the display
   - Only customers with DNA data appear in the dropdown

3. **Display Components**:
   - All metrics display correctly
   - Default values appear when data is missing
   - Formatting is consistent and readable

4. **Universal Data Access** (for universal test):
   - Switching connection types works properly
   - All connection types return consistent results
   - Error handling works for all connection types

## Test Implementation

The test scripts implement:

1. **P16 Component-wise Testing**:
   - Each UI component can be tested independently
   - Multiple data scenarios for comprehensive testing

2. **R74 Shiny Test Data**:
   - Structured test data with metadata
   - Multiple test scenarios
   - Edge cases and validation

3. **R75 Test Script Initialization**:
   - Proper environment setup
   - Root path detection
   - Portable initialization

4. **R91 Universal Data Access Pattern**:
   - Tests multiple connection types
   - Validates universal accessor functionality

## Manual Testing Checklist

- [ ] Run standard test with all scenarios
- [ ] Run universal test with all connection types
- [ ] Verify filter dropdown populates correctly
- [ ] Verify customer selection updates the display
- [ ] Verify all metrics display correctly
- [ ] Test error handling with invalid data
- [ ] Verify internationalization support
- [ ] Test responsive layout at different screen sizes

## Notes on Universal Data Access Testing

The universal test demonstrates the R91 Universal Data Access Pattern with these connection types:

1. **List Connection**: Direct access to data frames
2. **Function Connection**: Data accessed through getter functions
3. **Mock DBI Connection**: Simulated database connection
4. **Mixed Connection**: Combination of direct access and functions

Each type demonstrates a different way the module can interface with data sources while maintaining consistent functionality.