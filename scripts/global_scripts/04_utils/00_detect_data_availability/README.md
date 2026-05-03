# Data Availability Detection Module

This module implements MP45 (Automatic Data Availability Detection Metaprinciple) to detect data availability based on actual data in the system.

## Functions

This module contains the following functions:

1. **connect_to_app_database**: Establishes a connection to the application database
2. **detect_marketing_channel_availability**: Detects which marketing channels have data
3. **load_platform_dictionary**: Loads the platform dictionary mapping IDs to names
4. **initialize_data_availability**: Sets up the data availability detection system
5. **is_available**: Checks if data is available for a specific domain/dimension
6. **render_adaptive_radio_buttons**: (Deprecated) Renders adaptive UI based on data availability

## Usage

These functions are typically used during application initialization:

```r
# Initialize connection
conn <- connect_to_app_database("app_data/app_data.duckdb")

# Initialize data availability detection
initialize_data_availability(conn)

# Check if a specific channel is available
if (is_available("channel", "amazon")) {
  # Use Amazon-specific functionality
}
```

## Implementation Notes

- All functions follow R21 (One Function One File) principle
- File names follow R69 (Function File Naming) with the "fn_" prefix
- Functions implement MP45 (Automatic Data Availability Detection) and related principles
- Global variables are used to store availability information

## Relationships to Other Components

This module relates to:
- Database connection functionality
- UI adaptation based on data availability
- Marketing channel detection and usage

#LOCK FILE