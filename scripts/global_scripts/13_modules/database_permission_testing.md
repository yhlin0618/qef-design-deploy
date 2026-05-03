# Database Permission System Testing Guide

This document provides a comprehensive guide for testing the database permission system that implements the Data Source Hierarchy Principle.

## Background

The precision marketing application enforces a permission system based on operation modes:
- **APP_MODE**: Read-only access to app data and global data
- **UPDATE_MODE**: Read-write access to app/processing data, read-only to global data
- **GLOBAL_MODE**: Full read-write access to all data sources

An interactive test application has been created to validate this system's implementation.

## Test Application

**Location**: `/update_scripts/global_scripts/98_debug/rshinyapp/test_db_permission_app.R`

This Shiny application provides interactive testing of the database connection permission system, allowing you to:
1. Switch between operation modes
2. Test connections to different database types
3. Verify read/write permissions
4. Observe permission enforcement

## Running the Test Application

Following the Authentic Context Testing Principle (25_authentic_context_testing.md), you must run the test application from the project root directory:

```r
# IMPORTANT: Navigate to the project root directory first
# This ensures proper path resolution and environment initialization
setwd("/path/to/precision_marketing_app")  # Adjust path as needed

# Run the test application from the root context
# Use platform-neutral path construction (Platform-Neutral Code Principle)
app_path <- file.path("update_scripts", "global_scripts", "98_debug", 
                     "rshinyapp", "test_db_permission_app.R")
shiny::runApp(app_path)
```

This approach ensures that:
- Path resolution works correctly for all file references
- The initialization scripts run in the proper context
- Database connections use the correct paths
- The environment matches actual usage conditions

## Testing Procedure

### 1. Preparation

Before running tests, ensure:
- You have proper credentials to access databases
- Database files exist (the app will create them if missing)
- Any existing database connections are closed

### 2. APP_MODE Testing

1. Select "APP_MODE" from the dropdown menu
2. Click "Test App Data Connection"
   - Expect: Connection succeeds, read-only mode enforced
   - Write attempt should fail with permission error
3. Click "Test Processing Data Connection"
   - Expect: Connection may be blocked entirely (per permissions)
4. Click "Test Global Data Connection"
   - Expect: Connection succeeds, read-only mode enforced
   - Write attempt should fail with permission error

### 3. UPDATE_MODE Testing

1. Select "UPDATE_MODE" from the dropdown menu
2. Click "Test App Data Connection"
   - Expect: Connection succeeds with write access
   - Write attempt should succeed
3. Click "Test Processing Data Connection"
   - Expect: Connection succeeds with write access
   - Write attempt should succeed
4. Click "Test Global Data Connection"
   - Expect: Connection succeeds, read-only mode enforced
   - Write attempt should fail with permission error

### 4. GLOBAL_MODE Testing

1. Select "GLOBAL_MODE" from the dropdown menu
2. Click "Test App Data Connection"
   - Expect: Connection succeeds with write access
   - Write attempt should succeed
3. Click "Test Processing Data Connection"
   - Expect: Connection succeeds with write access
   - Write attempt should succeed
4. Click "Test Global Data Connection"
   - Expect: Connection succeeds with write access
   - Write attempt should succeed

## Interpreting Results

The test results panel provides detailed information about:

### Connection Status
- Success/failure of connection attempt
- Current operation mode
- Available tables in the database
- Read/write status of the connection

### Permission System Status
- Whether permission checking functions are available
- Which data layer was detected for the database
- Whether permission checks passed or were enforced

### Write Operation Results
- Success/failure of write attempts
- Specific error messages if permissions were enforced

## Troubleshooting Common Issues

### Permission Functions Not Available

If you see "Permission checking system not found":
- This is normal in APP_MODE when `11_rshinyapp_utils` loads after `02_db_utils`
- The basic APP_MODE protection should still enforce read-only mode
- Verify that write operations still fail in APP_MODE

### Database Connection Failures

If connections fail:
- Check database paths in `db_path_list`
- Verify file permissions for database locations
- Ensure parent directories exist

### Mode Switching Issues

If changing modes doesn't work:
- Check for errors in the initialization scripts
- Verify that `INITIALIZATION_COMPLETED` is being reset
- Restart the application and try again

## Advanced Testing

### Testing with Modified Rules

To test with modified permission rules:
1. Edit `/update_scripts/global_scripts/11_rshinyapp_utils/fn_check_data_access.R`
2. Modify the `access_rules` list to change permissions
3. Restart the test application
4. Observe the effects of your changes

### Testing Edge Cases

- Test with non-existent databases to verify creation behavior
- Test with readonly file permissions on database files
- Test rapid mode switching to verify proper cleanup

## Conclusion

The database permission testing application provides a thorough validation of the Data Source Hierarchy Principle implementation. By testing all combinations of operation modes and data layers, you can ensure that permissions are being properly enforced throughout the system.

After completing tests, document any issues or observations in the records directory according to the Instance vs. Principle Meta-Principle.