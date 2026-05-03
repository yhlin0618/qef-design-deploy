# Data Access Permission System Implementation

This document describes the implementation of a comprehensive data access permission system based on the Data Source Hierarchy Principle and the Operating Modes Principle.

## Background

The precision marketing application needs to enforce different permissions for data access based on the operating mode:
- **APP_MODE**: Read-only access to app data and a subset of global data
- **UPDATE_MODE**: Read-write access to app and processing data, read-only to global data
- **GLOBAL_MODE**: Full read-write access to all data sources

## Implementation Steps

### 1. Requirements Gathering

We first analyzed the principles to understand requirements:
- Identify data layers (App, Processing, Global, Development, External)
- Map database access to these layers
- Define permission rules for each mode
- Implement permission checking and enforcement

### 2. Core Permission Checking Function

We created `fn_check_data_access.R` that:
- Takes data layer, access type, and path as inputs
- Verifies if current operation mode allows the requested access
- Returns clear warning messages when access is denied

### 3. Database Connection Enhancement

We enhanced `dbConnect_from_list.R` to:
- Determine which data layer a database belongs to
- Check permissions based on operation mode
- Force read-only connections when write permission is denied
- Include fallback for APP_MODE initialization sequence compatibility

### 4. Testing Application

A test application was created to validate the permission system:
- Allows switching between operation modes
- Tests connections to different database types
- Verifies read/write permissions are correctly enforced
- Provides clear feedback about permission checking

## Key Code Snippets

### Permission Checking Logic

```r
# Check if the access type is allowed for this mode and layer
allowed_access <- access_rules[[current_mode]][[data_layer]]
access_allowed <- access_type %in% allowed_access

# Log access attempts based on result
if (!access_allowed) {
  if (is.null(data_path)) {
    warning(access_type, " access to ", data_layer, " layer not allowed in ", current_mode)
  } else {
    warning(access_type, " access to ", data_path, " (", data_layer, " layer) not allowed in ", current_mode)
  }
}
```

### Database Enforcement Logic

```r
# APP_MODE compatibility - enforce read-only even if permission checks aren't available
if (!has_permission_check && exists("OPERATION_MODE") && OPERATION_MODE == "APP_MODE") {
  # APP_MODE 下的簡化存取規則：強制資料庫為唯讀模式
  if (!read_only) {
    if (verbose) {
      message("注意：在 APP_MODE 中，資料庫連線被強制設為唯讀模式。")
    }
    read_only <- TRUE
  }
}

# With permission checking
if (!is.null(data_layer)) {
  has_permission <- check_data_access(data_layer, access_type, db_path)
  
  # Force read-only mode if write permission is denied
  if (!has_permission && access_type == "write") {
    if (verbose) {
      message("注意：根據目前操作模式的存取權限，資料庫 '", dataset, "' (",
              data_layer, " 層) 被強制設為唯讀模式。")
    }
    read_only <- TRUE
  }
}
```

## Testing Approach

The test application we created provides:
- Interactive mode switching to test different permission scenarios
- Tests for all three database layers (App, Processing, Global)
- Verification of read-only enforcement in APP_MODE
- Check for proper error messages when access is denied

To use it:
1. Run `test_db_permission_app.R`
2. Select different operation modes
3. Click test buttons to verify correct permissions
4. Observe connection status and read/write permissions

## Architectural Considerations

### 1. Initialization Sequence Compatibility

The enhanced database connection function works properly regardless of initialization order:
- If permission checking is available, uses the full permission model
- If unavailable but in APP_MODE, enforces basic read-only rule
- Provides graceful degradation for deployment scenarios

### 2. Deployment Requirements

This implementation highlighted a key deployment principle:
- All files sourced in APP_MODE must be included in the deployment package
- Proper loading order should be preserved
- Dependencies between utility directories must be maintained

### 3. Error Handling

The system provides:
- Clear warning messages about denied permissions
- Graceful degradation instead of hard failures
- Verbose output option for troubleshooting

## Future Enhancements

1. Add audit logging for sensitive data access operations
2. Implement similar permission checking for file operations
3. Create database migration tools that respect permission boundaries
4. Add role-based access control for multi-user scenarios

## Conclusion

This implementation creates a robust permission system that enforces the principles without breaking existing functionality. It demonstrates how architectural principles translate into practical code while maintaining backward compatibility.