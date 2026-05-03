# Migration Scripts Directory

**Purpose**: One-time migration and transformation scripts
**Created**: 2025-11-13

## Overview

This directory contains scripts that are executed **manually** for one-time migrations, transformations, or large-scale code updates. These scripts are NOT loaded during app initialization.

## Key Principles

### Why Separated from 04_utils?

1. **MP031 Compliance**: Migration scripts contain top-level executable code that runs immediately when sourced
2. **R045 Compliance**: Initialization should only load function definitions, not execute operations
3. **Performance**: Migration scripts scan thousands of files and should never run during app startup

### Architecture Decision

**Before**: Migration scripts were in `04_utils/`, causing them to be auto-loaded during initialization
**After**: Migration scripts are in `28_migration_scripts/`, preventing accidental execution
**Result**: App starts in seconds instead of hanging

## Files in This Directory

### migrate_drv_to_df_complete.R
**Purpose**: Complete migration of `drv` references to `df` table naming convention
**Usage**:
```r
# Run manually from project root
source("scripts/global_scripts/28_migration_scripts/migrate_drv_to_df_complete.R")
```

**What it does**:
- Scans ~5000+ files across the project
- Replaces `drv_*` table names with `df_*`
- Creates backups with `.backup_drv_TIMESTAMP` suffix
- Reports changes made

**Expected runtime**: 5-10 minutes for full codebase scan

### migrate_drv_to_df.py
**Purpose**: Python version of DRV to DF migration
**Usage**:
```bash
cd scripts/global_scripts/28_migration_scripts
python migrate_drv_to_df.py
```

### migrate_drv_to_df.sh
**Purpose**: Shell script wrapper for migration
**Usage**:
```bash
./scripts/global_scripts/28_migration_scripts/migrate_drv_to_df.sh
```

## Usage Guidelines

### DO:
- Run migration scripts manually when needed
- Review backup files after migration
- Test thoroughly after running migrations
- Keep migration scripts for reference/rollback

### DON'T:
- Source migration scripts during app initialization
- Move migration scripts back to `04_utils/`
- Delete backup files immediately after migration
- Run migration scripts in production without testing

## Historical Context

### 2025-11-13: Critical Bug Fix

**Problem**: App hung at startup after "Found 5348 files to scan"

**Root Cause**:
- `migrate_drv_to_df_complete.R` was in `04_utils/`
- `sc_initialization_app_mode.R` auto-sources all files from `04_utils/`
- Migration script has top-level code that executes immediately
- This caused 5000+ file scan during app startup

**Solution**:
- Moved all migration scripts to `28_migration_scripts/`
- Prevents auto-loading during initialization
- App now starts normally

**Lessons Learned**:
1. One-time scripts must be separated from runtime utilities
2. All sourced files should only contain function definitions
3. Executable code should be wrapped in functions or separated

## Best Practices for Future Migrations

### Naming Convention
```
migrate_<from>_to_<to>_<scope>.R
convert_<what>_to_<format>.R
transform_<operation>_<target>.R
```

Examples:
- `migrate_drv_to_df_complete.R` ✓
- `convert_csv_to_parquet.R` ✓
- `transform_snake_to_camel_case.R` ✓

### Script Structure
```r
#!/usr/bin/env Rscript
# <Description>
# Author: <name>
# Date: YYYY-MM-DD
# Expected runtime: <estimate>

# WRAP ALL CODE IN FUNCTION
migrate_operation <- function() {
  message("Starting migration...")

  # Configuration
  config <- list(...)

  # Main operation
  process_files()

  # Summary
  report_results()
}

# DON'T AUTO-EXECUTE
# Require explicit call:
# source("script.R"); migrate_operation()
```

### Safety Checklist

Before running any migration:
- [ ] Commit current changes to git
- [ ] Run on test branch first
- [ ] Review scope of changes
- [ ] Ensure backups are created
- [ ] Test after migration completes
- [ ] Verify backups work for rollback

## Rollback Procedure

If a migration causes issues:

```bash
# Find backup files
find . -name "*.backup_drv_*" -type f

# Restore a specific file
mv file.R.backup_drv_20251113_122113 file.R

# Bulk restore all files from timestamp
find . -name "*.backup_drv_20251113_122113" -exec sh -c 'mv "$1" "${1%.backup_drv_20251113_122113}"' _ {} \;
```

## Related Principles

- **MP031**: Proper autoinit/autodeinit usage - no top-level execution
- **R045**: Initialization imports only - only function definitions
- **MP099**: Real-time progress reporting - migrations should show progress
- **R119**: Table naming standardization - why DRV→DF migration was needed

---

**Maintained by**: principle-debugger
**Last updated**: 2025-11-13
