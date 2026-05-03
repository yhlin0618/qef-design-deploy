# 16_derivations - Cross-Company Derivation Core Functions

This directory contains **parameterized core functions** for derivations that are shared across all companies via the git subrepo system.

## Purpose

Following **DEV_R038** (Derivation Core Function Pattern), this directory separates:
- **Core functions** (here): Business logic, parameterized for any platform/company
- **Wrappers** (`update_scripts/DRV/{platform}/`): Thin scripts that call cores with platform-specific settings
- **Algorithms** (`04_utils/`): General-purpose algorithms (e.g., `fn_analysis_dna.R`)
- **Configuration** (`04_utils/fn_get_platform_config.R`): Platform config helper (MP065)

## Directory Structure

```
16_derivations/
├── README.md                     # This file
├── fn_D01_03_core.R             # D01_03: Customer DNA Analysis
├── fn_D01_04_core.R             # D01_04: Customer Profile Creation
└── fn_D01_05_core.R             # D01_05: App Views Generation
```

## Core Function Interface

All core functions follow this interface:

```r
run_D{group}_{seq} <- function(platform_id, config = NULL) {
  # platform_id: "cbz", "amz", "eby", "shopify", etc.
  # config: Optional platform config. If NULL, reads from app_config.yaml

  # Returns: list(success = TRUE/FALSE, ...)
}
```

## Relationship to Wrappers

```
Wrapper (company-specific)          Core (shared via subrepo)
============================        =========================
cbz_D01_03.R                   →    fn_D01_03_core.R
  └─ calls run_D01_03("cbz")            └─ contains all business logic
```

## Principles

- **DEV_R038**: Derivation Core Function Pattern
- **DEV_R037**: Wrapper Execution Standard
- **DM_R044**: Five-Part Derivation Structure
- **MP064**: ETL-Derivation Separation

## Usage from Wrappers

```r
# In wrapper script (e.g., cbz_D01_03.R):
core_path <- file.path(GLOBAL_DIR, "16_derivations", "fn_D01_03_core.R")
source(core_path)
result <- run_D01_03(platform_id = "cbz")
```

---
*Created: 2025-12-27*
*Following: DEV_R038, MP064*
