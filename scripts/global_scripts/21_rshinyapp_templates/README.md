# MAMBA Templates Directory

> **Scope**: Universal (distributed via git subrepo to all company projects)

This directory contains **base templates** that define configuration schemas and default values. These templates are **NOT** meant to be used directly - they serve as the foundation for company-specific configurations.

## Directory Contents

```
templates/
├── README.md                      # This file
└── _targets_config.base.yaml      # Pipeline configuration base template
```

## Key Principles

This directory implements:
- **SO_P016**: Configuration Scope Hierarchy (Universal → Company → Application)
- **MP142**: Configuration-Driven Pipeline (centralized YAML configuration)
- **MP122**: Triple-Track Subrepo Architecture (Track 1: global_scripts)

## Three-Layer Configuration Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│ Layer 1: Universal (THIS DIRECTORY)                                 │
│ Location: global_scripts/templates/_targets_config.base.yaml        │
│ Contains: Schema definitions, default values, excluded patterns     │
│ Scope: Shared across ALL company projects via git subrepo           │
│ Rule: NO company-specific content allowed                           │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              │ modifyList() merge
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Layer 2: Company Override                                           │
│ Location: app_config.yaml > pipeline: section                       │
│ Contains: Platform enabled flags, execution overrides               │
│ Scope: Project-specific (NOT in subrepo)                            │
│ Rule: Only override, never duplicate base schema                    │
└─────────────────────────────────────────────────────────────────────┘
                              │
                              │ make config-merge
                              ▼
┌─────────────────────────────────────────────────────────────────────┐
│ Layer 3: Generated Output                                           │
│ Location: {project_root}/_targets_config.yaml                       │
│ Contains: Merged final configuration                                │
│ Scope: Project-specific, git-ignored                                │
│ Rule: Auto-generated, do not edit manually                          │
└─────────────────────────────────────────────────────────────────────┘
```

## Template Files

### `_targets_config.base.yaml`

The base template for MAMBA pipeline orchestration (MP140-MP142).

**What it SHOULD contain (Schema/Defaults):**
```yaml
# Phase ordering (structure)
phase_order: ["0IM", "1ST", "2TR", "3PR", "4CL", "5DN", "6NM"]

# Default execution settings (reasonable defaults)
execution:
  parallel_workers: 4
  per_target_timeout_minutes: 30

# Platform schema (structure definition)
platform_schema:
  etl:
    naming_pattern: "{platform}_ETL_{datatype}_{phase}.R"
  drv:
    naming_pattern: "{platform}_D{group}_{seq}.R"

# Excluded patterns (universal patterns)
excluded_patterns:
  - "**/backup_*/**"
  - "**/archive/**"
```

**What it MUST NOT contain:**
```yaml
# WRONG - Company-specific enabled flags
platforms:
  cbz:
    enabled: true   # NEVER in base template!

# WRONG - Project-specific dependencies
explicit_dependencies:
  cbz_D04_02: ["cbz_D01_07"]  # NEVER in base template!

# WRONG - Environment-specific paths
log_path: "/opt/mamba/logs/"  # NEVER in base template!
```

## Usage Workflow

### For Developers (Company Project)

```bash
# 1. Edit company config (app_config.yaml)
vim ../../app_config.yaml

# Add/modify the pipeline: section:
# pipeline:
#   platforms:
#     cbz:
#       enabled: true
#       drv_groups: ["D01", "D04"]

# 2. Regenerate merged config
cd scripts/update_scripts
make config-merge

# 3. Verify
cat ../../_targets_config.yaml

# 4. Run pipeline
make run TARGET=cbz_D04_02
```

### For Framework Maintainers (global_scripts)

When modifying base templates:

1. **Only change structure/defaults**, never company-specific content
2. **Test locally** before pushing to subrepo
3. **Document changes** in principle files (MP142)
4. **Verify backward compatibility** - company configs should still work

```bash
# After modifying base template:
cd scripts/update_scripts
make config-merge  # Should succeed
make config-validate  # Should pass
```

## Related Commands

```bash
# From update_scripts/ directory:
make config-merge      # Merge base + company → generated
make config-scan       # Scan ETL/DRV directories for scripts
make config-full       # config-merge + config-scan + config-validate
make config-validate   # Validate configuration integrity
```

## Merge Function

The merge is performed by `fn_merge_pipeline_config.R`:

```r
# Located at: global_scripts/04_utils/fn_merge_pipeline_config.R
merge_pipeline_config(
  base_path = "global_scripts/templates/_targets_config.base.yaml",
  app_config_path = "../../app_config.yaml",
  output_path = "../../_targets_config.yaml"
)
```

**Merge behavior:**
- Uses R's `modifyList()` for deep merge
- Company config wins on conflicts
- Base provides defaults for missing values
- Lists are replaced, not appended

## Common Questions

### Q: Why not put the full config in global_scripts?

Because `global_scripts/` is distributed via git subrepo to ALL company projects. If we put company-specific settings (like `cbz: enabled: true`) in `global_scripts/`, those settings would be pushed to every project, causing conflicts.

### Q: Can I add new platforms to the base template?

Yes, you can add to `available_platforms:` (schema definition). But do NOT add `enabled: true` - that belongs in `app_config.yaml`.

### Q: What if I need a project-specific excluded pattern?

Add it to your `app_config.yaml > pipeline: excluded_patterns:` section. The merge will combine both lists.

### Q: Where is the generated config stored?

At `{project_root}/_targets_config.yaml`. This file:
- Should be in `.gitignore`
- Is regenerated by `make config-merge`
- Is read by `_targets.R` at runtime

## Version History

| Date | Version | Changes |
|------|---------|---------|
| 2025-12-23 | 1.0 | Initial creation with MP142 |
| 2025-12-24 | 1.1 | Added three-layer architecture per SO_P016 |

## See Also

- **MP142**: Configuration-Driven Pipeline
- **MP140**: Pipeline Orchestration Principle
- **MP141**: Scheduled Execution Pattern
- **SO_P016**: Configuration Scope Hierarchy
- **MP122**: Triple-Track Subrepo Architecture
