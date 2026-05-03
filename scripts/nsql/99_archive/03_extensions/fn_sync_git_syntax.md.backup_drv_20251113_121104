# Git Synchronization Function Syntax in NSQL

This document defines the syntax for git synchronization operations in NSQL.

## SYNC GIT Directive

The `SYNC GIT` directive provides a standardized interface for synchronizing git repositories, with primary focus on the global_scripts directory synchronization mandated by Rule R008.

### Basic Syntax

```
SYNC GIT [WITH [options]]
=== Git Synchronization Details ===
[synchronization_parameters]
```

Where:
- `[options]` are optional parameters to control the synchronization behavior
- `[synchronization_parameters]` define specific configuration for the synchronization operation

By default, `SYNC GIT` should synchronize the global_scripts directory across all company projects through both pulling and pushing, as mandated by Rule R008. This ensures that changes made in one repository are automatically propagated to all other repositories.

### Parameters

The synchronization parameters section supports the following parameters:

```
$commit_message = "Message describing the changes"
$auto_commit = TRUE|FALSE
$create_missing = TRUE|FALSE
$sync_across_companies = TRUE|FALSE
$verbose = TRUE|FALSE
$skip_pull = TRUE|FALSE
$push_all = TRUE|FALSE  # Whether to push changes to all repositories (default: TRUE)
$branch = "branch_name"
```

Where:
- `$commit_message` - The commit message for staged changes
- `$auto_commit` - Whether to automatically commit uncommitted changes
- `$create_missing` - Whether to create missing repositories for companies
- `$sync_across_companies` - Whether to synchronize across all company projects
- `$verbose` - Whether to display detailed output
- `$skip_pull` - Whether to skip pulling changes before pushing
- `$push_all` - Whether to push changes to all company repositories (default: TRUE)
- `$branch` - The branch to synchronize with (defaults to main)

### Examples

Basic synchronization across all company projects (should include full bidirectional sync):

```
SYNC GIT
=== Git Synchronization Details ===
$commit_message = "[R008] Update utility functions to handle new database structure"
$auto_commit = TRUE
$push_all = TRUE
```

Pull-only synchronization (non-standard behavior):

```
SYNC GIT
=== Git Synchronization Details ===
$commit_message = "[R008] Update utility functions to handle new database structure"
$auto_commit = TRUE
$push_all = FALSE
```

Synchronization with specific options:

```
SYNC GIT WITH verbose=TRUE
=== Git Synchronization Details ===
$commit_message = "[R008] Implement feature X according to requirements"
$auto_commit = TRUE
$create_missing = TRUE
$push_all = TRUE
```

Synchronization with single repository (non-default):

```
SYNC GIT WITH single_repository=TRUE
=== Git Synchronization Details ===
$commit_message = "Update documentation for local repository only"
$repository_path = "update_scripts/global_scripts"
$skip_pull = FALSE
```

## Current Implementation Status

**Important Note**: The current implementation has significant limitations:

1. **Pull-Only Implementation**: The current implementation in `update_global_scripts()` only **pulls changes from remote repositories**, but does not push local changes to all repositories.

2. **Missing Push-All Functionality**: According to R008, `SYNC GIT` should by default implement full bidirectional synchronization (pull and push across all repositories), but this functionality is not yet fully implemented.

3. **Single Repository Push**: When used with a single repository (current working directory), changes are committed and pushed only to that repository.

4. **Execution Environment Issues**: The current script has path and dependency issues that make it difficult to run reliably in different environments.

### Recommended Workaround

Until the implementation is fixed, use the following manual workflow instead:

1. Make changes in one repository (e.g., WISER)
2. Commit and push those changes to the remote for that repository
3. In each other repository (e.g., MAMBA, KitchenMAMA), use direct git commands:
   ```bash
   cd /path/to/other/repository/precision_marketing_app/update_scripts/global_scripts
   git pull
   ```
4. Future implementation will enable a single `SYNC GIT` command to handle both pushing and pulling automatically

## R Code Generation

The directive should translate to R code using the appropriate synchronization function with bidirectional capabilities:

```r
# Standard behavior - full bidirectional sync across all companies
update_global_scripts(
  auto_commit = TRUE,
  verbose = TRUE,
  create_missing = FALSE,
  push_all = TRUE  # Default should be TRUE
)

# Current implementation limitation - pull only
update_global_scripts(
  auto_commit = TRUE,
  verbose = TRUE,
  create_missing = FALSE
  # Missing push_all functionality
)

# When single_repository=TRUE is specified
git_sync(
  commit_message = "Update documentation",
  repo_path = "update_scripts/global_scripts",
  skip_pull = FALSE
)
```

## Implementation Details

The `SYNC GIT` directive is implemented through two primary mechanisms:

1. **Multi-Repository Synchronization (Default)**: Uses the `update_global_scripts()` function from `fn_update_global_scripts.R` to synchronize the global_scripts directory across all company projects.

2. **Single Repository Synchronization (Optional)**: When `single_repository=TRUE` is specified, uses the `git_sync()` function from `git_sync.R` to handle operations on a single repository.

According to Rule R008, the standard implementation should ensure full bidirectional synchronization. The current vs. required capabilities are as follows:

| Feature | Single Repository | Multi-Repository (Current) | Multi-Repository (Required) |
|---------|------------------|----------------------------|----------------------------|
| Commit Changes | ✓ | ✓ | ✓ |
| Pull from Remote | ✓ | ✓ | ✓ |
| Push to Remote | ✓ | ✗ | ✓ |
| Create Missing Repos | N/A | ✓ | ✓ |
| Conflict Resolution | ✓ | ✓ | ✓ |

## Grammar (EBNF)

```ebnf
sync_git_directive ::= 'SYNC GIT' [options] delimiter synchronization_parameters

options ::= 'WITH' option (',' option)*

option ::= identifier '=' value

delimiter ::= '=== Git Synchronization Details ==='

synchronization_parameters ::= (parameter_assignment)*

parameter_assignment ::= '$' parameter_name '=' parameter_value
```

## Related Principles and Rules

- **R008: Global Scripts Synchronization** - Mandates immediate synchronization of changes
- **MP014: Change Tracking Principle** - Ensures changes are tracked and recoverable
- **P008: Deployment Patterns** - Affects how changed code is deployed
- **MP069: AI-Friendly Format** - Promotes formats conducive to AI collaboration
- **MP070: Type-Prefix Naming** - Establishes consistent naming conventions
- **MP071: Capitalization Convention** - Defines SYNC GIT as a capitalized NSQL directive
- **MP072: Cognitive Distinction Principle** - Ensures different language paradigms have distinct visual signatures

## Benefits

1. **Consistency**: Ensures consistent application of R008 requirements
2. **Automation**: Reduces manual steps in the synchronization process
3. **Standardization**: Provides a standardized interface for git operations
4. **Documentation**: Self-documents synchronization operations in code
5. **Cross-Platform**: Works across different environments and company projects

## Future Enhancements

The following enhancements are planned for the `SYNC GIT` implementation:

1. **Full Bidirectional Synchronization**: Implement pushing to all repositories when `$push_all = TRUE`
2. **Conflict Resolution Strategy**: Add parameters to control how conflicts are resolved
3. **Selective Synchronization**: Allow synchronizing specific files or directories
4. **Dependency-Aware Sync**: Respect dependencies between components during synchronization
5. **Changelog Generation**: Automatically generate changelogs based on commits

## Implementation Priority

To fully implement the `SYNC GIT` directive according to R008, several critical improvements are needed:

1. **Fix Execution Environment**: Address the path resolution and dependency issues that currently prevent reliable execution in different environments.

2. **Add Full Bidirectional Sync**: Extend the `update_global_scripts()` function to support:
   - Identifying the source repository where changes were made
   - Committing and pushing those changes to the source repository's remote
   - Pulling these changes into all other company repositories
   - Handling any conflicts that might arise during the synchronization process
   - Setting `$push_all=TRUE` as the default behavior

3. **Improve Error Handling**: Add robust error handling for repository access and network issues.

4. **Create Path-Independent Execution**: Allow the script to run correctly regardless of the current working directory.

5. **Develop Test Suite**: Create tests to verify synchronization worked correctly across repositories.

These improvements should be implemented as soon as possible to ensure full compliance with Rule R008 and to prevent the error-prone manual synchronization currently required.