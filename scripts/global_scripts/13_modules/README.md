# Claude Prompts for Precision Marketing

This directory contains prompting resources and guidance for interacting with Claude AI to support the Precision Marketing projects.

## No-Duplication Rule

**CRITICAL**: Following our DRY (Don't Repeat Yourself) principle, we strictly prohibit duplicating any principles, guidelines, or architectural documentation. This ensures we maintain a single source of truth for all project guidance.

### Why This Matters:
- Prevents inconsistencies when updates occur
- Reduces maintenance burden
- Ensures everyone follows the same principles
- Preserves project cohesion

## Principle References

**Important Note**: The definitive project principles are maintained in the `00_principles` directory. Do not duplicate these files here. Always reference them directly.

To understand the project architecture and principles, please refer to these core documents:

1. [Code Organization Hierarchy](../00_principles/01_code_organization_hierarchy.md)
2. [Project Principles](../00_principles/02_project_principles.md)
3. [Script Separation Principles](../00_principles/03_script_separation_principles.md)
4. [Claude Interaction Principles](../00_principles/14_claude_interaction_principles.md)

## Directory Structure

- `/global/`: Prompts applicable to all company projects
- `/KitchenMAMA/`: Prompts specific to KitchenMAMA
- `/WISER/`: Prompts specific to WISER
- `/Example_Company/`: Templates for new company integrations

## Module Mapping

Each project contains a `module_mapping.md` file that maps between conceptual modules and their implementations:

- [KitchenMAMA Module Mapping](KitchenMAMA/module_mapping.md)
- [WISER Module Mapping](WISER/module_mapping.md)

## Working with Claude

When using Claude to assist with development:

1. Refer to [Claude Interaction Principles](../00_principles/14_claude_interaction_principles.md) for guidance
2. Use module mappings to help Claude locate relevant files
3. Ensure Claude is working in the correct project context
4. Have Claude sync changes across projects when modifying global scripts
5. Remember that Claude should only revise code when there are sufficient reasons
6. Claude should ask for guidance when multiple approaches are possible

## File Maintenance

- These prompt files should not duplicate content from the principles directory
- Update module mappings when adding or changing modules
- Maintain company-specific implementation details in the company-specific directories