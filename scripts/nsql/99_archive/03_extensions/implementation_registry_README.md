# Implementation Phrase Registry (CSV Version)

This directory contains the CSV-based implementation phrase registry for the NSQL language system.

## Registry Structure

The registry is split into multiple CSV files for easier maintenance:

1. `implementation_registry_core_phrases.csv` - Core implementation phrases (initialization, database connections)
2. `implementation_registry_table_creation_phrases.csv` - Table creation and modification phrases
3. `implementation_registry_db_documentation.csv` - Database documentation phrases
4. `implementation_registry_graph_phrases.csv` - Graph representation and analysis phrases
5. `implementation_registry_specialized_phrases.csv` - Specialized data processing phrases
6. `implementation_registry_examples.csv` - Example implementations using phrases

## CSV Format

Each registry file uses a common CSV format with the following columns:

- **Phrase ID**: Unique identifier for the phrase (e.g., INI001)
- **Directive**: The high-level action (e.g., INITIALIZE, CONNECT)
- **Context**: Where or how the directive applies (e.g., UPDATE_MODE, APP_DATA)
- **Implementation Code**: The actual R code that implements the directive
- **Description**: Brief description of what the phrase does
- **Parameters**: Customizable parameters in the implementation code
- **Extension**: Which extension the phrase belongs to
- **Added Date**: When the phrase was added
- **Added By**: Who added the phrase
- **Status**: Current status (Active, Deprecated, etc.)

## Adding New Phrases

To add a new phrase:

1. Choose the appropriate CSV file based on the phrase's domain
2. Add a new row with a unique Phrase ID
3. Fill in all required columns
4. Ensure parameters are documented in the Parameters column

## Referencing Phrases

In NSQL implementation directives, reference phrases using the pattern:

```
DIRECTIVE IN CONTEXT
```

Or for directives without context:

```
DIRECTIVE_NAME
```

## Examples

For usage examples, see the `implementation_registry_examples.csv` file or the example files in the IMPLEMENT directory.

## Meta-Principle

This registry implements MP068: Language as Index Meta-Principle, which recognizes that language constructs serve as powerful indexing mechanisms for knowledge, processes, and implementations.