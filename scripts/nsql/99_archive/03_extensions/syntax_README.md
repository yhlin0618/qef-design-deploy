# NSQL Syntax Documentation

This directory contains syntax definition files for all NSQL language extensions. Each file follows the naming pattern `[extension]_syntax.md` and provides comprehensive documentation of the syntax supported by that extension.

## Available Syntax Definitions

- `table_creation_syntax.md` - Syntax for creating and modifying database tables
- `database_documentation_syntax.md` - Syntax for documenting database structures
- `graph_representation_syntax.md` - Syntax for creating and analyzing graph structures
- `specialized_phrases_syntax.md` - Syntax for specialized data processing operations
- `implementation_syntax.md` - Syntax for implementing directives in scripts
- `initialization_syntax.md` - Syntax for initialization and deinitialization

## Syntax File Structure

Each syntax file follows a consistent structure:

1. **Introduction** - Overview of the extension's purpose
2. **Directives** - Detailed documentation of each directive:
   - Basic syntax
   - Parameters
   - Examples
3. **R Code Generation** - How directives translate to R code
4. **Grammar** - Formal grammar in EBNF notation

## Using Syntax Documentation

These syntax files serve multiple purposes:

1. **Human Reference** - For developers writing NSQL directives
2. **Validator Input** - For validation tools checking NSQL syntax
3. **Parser Specification** - For implementing parsers for each extension
4. **Test Case Generation** - For creating test cases covering syntax variations

## Related Principles

This documentation follows these principles:

- **MP024: Natural SQL Language** - Defines the core NSQL approach
- **MP068: Language as Index Meta-Principle** - Uses language to index functionality
- **MP069: AI-Friendly Format Meta-Principle** - Uses Markdown for machine-readable documentation

## Contributing

To add a new syntax definition:

1. Create a file named `[extension]_syntax.md`
2. Follow the structure of existing syntax files
3. Include all directives supported by the extension
4. Document the EBNF grammar
5. Provide examples of valid syntax

## Implementation

The syntax defined in these files is implemented in corresponding extension files:

- Syntax in `table_creation_syntax.md` → Implemented in `table_creation_extension.R`
- Syntax in `implementation_syntax.md` → Implemented in `implementation_extension.R`

This separation of concerns follows clean architecture principles: the syntax documentation defines the interface, while the extensions implement the behavior.