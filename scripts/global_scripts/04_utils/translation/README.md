# Translation System

This component provides translation functionality for multilingual support in Shiny applications, following the R108 Function Deployment Rule.

## Key Features

- Early initialization of translation functions in the application lifecycle
- Proper global assignment of functions
- Explicit function passing to components
- Fallback mechanisms for reliability
- Support for multiple languages
- Dynamic language switching

## Usage

### Basic Setup

Add this to your application initialization script:

```r
# Source the translation package
source("path/to/04_utils/translation/package.R")

# Initialize with default language (English)
translation_package$initialize_translation_system(language = "en")

# Now the translate function is available globally
# Example usage:
translate("Hello")  # Returns "Hello"
```

### With Translation Dictionary

For actual translations:

```r
# Create or load a translation dictionary
dict <- list(
  "en" = list(
    "Hello" = "Hello",
    "Goodbye" = "Goodbye"
  ),
  "zh" = list(
    "Hello" = "您好",
    "Goodbye" = "再見"
  )
)

# Initialize with dictionary
translation_package$initialize_translation_system(
  language = "en",
  translation_dict = dict
)

# Use translations
translate("Hello")  # Returns "Hello" in English

# Switch language at runtime
set_language("zh")
translate("Hello")  # Returns "您好" in Chinese
```

### In Components

When creating components that need translation:

```r
my_component <- function(id, translate_fn = NULL) {
  # Get translate function or use fallback
  if (is.null(translate_fn)) {
    translate_fn <- get_translate_function()
  }
  
  # Create component UI with translations
  ui <- function() {
    div(
      h3(translate_fn("Component Title")),
      p(translate_fn("Description text"))
    )
  }
  
  # Create component server logic
  server <- function(input, output, session) {
    # Component logic here
  }
  
  # Return the component
  return(list(
    ui = ui,
    server = server
  ))
}
```

## Functions

- `initialize_translation_system(language, translation_dict)`: Sets up the translation system
- `get_translate_function()`: Gets the translation function with fallback
- `translate(text, lang)`: Translates text (available after initialization)
- `set_language(new_language)`: Changes the current language (available after initialization)
- `update_translations(new_dict)`: Updates or adds translations (available after initialization)
- `create_sample_translation_dict()`: Creates a sample dictionary for testing

## Example Implementation

See the example in `package.R` for detailed implementation patterns.

## Related Principles

- **R108**: Function Deployment Rule
- **R21**: One Function One File
- **MP31**: Initialization First
- **R95**: Import Requirements Rule
- **R54**: Component Folder Organization