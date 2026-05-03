# shiny: Reactive Programming

## Description

Reactive programming is the foundation of Shiny's interactivity. It automatically updates outputs when inputs change, creating a responsive application without manual event handling.

## Code Example

```r
# Basic reactive example
ui <- fluidPage(
  numericInput("n", "Number of observations", 100),
  plotOutput("histogram")
)

server <- function(input, output) {
  # Reactive expression
  data <- reactive({
    rnorm(input$n)
  })
  
  # Output that depends on reactive expression
  output$histogram <- renderPlot({
    hist(data())
  })
}

shinyApp(ui, server)
```

## Reactive Constructs

| Construct | Description | Example Usage |
|-----------|-------------|---------------|
| reactive() | Create a reactive expression | data <- reactive({ calc(input$x) }) |
| render*() | Create reactive output | renderPlot({ plot(data()) }) |
| observe() | Execute code when dependencies change | observe({ log(input$x) }) |
| observeEvent() | Execute code when specific events happen | observeEvent(input$button, { action() }) |
| eventReactive() | Create reactive expression triggered by event | data <- eventReactive(input$button, { calc() }) |
| isolate() | Access reactive value without creating dependency | isolate(input$value) |
| req() | Require values to be available and valid | req(input$selection) |

## Notes

- Follow R102 Shiny Reactive Observation Rule for all reactive constructs
- Reactive expressions (reactive()) should be used for calculations needed by multiple outputs
- Use observeEvent() rather than observe() when responding to specific inputs
- Always call reactive expressions with parentheses: data()
- Use req() to ensure inputs have valid values before computation
- Follow MP52 Unidirectional Data Flow in your reactive chain
- Use isolate() when you need to read a reactive value without creating a dependency
- Beware of circular reactive dependencies

## Related Usage Patterns

- [Observers & Handlers](./observers.md): Detailed event handling
- [UI Components](./ui_components.md): UI elements with reactive behavior
- [Integration with reactive databases](./integration.md)