# dplyr Usage Guide

## Package Overview

dplyr is a grammar of data manipulation in R, providing a consistent set of verbs that help you solve the most common data manipulation challenges. It's the primary data wrangling package used in our precision marketing application.

## Core Components

| Component | Description | Documentation Link |
|-----------|-------------|-------------------|
| Data Pipeline | Standard data transformation pipeline | [data_pipeline.md](./data_pipeline.md) |
| Filtering | Row filtering operations | [filtering.md](./filtering.md) |
| Grouping & Summarizing | Aggregate operations | [grouping_summarizing.md](./grouping_summarizing.md) |
| Joining | Table combining operations | [joining.md](./joining.md) |
| Integration Examples | Project-specific examples | [integration.md](./integration.md) |

## Version Information

- **Required Version**: dplyr >= 1.1.0
- **Current Project Version**: 1.1.3
- **Key Dependencies**: rlang, tibble, tidyselect

## Related Principles

- **MP30**: Vectorization Principle
- **R50**: Data Table Vectorization
- **P81**: Tidyverse Shiny Terminology Alignment
- **R100**: Database Access tbl Rule
- **R101**: Unified tbl Data Access