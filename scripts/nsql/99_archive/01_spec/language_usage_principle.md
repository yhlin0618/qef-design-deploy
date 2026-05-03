# NSQL Language Usage Principle

## Core Principle

The language used in NSQL interactions must adapt to the context, user role, and purpose of the communication. Terminology choices should optimize for understanding and effectiveness, recognizing that different situations require different language approaches.

## Multi-Representation Framework

In NSQL, a fundamental principle is the recognition that any programming entity exists simultaneously in multiple representations. When communicating about code elements, always explicitly identify which representation you're referencing.

### Function Representation Example

Consider a function that calculates customer lifetime value. This single conceptual function exists in multiple representations:

| Representation Type | Example |
|---------------------|---------|
| **Concept** | Customer Lifetime Value calculation |
| **Mathematical Formula** | CLV = (ARPU × Gross Margin) ÷ Churn Rate |
| **NSQL Expression** | `calculate lifetime_value of customers` |
| **File Name** | `fn_calculate_clv.R` |
| **Function Object Name** | `calculate_clv` |
| **Function Signature** | `calculate_clv(customer_data, time_period = "lifetime")` |
| **SQL Translation** | `SELECT customer_id, SUM(revenue)*0.3/churn_probability AS clv FROM...` |
| **R Implementation** | `function(data) { ... }` |
| **Runtime Object** | The in-memory function object during execution |
| **UI Element** | "Calculate CLV" button in the interface |
| **Documentation Reference** | "The CLV calculation function" in user guides |
| **Error Message Reference** | "Error in calculate_clv(): insufficient data" |

### Terminology Guidelines

When referring to code elements, follow these guidelines to avoid confusion:

1. **Always specify the representation**: Indicate whether you're referring to the file, the function object, the concept, etc.

2. **Use consistent reference patterns**:
   - File names use the "fn_" prefix (e.g., `fn_calculate_clv.R`)
   - Function object names do not use the prefix (e.g., `calculate_clv`)
   - NSQL expressions use natural language forms (e.g., `calculate lifetime_value`)

3. **In documentation, clarify transitions between representations**:
   - "The NSQL expression `calculate lifetime_value` translates to the SQL function `CLV()`"
   - "The file `fn_calculate_clv.R` contains the definition of the `calculate_clv` function"

## Context-Based Language Selection

### 1. User Interaction Contexts

| Context | Primary Objective | Language Approach | Example |
|---------|-------------------|-------------------|---------|
| **Disambiguation** | Resolve ambiguity | Natural, everyday language | "Which Excel file do you want to use?" |
| **Explanation** | Explain implementation | Precise technical terms | "The SQL query joins the customers table with the orders table using a left join on customer_id." |
| **Instruction** | Teach usage | Mixed, with definitions | "This creates a view (a saved query that acts like a table) from your data." |
| **Error Communication** | Alert to problems | Simple problem description | "The file couldn't be opened because it's being used by another program." |
| **Results Presentation** | Share insights | Domain-specific business terms | "Customer acquisition cost increased by 15% in Q1." |

### 2. User Role Considerations

| User Type | Technical Knowledge | Language Approach | Example |
|-----------|---------------------|-------------------|---------|
| **Business User** | Low-Medium | Natural, business-focused | "This shows your top customers by total spending." |
| **Analyst** | Medium-High | Mix of technical and business | "The clustering algorithm grouped customers based on RFM metrics." |
| **Data Engineer** | High | Precise technical | "The transformation uses a window function with partitioning by customer_id." |
| **Developer** | High | Implementation-specific | "The SQL generated uses a CTE to handle the hierarchical structure." |

### 3. Communication Purpose

| Purpose | Focus | Language Approach | Example |
|---------|-------|-------------------|---------|
| **Clarification** | Remove ambiguity | Natural, accessible | "Do you want to look at this year's or last year's data?" |
| **Education** | Build understanding | Progressive disclosure | "This joins related information - like connecting customer names to their orders." |
| **Implementation Details** | Technical accuracy | Precise terminology | "The query uses an index on the timestamp column for performance." |
| **Business Insights** | Value communication | Outcome-focused | "This shows which product categories drive repeat purchases." |

## Implementation Guidelines

### 1. Context and Role Detection

Systems should detect interaction context and user role through:

#### Explicit Detection:
- Explicit user role settings or direct questions
- User-selected language preference settings
- Session-based role information

#### Implicit Detection:
- Analysis of user's language patterns and terminology usage
- Complexity of queries and operations requested
- Domain-specific terms and jargon employed by the user
- Technical specificity in user's questions and responses

#### Language Pattern Analysis for Role Detection:

| Language Pattern | Likely Role | Example |
|------------------|-------------|---------|
| Business metrics and KPIs | Business User | "Show me customer acquisition cost by channel" |
| Statistical terms without implementation details | Analyst | "I need a regression of sales on advertising spend" |
| Data structure terms and transformation concepts | Data Engineer | "Join the customer table with transactions and aggregate by month" |
| Implementation specifics and optimization concerns | Developer | "Use a window function with partitioning by ID for the calculation" |

The system should:
- Start with a neutral, slightly conservative language level
- Analyze initial interactions to calibrate language complexity
- Adjust progressively based on continued interaction
- Periodically validate role assumptions with explicit questions when appropriate

### 2. Adaptive Language Selection

The system should:
- Default to natural language for disambiguation
- Use technical language when explaining implementations
- Adjust language precision based on detected user expertise
- Allow users to request more or less technical detail

### 3. Progressive Disclosure

When uncertainty exists about appropriate language level:
- Start with more accessible language
- Provide ways to "drill down" into technical details
- Offer explanations of technical terms when used
- Build vocabulary through consistent usage and definition

### 4. Documentation Standards

Documentation should:
- Specify the intended audience for each section
- Use language appropriate to that audience
- Define technical terms on first use
- Maintain consistent terminology throughout a section

## Language Switching Indications

The following phrases signal appropriate language changes:

### 1. Shifting to Technical Language

- "In technical terms, this means..."
- "The underlying implementation uses..."
- "From a database perspective, this represents..."
- "The specific syntax being generated is..."

### 2. Shifting to Natural Language

- "In everyday terms, this is like..."
- "A simpler way to think about this is..."
- "The business meaning of this is..."
- "To explain this more simply..."

## Examples of Context-Appropriate Language

### Example 1: Multiple Approaches to the Same Concept

**Concept**: Joining customer and order data

**For Business Users (Disambiguation)**:
```
Do you want to combine customer information with their orders?
```

**For Analysts (Explanation)**:
```
This joins the customer dataset with the orders dataset using the customer ID as the common field.
```

**For Technical Users (Implementation)**:
```
This generates a LEFT JOIN between customers and orders tables on the customer_id column, preserving all customer records even without matching orders.
```

### Example 2: Error Communication

**For Business Users**:
```
We couldn't find any sales data for the date range you specified.
```

**For Analysts**:
```
The query returned zero records when filtering for the specified date range.
```

**For Technical Users**:
```
Query execution returned empty result set. Date range filter '2025-04-01 00:00:00' to '2025-04-04 23:59:59' matched no records in sales_transactions table.
```

## Benefits

1. **Enhanced Communication**: Better understanding through appropriate language
2. **Reduced Frustration**: Less confusion from mismatched terminology
3. **Efficient Interactions**: Faster resolution of tasks through clearer communication
4. **User Respect**: Shows respect for users' different backgrounds and needs
5. **Learning Support**: Helps users gradually build technical understanding

## Relationship to Other Rules and Principles

This principle works in conjunction with:
- **Natural Language Rule**: Governs language for user disambiguation
- **Reference Resolution Rule**: Guides how ambiguities are identified and resolved
- **Default Rules**: Establishes standard interpretations

## Conclusion

The Language Usage Principle recognizes that effective communication requires adapting language to context, audience, and purpose. By varying terminology appropriately between natural language and technical precision, NSQL provides an optimal experience for all users while maintaining the ability to precisely communicate both intent and implementation details.