# NSQL Natural Language Rule

## Core Rule

When asking questions or seeking clarification from users, NSQL implementations must employ familiar, accessible terminology rather than technical jargon. All disambiguation questions, prompts, and interactive feedback should prioritize natural, intuitive language that matches users' mental models rather than system implementation details.

## Terminology Guidelines

### Use Domain-Specific Language, Not Technical Terms

| Use This | Instead Of | Context |
|----------|------------|---------|
| dataset | schema, table | When referring to collections of related data |
| Excel file | .xlsx, .xls | When referring to spreadsheet files |
| CSV file | .csv | When referring to comma-separated files |
| text file | .txt | When referring to plain text files |
| report | output table, result set | When referring to query results |
| dashboard | visualization collection | When referring to multiple visualizations |
| customer information | customer entity | When referring to customer data |
| sales data | transaction records | When referring to sales information |
| date range | time period parameters | When referring to time constraints |
| filter | WHERE clause | When referring to data filtering |
| sorting | ORDER BY clause | When referring to result ordering |

### Use Natural Time References

| Use This | Instead Of | Context |
|----------|------------|---------|
| last month | previous 30 days | When referring to the previous calendar month |
| yesterday | prior date | When referring to the previous day |
| this year to date | current year partial | When referring to the current year |
| January to March | Q1, first quarter | When referring to specific months |
| Monday to Friday | weekdays | When referring to business days |
| morning, afternoon, evening | time ranges | When referring to parts of the day |

### Use Familiar Metric Terms

| Use This | Instead Of | Context |
|----------|------------|---------|
| total sales | sum of transaction amounts | When referring to sales metrics |
| average order size | mean transaction value | When referring to order metrics |
| customer count | distinct user count | When referring to customer metrics |
| top sellers | highest revenue products | When referring to product performance |
| sales growth | period-over-period delta | When referring to change over time |

## Implementation in Questions

When asking disambiguation questions:

### DO:
- "Which dataset contains customer information?"
- "Do you want to analyze data from this year or last year?"
- "Would you like to see total sales or average order size?"
- "Should the Excel file be saved to your reports directory?"
- "Do you want to filter the data to show only US customers?"

### DON'T:
- "Which schema contains the customer entity?"
- "What is the temporal parameter range for your query?"
- "Specify the aggregation function for the transaction amount field."
- "Select the output format for the XLSX export."
- "Define the WHERE clause predicates for geographical filtering."

## Implementation in Feedback

When providing feedback about operations:

### DO:
- "Found 3 datasets with customer information. Which one would you like to use?"
- "Your sales report has been saved to the reports directory."
- "The dashboard now shows sales by region for January to March."
- "Filtered results to include only orders over $100."

### DON'T:
- "Query returned 3 table entities matching 'customer' in metadata."
- "Output persisted to filesystem at specified path with XLSX encoding."
- "Visualization parameters updated to reflect Q1 temporal constraints."
- "Filter condition applied: transaction_amount > 100."

## Error Messages

Error messages should be especially clear and non-technical:

### DO:
- "The Excel file 'Customers.xlsx' couldn't be found. Would you like to browse for it?"
- "We don't have permission to access the sales dataset. Would you like to request access?"
- "The dates you specified don't make sense - the end date is before the start date."

### DON'T:
- "File read error: XLSX parser exception on input file."
- "Schema access violation: insufficient privileges for SELECT operation."
- "Logical error in temporal parameter sequence detected."

## Disambiguation Questions

When resolving ambiguities, questions should:

1. **Use Everyday Language**: Frame questions in terms users would naturally use
2. **Provide Context**: Explain why clarification is needed in simple terms
3. **Offer Meaningful Choices**: Describe options in business terms, not technical ones
4. **Include Examples**: When appropriate, provide examples of what you're asking for

## Examples of Natural Language Disambiguation

### Example 1: Table Ambiguity

**Natural Language Approach**:
```
I found several datasets with customer information:
1. Customer accounts (updated daily, contains 10,000 records)
2. Customer survey responses (from January 2025, contains 1,500 records)
3. Customer support interactions (last 6 months, contains 5,200 records)

Which dataset would you like to use?
```

**Technical Approach to Avoid**:
```
Multiple tables match the entity 'customer':
1. customer_master.customers (updated: daily, row count: 10000)
2. marketing.survey_responses (timestamp: 2025-01-15, row count: 1500)
3. support.customer_tickets (range: P6M, row count: 5200)

Select table for query execution:
```

### Example 2: Time Period Ambiguity

**Natural Language Approach**:
```
When you say "last quarter," do you mean:
1. January to March 2025 (Calendar Q1)
2. The last 3 months (January 4 to April 4, 2025)
3. The previous business quarter (based on your company's fiscal year)
```

**Technical Approach to Avoid**:
```
Ambiguous temporal parameter "last_quarter":
1. Calendar Q1 2025 (2025-01-01 to 2025-03-31)
2. Relative -3 months (2025-01-04 to 2025-04-04)
3. Fiscal Q3 2024 (based on configured fiscal calendar)
```

## Benefits

1. **Reduced Cognitive Load**: Users don't need to translate between their terminology and system terminology
2. **Increased Accessibility**: Makes NSQL accessible to non-technical users
3. **Faster Learning**: Matches users' existing mental models
4. **Reduced Errors**: Clearer communication leads to fewer misunderstandings
5. **Better User Experience**: Creates a more conversational, natural interaction

## Implementation Notes

1. Maintain a domain-specific glossary that maps technical terms to natural language equivalents
2. Update question templates to use natural language terms
3. Test language with actual users to ensure it matches their mental models
4. When technical terms are unavoidable, provide brief explanations
5. Consider different domains may have different natural language terms for the same concepts