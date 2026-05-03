# NSQL: Human-AI Confirmation Protocol

A structured format for AI to confirm understanding of human requests, eliminating ambiguity through dialogue.

> **Key Insight**: NSQL is not a language humans need to learn. It's a format AI uses to show what it understood, so humans can confirm or correct.

## How It Works

```
┌─────────────────────────────────────────────────────────────────┐
│                     Confirmation Loop                           │
│                                                                 │
│   Human                        AI                               │
│     │                           │                               │
│     │  "看上個月各區銷售"        │                               │
│     │ ──────────────────────>   │                               │
│     │                           │  Parse & identify ambiguity   │
│     │                           │                               │
│     │   [NSQL Confirmation]     │                               │
│     │ <──────────────────────   │                               │
│     │                           │                               │
│     │  "對" / "不對，改成..."    │                               │
│     │ ──────────────────────>   │                               │
│     │                           │                               │
│     │   [Execute or Iterate]    │                               │
│     │ <──────────────────────   │                               │
└─────────────────────────────────────────────────────────────────┘
```

## Example

**User**: 找出高價值客戶

**AI**: 「高價值客戶」有幾種定義：

1. RFM 分數 ≥ 4（推薦）
2. 年消費 > $10,000
3. Top 10% 消費者

您想用哪一個？

**User**: 1

**AI**: 我理解您要的是：

```
transform Customers to HighValueCustomers
as customer_id, customer_name, rfm_score
where rfm_r >= 4 and rfm_f >= 4 and rfm_m >= 4
```

這樣對嗎？

**User**: 對

**AI**: [執行查詢]

## Documentation

| Document | Description |
|----------|-------------|
| **[Concept Paper](docs/concept.md)** | Theoretical foundations and design rationale |
| **[Implementation Guide](docs/guide.md)** | Practical guide for integrating NSQL in your AI system |

## Directory Structure

```
nsql/
├── README.md           # This file
├── protocol.yaml       # Confirmation protocol specification
├── dictionary.yaml     # Terminology definitions
├── grammar.ebnf        # Formal grammar (reference only)
│
├── docs/               # Documentation
│   ├── concept.md      # Concept paper (whitepaper)
│   └── guide.md        # Implementation guide
│
├── examples/           # Dialogue examples
│   ├── query_confirmation.md
│   ├── disambiguation_flow.md
│   └── operation_confirmation.md
│
└── 99_archive/         # Historical specifications
```

## Core Files

| File | Purpose |
|------|---------|
| `protocol.yaml` | Defines confirmation formats, triggers, and workflow |
| `dictionary.yaml` | Business terms, time references, aggregation functions |
| `docs/` | Concept paper and implementation guide |
| `examples/` | Real dialogue examples showing the protocol in action |

## Key Principles

1. **Read-Only for Humans** - Users confirm/correct, they don't write NSQL
2. **Structured but Readable** - Clear enough for non-technical users
3. **Dynamic Confirmation** - Achieve consensus through dialogue
4. **Eliminate Ambiguity** - Clarify before execute, never guess

## When to Use NSQL Confirmation

- Data queries with potential ambiguity
- Bulk operations that need user approval
- Business term interpretations
- Time range clarifications
- Any request where assumptions could lead to wrong results

## Benefits

| vs Pure Natural Language | vs Technical Query Languages |
|-------------------------|------------------------------|
| No hidden assumptions | No learning curve for users |
| Explicit confirmation | Readable by non-programmers |
| Traceable understanding | Still unambiguous |

---

*Version 2.0.0 | Repositioned as Confirmation Protocol | 2025-12-24*
