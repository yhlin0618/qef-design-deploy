# Disambiguation Flow Examples

Examples of how AI handles ambiguous requests through structured clarification.

---

## Example 1: Undefined Business Term

**User**: 找出高價值客戶

**AI**: 「高價值客戶」有幾種定義方式：

1. **RFM-based**（推薦）：RFM 分數皆 ≥ 4
2. **Revenue-based**：年消費 > $10,000
3. **Percentile-based**：消費 Top 10%

您想用哪一個？

**User**: 用第一個

**AI**: 我理解您要的是：

```
transform Customers to HighValueCustomers
as customer_id, customer_name, rfm_score
where rfm_r >= 4 and rfm_f >= 4 and rfm_m >= 4
```

這樣對嗎？

**User**: 對

---

## Example 2: Vague Time Reference

**User**: 看上個月的銷售報告

**AI**: 「上個月」可以有不同解釋：

1. **日曆月**（推薦）：2024-11-01 至 2024-11-30
2. **滾動30天**：過去30天（2024-11-24 至 2024-12-23）

您指的是哪一個？

**User**: 日曆月

**AI**: 我理解您要的是：

```
transform Sales to MonthlySalesReport
as sum(revenue) as total_revenue,
   count(order_id) as order_count,
   average(order_amount) as avg_order_value
where date >= '2024-11-01' and date <= '2024-11-30'
```

這樣對嗎？

---

## Example 3: Polysemous Term

**User**: 各區的銷售表現

**AI**: 「銷售」可能指：

1. **營收金額** (sum of revenue)
2. **訂單數量** (count of orders)
3. **銷售件數** (sum of quantity)

您想看的是？

**User**: 營收金額

**AI**: 我理解您要的是：

```
transform Sales to RegionPerformance
as region, sum(revenue) as total_revenue
grouped by region
ordered by total_revenue desc
```

這樣對嗎？

---

## Example 4: Implicit Aggregation

**User**: 每個客戶的購買頻率

**AI**: 「購買頻率」您想如何呈現？

1. **總訂單數**：該客戶的總訂單筆數
2. **月平均**：每月平均訂單數
3. **天數間隔**：平均購買間隔天數

您想用哪一種？

**User**: 月平均

**AI**: 我理解您要的是：

```
transform Orders to CustomerFrequency
as customer_id,
   count(order_id) / count(distinct month(order_date)) as monthly_avg_orders
grouped by customer_id
ordered by monthly_avg_orders desc
```

這樣對嗎？

---

## Example 5: Multiple Ambiguities

**User**: 活躍客戶最近的消費

**AI**: 這個請求有幾個需要確認的地方：

**1. 「活躍客戶」的定義：**
- A. 過去90天內有消費（推薦）
- B. 季消費 ≥ 3 次

**2. 「最近」的範圍：**
- A. 最後一筆訂單
- B. 過去7天
- C. 過去30天

請告訴我您的選擇？

**User**: 用A和A

**AI**: 我理解您要的是：

```
transform Orders to ActiveCustomerRecent
as customer_id, customer_name,
   max(order_date) as last_order_date,
   max(order_amount) as last_order_amount
where last_purchase_date >= today() - 90
grouped by customer_id, customer_name
ordered by last_order_date desc
```

這樣對嗎？

---

## Key Principles

1. **Identify ALL ambiguities first** - Don't guess
2. **Provide options with recommended default** - Help user decide
3. **Explain each option briefly** - User can make informed choice
4. **Confirm final understanding** - No hidden assumptions
5. **Allow iterative refinement** - User can still correct after confirmation
