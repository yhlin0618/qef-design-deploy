# Query Confirmation Examples

Examples of how AI uses NSQL format to confirm understanding of data queries.

---

## Example 1: Simple Query

**User**: 看一下上個月各區的銷售

**AI**: 我理解您要的是：

```
transform Sales to RegionReport
as sum(revenue) as 總營收
grouped by region
where date in [2024-11-01, 2024-11-30]
```

這樣對嗎？

**User**: 對

**AI**: [執行查詢並顯示結果]

---

## Example 2: Top N Query

**User**: 找出銷售最好的前10個產品

**AI**: 我理解您要的是：

```
transform Sales to TopProducts
as product_name, sum(revenue) as total_revenue
grouped by product_name
ordered by total_revenue desc
limit 10
```

這樣對嗎？

**User**: 對，但我要的是數量不是金額

**AI**: 了解，我更新確認：

```
transform Sales to TopProducts
as product_name, count(order_id) as order_count
grouped by product_name
ordered by order_count desc
limit 10
```

這樣對嗎？

**User**: 對

---

## Example 3: Filtered Query

**User**: 計算VIP客戶的平均訂單金額

**AI**: 我理解您要的是：

```
transform Orders to VIPMetrics
as average(order_amount) as avg_order_value
where customer_type = 'VIP'
```

這樣對嗎？

**User**: 對

---

## Example 4: Multi-Dimension Query

**User**: 看看各區各月的營收趨勢

**AI**: 我理解您要的是：

```
transform Sales to RevenueTrend
as sum(revenue) as monthly_revenue
grouped by region, month(date)
ordered by month(date) asc
```

這樣對嗎？

**User**: 對，但只看今年的

**AI**: 了解，我加上時間篩選：

```
transform Sales to RevenueTrend
as sum(revenue) as monthly_revenue
grouped by region, month(date)
where year(date) = 2024
ordered by month(date) asc
```

這樣對嗎？

**User**: 對

---

## Key Points

1. **AI presents structured confirmation** - User doesn't need to write NSQL
2. **User only confirms or corrects** - Simple "對/不對" responses
3. **Iterative refinement** - If wrong, AI updates and re-confirms
4. **Both parties reach consensus** - No ambiguity when execution starts
