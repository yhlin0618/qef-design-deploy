# Operation Confirmation Examples

Examples of how AI confirms understanding for data operations (not just queries).

---

## Example 1: Data Update

**User**: 把所有過期客戶標記為 inactive

**AI**: 我將執行以下操作：

```
UPDATE Customers
SET status = 'inactive'
WHERE last_purchase_date < today() - 180
```

預計影響 **247 筆** 記錄。確認執行嗎？

**User**: 對

**AI**: 已完成。247 位客戶已標記為 inactive。

---

## Example 2: Bulk Operation with Clarification

**User**: 刪除所有測試資料

**AI**: 「測試資料」需要確認範圍：

1. **email 包含 @test.com 的記錄**
2. **name 包含 "test" 的記錄**
3. **created_by = 'test_user' 的記錄**
4. **以上全部**

您想刪除哪些？

**User**: 1

**AI**: 我將執行以下操作：

```
DELETE FROM Customers
WHERE email LIKE '%@test.com'
```

預計刪除 **15 筆** 記錄。**此操作無法復原**。確認執行嗎？

**User**: 等等，先讓我看看是哪些

**AI**: 好的，讓我先列出這些記錄：

```
transform Customers to TestRecords
as customer_id, name, email, created_at
where email like '%@test.com'
```

[顯示15筆記錄]

確認這些都是要刪除的嗎？

---

## Example 3: Schema Change

**User**: 加一個欄位記錄客戶的 VIP 等級

**AI**: 我將執行以下操作：

```
ALTER TABLE Customers
ADD COLUMN vip_level VARCHAR(20) DEFAULT NULL
```

欄位規格：
- 名稱：`vip_level`
- 類型：VARCHAR(20)
- 預設值：NULL

確認新增嗎？

**User**: 對，但預設值改成 'standard'

**AI**: 了解，我更新操作：

```
ALTER TABLE Customers
ADD COLUMN vip_level VARCHAR(20) DEFAULT 'standard'
```

確認執行嗎？

**User**: 對

---

## Example 4: Data Import

**User**: 把這個 Excel 匯入客戶資料表

**AI**: 我分析了您的 Excel 檔案，匯入規劃如下：

```
IMPORT 'customers_2024.xlsx' INTO Customers
MAPPING:
  - Excel "客戶名稱" -> customer_name
  - Excel "電話" -> phone
  - Excel "Email" -> email
  - Excel "加入日期" -> created_at

OPTIONS:
  - 重複 email 處理：跳過（不覆蓋）
  - 空值處理：保留為 NULL
```

共 **156 筆** 記錄待匯入。確認執行嗎？

**User**: 重複的話用新的覆蓋

**AI**: 了解，我更新選項：

```
OPTIONS:
  - 重複 email 處理：覆蓋（使用新資料）
  - 空值處理：保留為 NULL
```

確認執行嗎？

---

## Key Principles for Operations

1. **Always show impact** - How many records affected
2. **Warn on destructive operations** - DELETE, DROP, TRUNCATE
3. **Allow preview before execute** - Especially for bulk operations
4. **Show exact changes** - No hidden side effects
5. **Confirm before execution** - Especially for irreversible operations
