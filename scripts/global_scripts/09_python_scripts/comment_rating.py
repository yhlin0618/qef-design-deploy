#!/usr/bin/env python
# -*- coding: utf-8 -*-

# 載入 .env 文件中的環境變量
load_dotenv()

# -----------------------------
# 參數與路徑設定
product = "開瓶器"
filename = "007_Bottle_Openers"
GPTfolder = os.path.abspath('../../data/GPTComments/')
asin_list = [
    "B0001YH1A2",
    "B08S359FPG",
    "B01N5SZTPR",
    "B0B2NZTQGQ",
    "B09FPL9GQT",
    "B0BL82L4YV",
    "B0BL7YBFCL",
    "B09ZKVP14F",
    "B007CE373O"
]
max_count = 1000001

# 定義檔案路徑
rating_path_feather = Path(f'{GPTfolder}/Comment_Property_Rating_{filename}_Dta.feather')
rating_path_xlsx = Path(f'{GPTfolder}/Comment_Property_Rating_{filename}_Dta.xlsx')
ratingonly_path_feather = Path(f'{GPTfolder}/Comment_Property_Ratingonly_{filename}_Dta.feather')
ratingonly_path_xlsx = Path(f'{GPTfolder}/Comment_Property_Ratingonly_{filename}_Dta.xlsx')
temp_folder_path = os.path.join(GPTfolder, 'temp')
if not os.path.exists(temp_folder_path):
    os.makedirs(temp_folder_path)
    print(f"Created temp folder at {temp_folder_path}")
else:
    print(f"Temp folder already exists at {temp_folder_path}")

# -----------------------------
# 讀取資料
print("讀取商品評論資料...")
Property_Rating = pd.read_feather(rating_path_feather)
print(f"總共有 {len(Property_Rating)} 筆評論資料。")

# 讀取屬性定義檔 (Excel)
print("讀取屬性定義檔...")
Property = pd.read_excel(Path(f'{GPTfolder}/Comment_Property_{filename}_Dta.xlsx'))
print(Property.head())

# -----------------------------
# 設定 OpenAI API
api_key = os.getenv("OPENAI_API_KEY")
if api_key is None:
    raise ValueError("API key not found. Please set the OPENAI_API_KEY environment variable.")
openai.api_key = api_key

# -----------------------------
# 定義輔助函式

def save_to_feather_with_timestamp(df, folder_path):
    """將 DataFrame 存成帶有時間戳記的 feather 檔案"""
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    file_path = os.path.join(folder_path, f"{timestamp}.feather")
    try:
        df.to_feather(file_path)
        print(f"Data successfully saved to {file_path}")
    except Exception as e:
        print(f"Error saving file: {e}")

# -----------------------------
# 屬性評分：針對每個屬性直接進行評分（不依賴 Analysis 欄位）
for idx, row in Property.iterrows():
    propertyname = row['Property']
    print(f"\n正在評分屬性：{propertyname}")

    # 若 Property_Rating 中沒有此屬性欄位，則新增
    if propertyname not in Property_Rating.columns:
        Property_Rating[propertyname] = pd.NA

    count = 0
    start_time = time.perf_counter()

    # 依序針對每筆評論進行評分
    for indexc, rowc in Property_Rating.iterrows():
        # 僅對 asin_list 中的商品進行評分
        if rowc['ASIN'] in asin_list and pd.isna(rowc[propertyname]):
            try:
                prompt = (
                    f"「{rowc['Body']}」, 顧客評論標題：「{rowc['Title']}」。\n"
                    f"請問這個人覺得這個產品的「{propertyname}」如何？\n"
                    f"請針對符合該屬性的程度評分：0 代表非常不符合、10 代表非常符合；"
                    f"如果評論中未提及該屬性，請填入 NaN。\n"
                    f"請回傳結果格式為「分數,理由」。"
                )
                # 使用 GPT 模型進行評分（請根據需求修改 model 名稱）
                completion = openai.ChatCompletion.create(
                    model="gpt-4o-mini",
                    messages=[{"role": "user", "content": prompt}]
                )
                response_text = completion.choices[0].message.content.strip()
                Property_Rating.at[indexc, propertyname] = response_text
                count += 1
                if count % 20 == 1:
                    save_to_feather_with_timestamp(Property_Rating, temp_folder_path)
                    print(f"正在處理屬性 '{propertyname}'；目前索引：{indexc}；累計評分：{count}")
            except Exception as e:
                print(f"Error processing index {indexc}: {e}")
            if count >= max_count:
                break
        if count >= max_count:
            break

    end_time = time.perf_counter()
    elapsed_time = end_time - start_time
    print(f"屬性 {propertyname} 評分完成，共 {count} 筆評分。")
    print(f"總耗時：{round(elapsed_time, 3)} 秒；平均每筆耗時：{round(elapsed_time/max(count,1), 3)} 秒。")

# -----------------------------
# 儲存最終結果
print("儲存最終結果...")
Property_Rating.to_feather(rating_path_feather)
Property_Rating.to_excel(rating_path_xlsx, index=False)

# -----------------------------
# 從評分結果中抽取純數字評分 (若需要)
print("從評分結果中抽取數字評分...")

# 先複製一份用來產生「僅評分」版本
Property_Ratingonly = Property_Rating.copy()

def find_number_or_nan(s):
    # 檢查是否包含 'NaN' (不區分大小寫)
    if isinstance(s, str) and ('nan' in s.lower()):
        return np.nan
    # 匹配 0~10 的數字（10 或單個數字）
    match = re.search(r'\b(10|[0-9])\b', s) if isinstance(s, str) else None
    if match:
        return int(match.group())
    else:
        return np.nan

def process_element(x):
    if isinstance(x, str):
        elements = re.split(r',|，', x)
        return find_number_or_nan(elements[0])
    else:
        return np.nan

# 只針對屬性欄位進行數字提取 (排除 key 欄位，若有 Analysis 欄位則也排除)
property_cols = [col for col in Property_Ratingonly.columns if col not in ['ASIN', 'Title', 'Body', 'time', 'Analysis']]
for col in property_cols:
    if Property_Ratingonly[col].dtype == 'object':
        Property_Ratingonly[col] = Property_Ratingonly[col].apply(process_element)

# 儲存「僅評分」結果
Property_Ratingonly.to_excel(ratingonly_path_xlsx, index=False)
Property_Ratingonly.to_feather(ratingonly_path_feather)

print("評分結果：")
print(Property_Ratingonly)