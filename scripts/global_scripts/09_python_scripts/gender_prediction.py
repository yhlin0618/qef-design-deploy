# Gender_prediction_openai.py
from openai import OpenAI
import os
from dotenv import load_dotenv

# 載入 .env 文件中的環境變數
load_dotenv()

# 建立 OpenAI 客戶端，使用新 API 方式
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def predict_gender(name: str) -> str:
    """
    根據傳入的名字 (name) 使用 OpenAI API 預測性別。
    輸入範例： "Alice" 或 "Bob"，
    回傳： "female" 或 "male"；若無法明確判斷則回傳 API 回應的原始內容。
    
    使用新的客戶端實例方式及 GPT-4-turbo 模型（可依需要調整）。
    """
    prompt = (
        f"Based on the name '{name}', is this more likely to be a male or female name? "
        "Answer with only 'male' or 'female'."
    )
    
    try:
        completion = client.chat.completions.create(
            model="gpt-4-turbo",  # 新 API 推薦使用的模型名稱，請根據需要調整
            messages=[
                {"role": "system", "content": "You are a helpful assistant that predicts gender from a name."},
                {"role": "user", "content": prompt}
            ],
            max_tokens=10,
            temperature=0.0  # 設定低溫以獲得確定性高的答案
        )
    except Exception as e:
        return f"Error: {str(e)}"
    
    # 提取回應內容並標準化
    result = completion.choices[0].message.content.strip().lower()
    if "female" in result:
        return "female"
    elif "male" in result:
        return "male"
    else:
        return result

# 測試（可選，可在直接執行該腳本時測試）
#if __name__ == "__main__":
#    test_names = ["Alice", "Bob", "Jordan"]
#    for n in test_names:
#        print(f"{n}: {predict_gender(n)}")
