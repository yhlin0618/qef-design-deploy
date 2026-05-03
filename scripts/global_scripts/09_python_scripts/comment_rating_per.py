# comment_per.py
import openai

def rate_comment(title, body, product_line_name, property_name, property_type, gpt_key, model="o4-mini"):
    """
    使用 GPT 模型對單筆評論進行評分。

    參數:
      title: 評論標題
      body: 評論內容
      product_line_name: 生產線名稱
      property_name: 要評分的屬性名稱
      property_type: 評分類別，例如 "屬性" 或 "品牌個性"
      gpt_key: GPT API 金鑰
      model: 使用的 GPT 模型，預設為 "gpt-4o-mini"

    回傳:
      GPT 回應的評分結果字串，格式預期為 "[分數,理由]"，
      若發生錯誤則回傳錯誤訊息。
    """
    # 建立 OpenAI 客戶端，並指定 API 金鑰
    client = openai.OpenAI(api_key=gpt_key)

    # 構造提示訊息
    message_text = (f"""
The following is a comment on a {product_line_name} product:
Title: {title}
Body: {body}
Evaluate the comment regarding the product's '{property_name}', which is categorized as a {property_type} feature.
Use the following rules to respond:
1. If the comment does not demonstrate the stated characteristic in any way, reply exactly [NaN,NaN] without any additional reasoning or explanation.
2. Otherwise, rate your agreement with the statement on a scale from 1 to 5:
- ‘5’ for Strongly Agree
- ‘4’ for Agree
- ‘3’ for Neither Agree nor Disagree
- ‘2’ for Disagree
- ‘1’ for Strongly Disagree
Provide your rationale in the format: [Score, Reason].
** Please double-check that if the comment does not demonstrate the stated characteristic in any way, your reply is exactly [NaN,NaN] with no extra explanation.
""")

    try:
        # 呼叫新版 API 來產生回應
        completion = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": "Forget any previous information."},
                {"role": "user", "content": message_text}
            ]
        )
        # 根據回應結構取得產生的文字
        return completion.choices[0].message.content.strip()
    except Exception as e:
        return f"Error: {e}"
