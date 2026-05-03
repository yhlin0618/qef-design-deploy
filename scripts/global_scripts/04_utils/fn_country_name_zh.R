# =============================================================================
# fn_country_name_zh.R — ISO-2 Country Code → Traditional Chinese Name
# Following: DEV_R050 (externalized display data), DEV_R052 (English keys internal),
#            UI_R025 (Taiwan Traditional Chinese), SO_R007 (one function one file)
# Issue: #330
# =============================================================================

#' Convert ISO-2 country codes to Traditional Chinese names
#'
#' @param iso2_codes Character vector of ISO-2 country codes (e.g., "US", "JP")
#' @param with_code  Logical. If TRUE, format as "中文名（ISO）" (UI_R027 pattern).
#'                   Default FALSE returns pure Chinese name.
#' @return Character vector of same length; unknown codes returned as-is.
country_name_zh <- function(iso2_codes, with_code = FALSE) {
  # Named vector: ISO-2 → 臺灣正體中文 (UI_R025)
  iso2_zh <- c(
    AD = "\u5b89\u9053\u723e",         # 安道爾
    AE = "\u963f\u806f\u914b",         # 阿聯酋
    AF = "\u963f\u5bcc\u6c57",         # 阿富汗
    AG = "\u5b89\u5730\u5361\u53ca\u5df4\u5e03\u9054", # 安地卡及巴布達
    AL = "\u963f\u723e\u5df4\u5c3c\u4e9e", # 阿爾巴尼亞
    AM = "\u4e9e\u7f8e\u5c3c\u4e9e",   # 亞美尼亞
    AO = "\u5b89\u54e5\u62c9",         # 安哥拉
    AR = "\u963f\u6839\u5ef7",         # 阿根廷
    AT = "\u5967\u5730\u5229",         # 奧地利
    AU = "\u6fb3\u6d32",               # 澳洲
    AZ = "\u4e9e\u585e\u62dc\u7136",   # 亞塞拜然
    BA = "\u6ce2\u58eb\u5c3c\u4e9e\u8207\u8d6b\u585e\u54e5\u7dad\u7d0d", # 波士尼亞與赫塞哥維納
    BB = "\u5df4\u8c9d\u591a",         # 巴貝多
    BD = "\u5b5f\u52a0\u62c9",         # 孟加拉
    BE = "\u6bd4\u5229\u6642",         # 比利時
    BF = "\u5e03\u5409\u7d0d\u6cd5\u7d22", # 布吉納法索
    BG = "\u4fdd\u52a0\u5229\u4e9e",   # 保加利亞
    BH = "\u5df4\u6797",               # 巴林
    BJ = "\u8c9d\u5357",               # 貝南
    BM = "\u767e\u6155\u9054",         # 百慕達
    BN = "\u6c76\u840a",               # 汶萊
    BO = "\u73bb\u5229\u7dad\u4e9e",   # 玻利維亞
    BR = "\u5df4\u897f",               # 巴西
    BS = "\u5df4\u54c8\u99ac",         # 巴哈馬
    BT = "\u4e0d\u4e39",               # 不丹
    BW = "\u6ce2\u672d\u90a3",         # 波札那
    BY = "\u767d\u4fc4\u7f85\u65af",   # 白俄羅斯
    BZ = "\u8c9d\u91cc\u65af",         # 貝里斯
    CA = "\u52a0\u62ff\u5927",         # 加拿大
    CD = "\u525b\u679c\u6c11\u4e3b\u5171\u548c\u570b", # 剛果民主共和國
    CH = "\u745e\u58eb",               # 瑞士
    CI = "\u8c61\u7259\u6d77\u5cb8",   # 象牙海岸
    CL = "\u667a\u5229",               # 智利
    CM = "\u5580\u9ea5\u9686",         # 喀麥隆
    CN = "\u4e2d\u570b",               # 中國
    CO = "\u54e5\u502b\u6bd4\u4e9e",   # 哥倫比亞
    CR = "\u54e5\u65af\u5927\u9ece\u52a0", # 哥斯大黎加
    CU = "\u53e4\u5df4",               # 古巴
    CY = "\u8cfd\u666e\u52d2\u65af",   # 賽普勒斯
    CZ = "\u6377\u514b",               # 捷克
    DE = "\u5fb7\u570b",               # 德國
    DK = "\u4e39\u9ea5",               # 丹麥
    DM = "\u591a\u7c73\u5c3c\u514b",   # 多米尼克
    DO = "\u591a\u660e\u5c3c\u52a0\u5171\u548c\u570b", # 多明尼加共和國
    DZ = "\u963f\u723e\u53ca\u5229\u4e9e", # 阿爾及利亞
    EC = "\u5384\u74dc\u591a",         # 厄瓜多
    EE = "\u611b\u6c99\u5c3c\u4e9e",   # 愛沙尼亞
    EG = "\u57c3\u53ca",               # 埃及
    ES = "\u897f\u73ed\u7259",         # 西班牙
    ET = "\u8863\u7d22\u6bd4\u4e9e",   # 衣索比亞
    FI = "\u82ac\u862d",               # 芬蘭
    FJ = "\u6590\u6fdf",               # 斐濟
    FR = "\u6cd5\u570b",               # 法國
    GA = "\u52a0\u5f6d",               # 加彭
    GB = "\u82f1\u570b",               # 英國
    GD = "\u683c\u745e\u90a3\u9054",   # 格瑞那達
    GE = "\u55ac\u6cbb\u4e9e",         # 喬治亞
    GH = "\u8fe6\u7d0d",               # 迦納
    GR = "\u5e0c\u81d8",               # 希臘
    GT = "\u74dc\u5730\u99ac\u62c9",   # 瓜地馬拉
    GU = "\u95dc\u5cf6",               # 關島
    GY = "\u84cb\u4e9e\u90a3",         # 蓋亞那
    HK = "\u9999\u6e2f",               # 香港
    HN = "\u5b8f\u90fd\u62c9\u65af",   # 宏都拉斯
    HR = "\u514b\u7f85\u57c3\u897f\u4e9e", # 克羅埃西亞
    HT = "\u6d77\u5730",               # 海地
    HU = "\u5308\u7259\u5229",         # 匈牙利
    ID = "\u5370\u5c3c",               # 印尼
    IE = "\u611b\u723e\u862d",         # 愛爾蘭
    IL = "\u4ee5\u8272\u5217",         # 以色列
    IN = "\u5370\u5ea6",               # 印度
    IQ = "\u4f0a\u62c9\u514b",         # 伊拉克
    IR = "\u4f0a\u6717",               # 伊朗
    IS = "\u51b0\u5cf6",               # 冰島
    IT = "\u7fa9\u5927\u5229",         # 義大利
    JM = "\u7259\u8cb7\u52a0",         # 牙買加
    JO = "\u7d04\u65e6",               # 約旦
    JP = "\u65e5\u672c",               # 日本
    KE = "\u80af\u4e9e",               # 肯亞
    KG = "\u5409\u723e\u5409\u65af",   # 吉爾吉斯
    KH = "\u67ec\u57d4\u5be8",         # 柬埔寨
    KR = "\u97d3\u570b",               # 韓國
    KW = "\u79d1\u5a01\u7279",         # 科威特
    KZ = "\u54c8\u85a9\u514b",         # 哈薩克
    LA = "\u5bee\u570b",               # 寮國
    LB = "\u9ece\u5df4\u5ae9",         # 黎巴嫩
    LI = "\u5217\u652f\u6566\u58eb\u767b", # 列支敦士登
    LK = "\u65af\u91cc\u862d\u5361",   # 斯里蘭卡
    LT = "\u7acb\u9676\u5b9b",         # 立陶宛
    LU = "\u76e7\u68ee\u5821",         # 盧森堡
    LV = "\u62c9\u812b\u7dad\u4e9e",   # 拉脫維亞
    LY = "\u5229\u6bd4\u4e9e",         # 利比亞
    MA = "\u6469\u6d1b\u54e5",         # 摩洛哥
    MC = "\u6469\u7d0d\u54e5",         # 摩納哥
    MD = "\u6469\u723e\u591a\u74e6",   # 摩爾多瓦
    ME = "\u8499\u7279\u5167\u54e5\u7f85", # 蒙特內哥羅
    MG = "\u99ac\u9054\u52a0\u65af\u52a0", # 馬達加斯加
    MK = "\u5317\u99ac\u5176\u9813",   # 北馬其頓
    ML = "\u99ac\u5229",               # 馬利
    MM = "\u7dec\u7538",               # 緬甸
    MN = "\u8499\u53e4",               # 蒙古
    MO = "\u6fb3\u9580",               # 澳門
    MT = "\u99ac\u723e\u4ed6",         # 馬爾他
    MU = "\u6a21\u91cc\u897f\u65af",   # 模里西斯
    MV = "\u99ac\u723e\u5730\u592b",   # 馬爾地夫
    MW = "\u99ac\u62c9\u5a01",         # 馬拉威
    MX = "\u58a8\u897f\u54e5",         # 墨西哥
    MY = "\u99ac\u4f86\u897f\u4e9e",   # 馬來西亞
    MZ = "\u83ab\u4e09\u6bd4\u514b",   # 莫三比克
    "NA" = "\u7d0d\u7c73\u6bd4\u4e9e",   # 納米比亞
    NG = "\u5948\u53ca\u5229\u4e9e",   # 奈及利亞
    NI = "\u5c3c\u52a0\u62c9\u74dc",   # 尼加拉瓜
    NL = "\u8377\u862d",               # 荷蘭
    NO = "\u632a\u5a01",               # 挪威
    NP = "\u5c3c\u6cca\u723e",         # 尼泊爾
    NZ = "\u7d10\u897f\u862d",         # 紐西蘭
    OM = "\u963f\u66fc",               # 阿曼
    PA = "\u5df4\u62ff\u99ac",         # 巴拿馬
    PE = "\u79d8\u9b6f",               # 秘魯
    PG = "\u5df4\u5e03\u4e9e\u7d10\u5e7e\u5167\u4e9e", # 巴布亞紐幾內亞
    PH = "\u83f2\u5f8b\u8cd3",         # 菲律賓
    PK = "\u5df4\u57fa\u65af\u5766",   # 巴基斯坦
    PL = "\u6ce2\u862d",               # 波蘭
    PR = "\u6ce2\u591a\u9ece\u5404",   # 波多黎各
    PS = "\u5df4\u52d2\u65af\u5766",   # 巴勒斯坦
    PT = "\u8461\u8404\u7259",         # 葡萄牙
    PY = "\u5df4\u62c9\u572d",         # 巴拉圭
    QA = "\u5361\u9054",               # 卡達
    RO = "\u7f85\u99ac\u5c3c\u4e9e",   # 羅馬尼亞
    RS = "\u585e\u723e\u7dad\u4e9e",   # 塞爾維亞
    RU = "\u4fc4\u7f85\u65af",         # 俄羅斯
    RW = "\u76e7\u5b89\u9054",         # 盧安達
    SA = "\u6c99\u70cf\u5730\u963f\u62c9\u4f2f", # 沙烏地阿拉伯
    SD = "\u8607\u4e39",               # 蘇丹
    SE = "\u745e\u5178",               # 瑞典
    SG = "\u65b0\u52a0\u5761",         # 新加坡
    SI = "\u65af\u6d1b\u7dad\u5c3c\u4e9e", # 斯洛維尼亞
    SK = "\u65af\u6d1b\u4f10\u514b",   # 斯洛伐克
    SN = "\u585e\u5167\u52a0\u723e",   # 塞內加爾
    SO = "\u7d22\u99ac\u5229\u4e9e",   # 索馬利亞
    SV = "\u85a9\u723e\u74e6\u591a",   # 薩爾瓦多
    SY = "\u6558\u5229\u4e9e",         # 敘利亞
    TH = "\u6cf0\u570b",               # 泰國
    TJ = "\u5854\u5409\u514b",         # 塔吉克
    TN = "\u7a81\u5c3c\u897f\u4e9e",   # 突尼西亞
    TR = "\u571f\u8033\u5176",         # 土耳其
    TT = "\u5343\u91cc\u9054\u53ca\u6258\u5df4\u54e5", # 千里達及托巴哥
    TW = "\u81fa\u7063",               # 臺灣
    TZ = "\u5766\u5c1a\u5c3c\u4e9e",   # 坦尚尼亞
    UA = "\u70cf\u514b\u862d",         # 烏克蘭
    UG = "\u70cf\u5e72\u9054",         # 烏干達
    US = "\u7f8e\u570b",               # 美國
    UY = "\u70cf\u62c9\u572d",         # 烏拉圭
    UZ = "\u70cf\u8332\u5225\u514b",   # 烏茲別克
    VE = "\u59d4\u5167\u745e\u62c9",   # 委內瑞拉
    VN = "\u8d8a\u5357",               # 越南
    YE = "\u8449\u9580",               # 葉門
    ZA = "\u5357\u975e",               # 南非
    ZM = "\u5c1a\u6bd4\u4e9e",         # 尚比亞
    ZW = "\u8f9b\u5df4\u5a01"          # 辛巴威
  )

  zh_names <- iso2_zh[iso2_codes]
  # Fallback: unknown codes returned as-is
  zh_names[is.na(zh_names)] <- iso2_codes[is.na(zh_names)]

  if (with_code) {
    # UI_R027 pattern: 中文名（ISO）
    paste0(zh_names, "\uff08", iso2_codes, "\uff09")
  } else {
    zh_names
  }
}
