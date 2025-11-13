import pandas as pd
import sqlite3

# ==========================================
# 1) 경로 설정
# ==========================================
excel_path = "Users/a/Desktop/cursor/DataAnalysis/2025_유저_동향_자료.xlsx"
db_path = "Users/a/Desktop/cursor/DataAnalysis/user_feedback_normalized.db"

# 원본 데이터 불러오기
df = pd.read_excel(excel_path)

# SQLite 연결
conn = sqlite3.connect(db_path)
cur = conn.cursor()

# ==========================================
# 2) 차원 테이블 만드는 유틸 함수
# ==========================================
def create_dim_table(conn, table_name, id_col, name_col, values):
    dim_df = pd.DataFrame({name_col: sorted(values)})
    dim_df[id_col] = dim_df.index + 1  # 1부터 시작하는 ID
    dim_df = dim_df[[id_col, name_col]]

    dim_df.to_sql(table_name, conn, if_exists="replace", index=False)
    return dim_df

# ==========================================
# 3) 각 차원 테이블 생성
# ==========================================

# 기간
period_dim = create_dim_table(
    conn, "d_period", "period_id", "period_name",
    df[[c for c in df.columns if "기간" in c or "동향" in c][0]].unique()
)

# 지역
region_dim = create_dim_table(
    conn, "d_region", "region_id", "region_name",
    df[[c for c in df.columns if "지역" in c][0]].unique()
)

# 출처
source_dim = create_dim_table(
    conn, "d_source", "source_id", "source_name",
    df[[c for c in df.columns if "출처" in c][0]].unique()
)

# 유형
type_col = [c for c in df.columns if "유형" in c or "카테고리" in c or "분류" in c][0]
type_dim = create_dim_table(
    conn, "d_type", "type_id", "type_name",
    df[type_col].dropna().unique()
)

# 긍정/부정
sentiment_dim = pd.DataFrame({
    "sentiment_id": [0, 1],
    "sentiment_name": ["긍정", "부정"]
})
sentiment_dim.to_sql("d_sentiment", conn, if_exists="replace", index=False)

# ==========================================
# 4) 제목/링크 raw_post 테이블 생성 (post_id = 3000부터)
# ==========================================

title_col = [c for c in df.columns if "제목" in c or "title" in c][0]
link_col  = [c for c in df.columns if "링크" in c or "url" in c][0]

raw_post = pd.DataFrame({
    "post_id": df.index + 3000,   
    "title": df[title_col].fillna(""),   
    "url": df[link_col].fillna("")
})

raw_post.to_sql("raw_post", conn, if_exists="replace", index=False)

# ==========================================
# 5) 차원 테이블을 이용한 팩트 테이블 생성
# ==========================================

period_col = [c for c in df.columns if "기간" in c or "동향" in c][0]
region_col = [c for c in df.columns if "지역" in c][0]
source_col = [c for c in df.columns if "출처" in c][0]
sent_col = [c for c in df.columns if "부정" in c][0]

df["period_id"] = df[period_col].map(dict(zip(period_dim["period_name"], period_dim["period_id"])))
df["region_id"] = df[region_col].map(dict(zip(region_dim["region_name"], region_dim["region_id"])))
df["source_id"] = df[source_col].map(dict(zip(source_dim["source_name"], source_dim["source_id"])))
df["type_id"] = df[type_col].map(dict(zip(type_dim["type_name"], type_dim["type_id"])))

df["sentiment_id"] = df[sent_col].map({
    0: 0, "긍정": 0,
    1: 1, "부정": 1,
    True: 1, False: 0
})

fact_df = df[[
    "period_id", "region_id", "source_id",
    "sentiment_id", "type_id"
]].copy()

fact_df["feedback_id"] = fact_df.index + 1
fact_df["post_id"] = fact_df["feedback_id"]

fact_df = fact_df[[
    "feedback_id", "post_id", "period_id", "region_id",
    "source_id", "sentiment_id", "type_id"
]]


fact_df.to_sql("fact_user_feedback", conn, if_exists="replace", index=False)


conn.commit()
conn.close()

print("정규화 + 제목/링크 테이블까지 생성 완료!")
print("DB 경로:", db_path)
print("d_period / d_region / d_source / d_sentiment / d_type / raw_post / fact_user_feedback 생성됨!")
