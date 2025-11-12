import pandas as pd
import sqlite3

# ==========================================================
# 1. SQLite DB 연결 및 데이터 로드
# ==========================================================
db_path = "Users/a/Desktop/cursor/DataAnalysis/user_feedback.db"
conn = sqlite3.connect(db_path)
df = pd.read_sql("SELECT * FROM user_feedback", conn)
conn.close()

# ==========================================================
# 2. 주요 컬럼 자동 탐지
# ==========================================================
period_col = [c for c in df.columns if "동향" in c or "기간" in c][0]
region_col = [c for c in df.columns if "지역" in c][0]
neg_col = [c for c in df.columns if "부정" in c][0]
source_col = [c for c in df.columns if "출처" in c][0]

# ==========================================================
# 3. 부정여부 값 정리
# ==========================================================
df[neg_col] = df[neg_col].map({1: "부정", 0: "긍정", True: "부정", False: "긍정"})

total_count = len(df)
print(f"\n전체 데이터 수: {total_count}건\n")

# ==========================================================
# 4. 기간별 게시글 수 요약
# ==========================================================
period_summary = df[period_col].value_counts().reset_index()
period_summary.columns = ["동향 확인 기간", "게시글 수"]
period_summary = period_summary.sort_values("동향 확인 기간").reset_index(drop=True)
print("기간별 게시글 수")
print(period_summary, "\n")

# ==========================================================
# 5. 기간별 지역 비율
# ==========================================================
region_summary = (
    df.groupby([period_col, region_col])
    .size()
    .reset_index(name="게시글 수")
)
region_summary["비율(%)"] = (
    region_summary.groupby(period_col)["게시글 수"]
    .transform(lambda x: round((x / x.sum()) * 100, 2))
)
print("기간별 지역 비율 (국내 vs 해외)")
print(region_summary, "\n")

# ==========================================================
# 6. 기간별 부정/긍정 비율
# ==========================================================
neg_summary = (
    df.groupby([period_col, neg_col])
    .size()
    .reset_index(name="게시글 수")
)
neg_summary["비율(%)"] = (
    neg_summary.groupby(period_col)["게시글 수"]
    .transform(lambda x: round((x / x.sum()) * 100, 2))
)
print("기간별 부정/긍정 비율")
print(neg_summary, "\n")

# ==========================================================
# 7. 기간별 출처별 게시글 비율
# ==========================================================
source_summary = (
    df.groupby([period_col, source_col])
    .size()
    .reset_index(name="게시글 수")
)
source_summary["비율(%)"] = (
    source_summary.groupby(period_col)["게시글 수"]
    .transform(lambda x: round((x / x.sum()) * 100, 2))
)
print("기간별 출처 비율")
print(source_summary, "\n")

# ==========================================================
# 8. 교차 분석 (기간 × 지역 × 부정여부 × 출처)
# ==========================================================
cross_summary = (
    df.groupby([period_col, region_col, neg_col, source_col])
    .size()
    .reset_index(name="게시글 수")
)
cross_summary["비율(%)"] = (
    cross_summary.groupby(period_col)["게시글 수"]
    .transform(lambda x: round((x / x.sum()) * 100, 2))
)
print("복합 교차 분석 (기간별 지역×부정여부×출처)")
print(cross_summary, "\n")

# ==========================================================
# 9. 피벗 요약표 (시각화용)
# ==========================================================
pivot = pd.pivot_table(
    df,
    index=period_col,
    columns=[region_col, neg_col],
    values=source_col,
    aggfunc="count",
    fill_value=0,
)
print("피벗 요약표 (기간×지역×부정여부)")
print(pivot, "\n")

# ==========================================================
# 10. 결과 저장 (엑셀 내보내기)
# ==========================================================
output_path = "C:/python/유저동향_요약결과_SQLite2.xlsx"
with pd.ExcelWriter(output_path) as writer:
    period_summary.to_excel(writer, sheet_name="1_기간별_게시글수", index=False)
    region_summary.to_excel(writer, sheet_name="2_지역별_비율", index=False)
    neg_summary.to_excel(writer, sheet_name="3_부정긍정비율", index=False)
    source_summary.to_excel(writer, sheet_name="4_출처비율", index=False)
    cross_summary.to_excel(writer, sheet_name="5_교차분석", index=False)
    pivot.to_excel(writer, sheet_name="6_피벗요약")
print(f"엑셀 저장 완료: {output_path}")

# ==========================================================
# 11. 공식 포럼 (국내/해외 × 긍·부정 비율)
# ==========================================================
official_forum = df[df[source_col] == "공식포럼"]
forum_summary = (
    official_forum.groupby([period_col, region_col, neg_col])
    .size()
    .reset_index(name="게시글 수")
)
forum_summary["비율(%)"] = (
    forum_summary.groupby([period_col, region_col])["게시글 수"]
    .transform(lambda x: round((x / x.sum()) * 100, 2))
)
print("🏛 공식 포럼 내 국내/해외별 긍·부정 비율")
print(forum_summary, "\n")

with pd.ExcelWriter(output_path, mode="a", engine="openpyxl") as writer:
    forum_summary.to_excel(writer, sheet_name="7_공식포럼_긍부정비율", index=False)
print("공식 포럼 분석 시트 추가 완료!")

# ==========================================================
# 12. 특정 기간(9월18일~10월2일) 부정 유형 비율
# ==========================================================
type_col = [c for c in df.columns if "유형" in c or "카테고리" in c or "분류" in c]
if not type_col:
    raise ValueError("'유형' 관련 컬럼을 찾을 수 없습니다. 컬럼명을 확인해주세요.")
type_col = type_col[0]

filtered_df = df[
    (df[period_col] == "9월18일~10월2일") &
    (df[neg_col].isin(["부정", True, 1]))
]

type_summary = (
    filtered_df.groupby(type_col)
    .size()
    .reset_index(name="게시글 수")
    .sort_values("게시글 수", ascending=False)
    .reset_index(drop=True)
)

total = type_summary["게시글 수"].sum()
type_summary["비율(%)"] = round((type_summary["게시글 수"] / total) * 100, 2)

type_summary = type_summary.rename(columns={type_col: "유형"})
type_summary = type_summary[["유형", "비율(%)"]]
print("9월18일~10월2일 부정 유형별 비율")
print(type_summary, "\n")

with pd.ExcelWriter(output_path, mode="a", engine="openpyxl") as writer:
    type_summary.to_excel(writer, sheet_name="8_부정유형비율", index=False)
print("부정 유형 비율 분석 시트 추가 완료")