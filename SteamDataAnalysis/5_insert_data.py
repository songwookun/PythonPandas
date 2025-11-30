import sqlite3
import pandas as pd

DB_PATH = "steam_analysis.db"
KAGGLE_PATH = "kaggle_clean.xlsx"
STEAMSPY_PATH = "steamspy_raw.xlsx"

def insert_all(db_path=DB_PATH):
    print("Kaggle / SteamSpy 데이터 로딩 중...")

    df_kaggle = pd.read_excel(KAGGLE_PATH, engine="openpyxl")

    df_spy = pd.read_excel(STEAMSPY_PATH, engine="openpyxl")

    # -----------------------------
    # 1) Kaggle ∩ SteamSpy 교집합 생성
    # -----------------------------
    common = pd.merge(
        df_kaggle,
        df_spy,
        on="appid",
        how="inner",
        suffixes=("_kg", "_spy")     
    )

    print(f"Kaggle ∩ SteamSpy 교집합 게임 수: {len(common)}")

    conn = sqlite3.connect(db_path)

    # -----------------------------
    # 2) dim_game
    # -----------------------------
    dim_game = common[["appid", "name_kg", "release_date", "price_kg"]].copy()
    dim_game.rename(columns={
        "name_kg": "name",
        "price_kg": "price"
    }, inplace=True)

    dim_game.to_sql("dim_game", conn, if_exists="append", index=False)
    print(f"dim_game 삽입 완료 (행: {len(dim_game)})")

    # -----------------------------
    # 3) dim_genre + bridge_game_genre
    # -----------------------------
    genre_set = set()

    for g in common["genres"].dropna():
        for item in str(g).split(";"):
            genre_set.add(item.strip())

    dim_genre = pd.DataFrame({"genre_name": sorted(list(genre_set))})

    dim_genre.to_sql("dim_genre", conn, if_exists="append", index=False)
    print(f"dim_genre 생성 완료 (장르 수: {len(dim_genre)})")

    dim_genre_db = pd.read_sql("""
        SELECT rowid AS genre_id, genre_name FROM dim_genre
    """, conn)

    mappings = []
    for _, row in common[["appid", "genres"]].dropna().iterrows():
        appid = row["appid"]
        for g in str(row["genres"]).split(";"):
            gname = g.strip()
            gid = dim_genre_db.loc[
                dim_genre_db.genre_name == gname, "genre_id"
            ].iloc[0]
            mappings.append({"appid": appid, "genre_id": gid})

    bridge_df = pd.DataFrame(mappings)
    bridge_df.to_sql("bridge_game_genre", conn, if_exists="append", index=False)
    print(f"bridge_game_genre 삽입 완료 (매핑 수: {len(bridge_df)})")

    # -----------------------------
    # 4) fact_playtime
    # -----------------------------
    fact_playtime = common[[
        "appid",
        "average_forever",
        "median_forever",
        "owners"
    ]].copy()

    fact_playtime.rename(columns={
        "average_forever": "avg_playtime",
        "median_forever": "median_playtime",
        "owners": "owners_text"
    }, inplace=True)

    fact_playtime.to_sql("fact_playtime", conn, if_exists="append", index=False)
    print(f"✅ fact_playtime 삽입 완료 (행: {len(fact_playtime)})")

    # -----------------------------
    # 5) fact_review
    # -----------------------------
    fact_review = common[["appid", "positive", "negative"]].copy()
    fact_review.rename(columns={
        "positive": "positive_cnt",
        "negative": "negative_cnt"
    }, inplace=True)

    total = fact_review["positive_cnt"] + fact_review["negative_cnt"]

    # 제로 디비전 방지
    fact_review["positive_rate"] = fact_review["positive_cnt"] / total.replace(0, 1)

    fact_review.to_sql("fact_review", conn, if_exists="append", index=False)
    print(f"fact_review 삽입 완료 (행: {len(fact_review)})")

    conn.close()
    print(f"모든 데이터 삽입 완료 → {db_path}")

if __name__ == "__main__":
    insert_all()