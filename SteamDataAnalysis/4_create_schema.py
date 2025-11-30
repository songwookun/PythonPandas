import sqlite3

DB_PATH = "steam_analysis.db"

def create_schema(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)

    conn.execute("PRAGMA foreign_keys = ON")
    cur = conn.cursor()

    print("기존 테이블 삭제 중...")
    cur.executescript("""
        DROP TABLE IF EXISTS bridge_game_genre;
        DROP TABLE IF EXISTS fact_review;
        DROP TABLE IF EXISTS fact_playtime;
        DROP TABLE IF EXISTS dim_genre;
        DROP TABLE IF EXISTS dim_game;
    """)

    print("새 스키마 생성 중...")
    cur.executescript("""
        --------------------------------------------------------
        -- 1) 게임 기본 정보 테이블
        --------------------------------------------------------
        CREATE TABLE dim_game (
            appid        INTEGER PRIMARY KEY,
            name         TEXT,
            release_date TEXT,
            price        REAL DEFAULT 0
        );

        --------------------------------------------------------
        -- 2) 장르 마스터 테이블
        --------------------------------------------------------
        CREATE TABLE dim_genre (
            genre_id   INTEGER PRIMARY KEY AUTOINCREMENT,
            genre_name TEXT UNIQUE
        );

        --------------------------------------------------------
        -- 3) 게임-장르 다대다 매핑 테이블
        --------------------------------------------------------
        CREATE TABLE bridge_game_genre (
            appid    INTEGER,
            genre_id INTEGER,
            FOREIGN KEY(appid) REFERENCES dim_game(appid),
            FOREIGN KEY(genre_id) REFERENCES dim_genre(genre_id)
        );

        --------------------------------------------------------
        -- 4) 플레이타임 / 유저수 (SteamSpy)
        --------------------------------------------------------
        CREATE TABLE fact_playtime (
            appid            INTEGER PRIMARY KEY,
            avg_playtime     INTEGER,
            median_playtime  INTEGER,
            owners_text      TEXT,
            FOREIGN KEY(appid) REFERENCES dim_game(appid)
        );

        --------------------------------------------------------
        -- 5) 리뷰 데이터 (SteamSpy positive / negative)
        --------------------------------------------------------
        CREATE TABLE fact_review (
            appid          INTEGER PRIMARY KEY,
            positive_cnt   INTEGER,
            negative_cnt   INTEGER,
            positive_rate  REAL,
            FOREIGN KEY(appid) REFERENCES dim_game(appid)
        );
    """)

    conn.commit()
    conn.close()
    print(f"스키마 생성 완료 → {db_path}")


if __name__ == "__main__":
    create_schema()