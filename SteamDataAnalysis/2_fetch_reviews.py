import requests
import pandas as pd

def fetch_reviews(appid, num=200):
    url = f"https://store.steampowered.com/appreviews/{appid}?json=1&num_per_page={num}"
    j = requests.get(url).json()
    return j.get("reviews", [])

def fetch_all(appids, num=200):
    rows = []
    for appid in appids:
        print(f"[INFO] 리뷰 수집 중: {appid}")
        reviews = fetch_reviews(appid, num)
        for r in reviews:
            rows.append({
                "appid": appid,
                "review_text": r.get("review"),
                "voted_up": r.get("voted_up"),
                "playtime": r.get("author", {}).get("playtime_forever", 0),
                "timestamp_created": r.get("timestamp_created")
            })
    return pd.DataFrame(rows)

if __name__ == "__main__":
    appids = [570, 578080, 271590]
    df = fetch_all(appids)
    df.to_excel("reviews_raw.xlsx", index=False, engine="openpyxl")
    print("리뷰 데이터 수집 완료 → reviews_raw.xlsx 저장")