import requests
from bs4 import BeautifulSoup
import csv
import os
import urllib3
import time

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def deep_scout_high_frequency():
    target_feeds = [
        "https://finance.yahoo.com/news/rssindex",
        "https://finance.yahoo.com/rss/stocks"
    ]
    filename = "finance_permanent_db.csv"
    
    # 1. 기존 링크 로드 (중복 방지)
    existing_links = set()
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                existing_links.add(row["Link"])

    new_count = 0
    print(f"📡 [고빈도 정찰] 스캔 시작... (현재 DB: {len(existing_links)}건)")

    headers = {"User-Agent": "Mozilla/5.0"}
    
    try:
        current_batch = []
        for url in target_feeds:
            res = requests.get(url, headers=headers, verify=False, timeout=10)
            soup = BeautifulSoup(res.content, features="xml")
            items = soup.find_all('item')
            
            for item in items:
                link = item.link.text.split('?')[0]
                if link not in existing_links:
                    current_batch.append({
                        "Date": item.pubDate.text,
                        "Title": item.title.text.strip(),
                        "Link": link
                    })
                    existing_links.add(link)
                    new_count += 1
        
        # 2. 새로운 기사가 있을 때만 기록
        if current_batch:
            # 최신 기사가 아래로 가게 날짜순 정렬 (선택 사항)
            file_exists = os.path.isfile(filename)
            with open(filename, "a", newline="", encoding="utf-8-sig") as f:
                writer = csv.DictWriter(f, fieldnames=["Date", "Title", "Link"])
                if not file_exists:
                    writer.writeheader()
                writer.writerows(reversed(current_batch)) # RSS는 최신순이므로 거꾸로 넣어야 시간순
            print(f"✅ 신규 첩보 {new_count}건 확보 성공!")
        else:
            print("💤 새로운 소식이 없습니다.")

    except Exception as e:
        print(f"🚨 정찰 중 오류: {e}")

if __name__ == "__main__":
    # 사령관님, 이 코드를 20분마다 실행하는 스케줄러에 등록하십시오.
    deep_scout_high_frequency()