import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from datetime import datetime
import time


class MacroScouter:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        # 필터 키워드
        self.jp_keywords = [
            # --- 주요 기관 및 정책 ---
            '日銀',          # 일본은행 (BOJ)
            '財務省',        # 재무성 (기재부 역할)
            '金融政策',      # 금융정책
            '金利',          # 금리
            '為替',          # 환율
            '物価',          # 물가
            '円',            # 엔화
            '介入',          # 개입 (외환시장 개입)
            '決定会合',      # 정책결정회의
            '緩和',          # 완화 (돈 풀기)
            '出口',          # 출구 (긴축 전환)

            # --- 핵심 직책 (누가 앉아도 잡히는 그물) ---
            '総裁',          # 총재 (BOJ 수장)
            '副総裁',        # 부총재
            '審議委員',      # 심의위원 (BOJ 금통위원)
            '財務相',        # 재무상 (재무부 장관)
            '財務官',        # 재무관 (실무 책임자)
            '首相',          # 수상 (총리)
            '閣僚',          # 각료 (장관급 인사)

            # --- 핵심 경제 지표 (신규 추가) ---
            'GDP',          # 국내총생산
            'CPI',          # 소비자물가지수
            '短観',          # 단칸지표 (기업경기실사지표)
            '貿易収支',      # 무역수지 (수출입 상황)
            '失業率',        # 실업률
            '賃金',          # 임금 (BOJ가 최근 가장 강조함)
            '実質賃金',      # 실질임금 (이게 올라야 금리 올림)
            '家計調査',      # 가계조사 (소비자 지출)
            '機械受注',      # 기계수주 (기업 투자 지표)
            '景気動向指数',   # 경기동향지수

            # --- 핵심 액션 및 상태 ---
            '利上げ',        # 금리 인상 (가장 중요한 단어!)
            '利下げ',        # 금리 인하
            '据え置き',      # 금리 동결
            '修正',          # 정책 수정
            '点検',          # 정책 점검
            '当面',          # 당분간 (정책 유지 시 자주 씀)
            '踏み込む'       # (정책에) 발을 들이다, 단행하다
        ]
        self.db_path = "macro_intelligence.db"
        self._init_db()

    def _init_db(self):
        """DB 초기화 (country 컬럼 포함)"""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS news 
                       (link TEXT PRIMARY KEY, title TEXT, source TEXT, 
                        country TEXT, date TEXT, collected_at TEXT)''')
        conn.commit()
        conn.close()

    def _is_new(self, link):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("SELECT link FROM news WHERE link=?", (link,))
        exists = cur.fetchone()
        conn.close()
        return exists is None

    def _save_to_db(self, item, country="Japan"):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        try:
            cur.execute("INSERT INTO news (link, title, source, country, date, collected_at) VALUES (?, ?, ?, ?, ?, ?)",
                        (item['link'], item['title'], item['source'], country, item['date'], datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            conn.commit()
        except sqlite3.IntegrityError:
            pass
        finally:
            conn.close()

    def _get_soup(self, url):
        try:
            res = requests.get(url, headers=self.headers, timeout=15)
            res.encoding = 'utf-8'
            if res.status_code == 200:
                return BeautifulSoup(res.text, 'html.parser')
        except Exception as e:
            print(f"Connection Error ({url}): {e}")
        return None

    def scout_boj_universal(self, year=None):
        """s링크 패턴을 분석하여 BOJ 자료를 낚아챕니다."""
        if year:
            url = f"https://www.boj.or.jp/about/press/koen_{year}/index.htm"
            source_name = f"BOJ_{year}"
        else:
            url = "https://www.boj.or.jp/about/press/index.htm"
            source_name = "BOJ_Latest"

        soup = self._get_soup(url)
        results = []
        if not soup:
            return results

        # '모든 링크 뒤지기' 전략
        links = soup.find_all('a', href=True)
        for a in links:
            href = a['href']
            title = a.get_text().strip()

            if ('/ko' in href or 'koen' in href) and len(title) > 10:
                full_link = "https://www.boj.or.jp" + \
                    href if href.startswith('/') else href

                # 1. 날짜 추출 시도
                found_date = None
                try:
                    # 실제 표(tr) 안에 날짜(td)가 있는지 확인
                    found_date = a.find_parent('tr').find(
                        'td').get_text().strip()
                except:
                    # 날짜가 없으면 기사가 아닐 확률이 높으므로 'found_date'는 None
                    pass

                # 🚨 [핵심 수정]
                # 날짜를 못 찾았거나, 찾았는데 'latest' 같은 안내 문구라면 저장하지 않고 건너뜀
                if not found_date or "latest" in found_date.lower():
                    continue

                # 정상적인 날짜가 있을 때만 진행
                date = self._standardize_date(found_date, year_hint=year)

                results.append({
                    "source": source_name,
                    "date": date,
                    "title": title,
                    "link": full_link
                })
        return results

    def scout_news_jp(self):
        """시사통신 및 로이터 정찰"""
        sources = [
            {"name": "Jiji", "url": "https://www.jiji.com/jc/c?g=eco"},
            {"name": "Reuters", "url": "https://jp.reuters.com/markets/japan/"}
        ]
        results = []
        for src in sources:
            soup = self._get_soup(src['url'])
            if not soup:
                continue

            links = soup.find_all('a', href=True)
            for link in links:
                title = link.get_text().strip()
                # 키워드 필터링 적용
                if len(title) > 15 and any(kw in title for kw in self.jp_keywords):
                    href = link['href']
                    full_link = href if href.startswith('http') else (
                        "https://www.jiji.com" +
                        href if src['name'] == "Jiji" else "https://jp.reuters.com" + href
                    )
                    results.append({
                        "source": src['name'],
                        # 오늘 날짜(YYYY-MM-DD)로 변환
                        "date": self._standardize_date("Today"),
                        "title": title,
                        "link": full_link
                    })
        return results

    def _standardize_date(self, date_str, year_hint=None):
        """중구난방인 날짜 형식을 YYYY-MM-DD로 통일합니다."""
        now = datetime.now()

        # 1. 'Today', 'Recent' 처리 -> 오늘 날짜로
        if any(word in date_str.lower() for word in ['today', 'recent', '最新']):
            return now.strftime('%Y-%m-%d')

        # 2. '2025年 7月 3日' 처리
        if '年' in date_str:
            date_str = date_str.replace(
                '年', '-').replace('月', '-').replace('日', '').replace(' ', '')
            # 2025-7-3 -> 2025-07-03 형태로 보정
            try:
                dt = datetime.strptime(date_str, '%Y-%m-%d')
                return dt.strftime('%Y-%m-%d')
            except:
                pass

        # 3. '2021-XX' 처리 (과거 데이터 백필용)
        if '-XX' in date_str:
            return f"{date_str.split('-')[0]}-01-01"  # 일단 그 해 1월 1일로 처리

        # 4. 기타 '07/03' 등 연도가 없는 경우
        # (BOJ 최신 리스트 등에서 연도 없이 월/일만 나올 때 사용)
        if len(date_str) <= 6 and '/' in date_str:
            year = year_hint if year_hint else now.year
            return f"{year}-{date_str.replace('/', '-')}"

        return date_str  # 변환 실패 시 원본 유지

    def run_all_jp(self):
        """일본 정찰 통합 실행"""
        print("🛰️ 일본 매크로 통합 정찰 개시...")
        new_items = []

        # 1. BOJ 최신
        for item in self.scout_boj_universal():
            if self._is_new(item['link']):
                self._save_to_db(item, country="Japan")
                new_items.append(item)

        # 2. 뉴스 (지자, 로이터)
        for item in self.scout_news_jp():
            if self._is_new(item['link']):
                self._save_to_db(item, country="Japan")
                new_items.append(item)
        return new_items


if __name__ == "__main__":
    scouter = MacroScouter()

    # [1] 과거 데이터 채우기 (2021~2025)
    # print("⏳ 과거 아카이브 채우는 중...")
    # for y in range(2021, 2026):
    #     historical = scouter.scout_boj_universal(year=y)
    #     count = 0
    #     for item in historical:
    #         if scouter._is_new(item['link']):
    #             scouter._save_to_db(item, country="Japan")
    #             count += 1
    #     print(f"✅ {y}년도 완료: {count}건 저장")

    # [2] 오늘자 최신 정찰
    new_data = scouter.run_all_jp()
    if new_data:
        print(f"🔥 신규 데이터 {len(new_data)}건 발견!")
    else:
        print("ℹ️ 새로 업데이트된 소식 없음.")
