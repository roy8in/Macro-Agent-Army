import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import urllib3
from dateutil import parser # pip install python-dateutil 필수

# SSL 경고 무시
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class MacroScouter:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        
        # 🇯🇵 [일본 정밀 타격 키워드 - 사령관님의 원본 주석 100% 복구]
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

            # --- 핵심 직책 ---
            '総裁',          # 총재 (BOJ 수장)
            '副総裁',        # 부총재
            '審議委員',      # 심의위원 (BOJ 금통위원)
            '財務相',        # 재무상 (재무부 장관)
            '財務官',        # 재무관 (실무 책임자)
            '首相',          # 수상 (총리)
            '閣僚',          # 각료 (장관급 인사)

            # --- 핵심 경제 지표 ---
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
            '利上げ',        # 금리 인상
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
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS news 
                       (link TEXT PRIMARY KEY, title TEXT, source TEXT, 
                        country TEXT, date TEXT, collected_at TEXT)''')
        conn.commit()
        conn.close()

    def _standardize_date(self, date_str):
        """지저분한 날짜를 'YYYY-MM-DD HH:MM:SS'로 세탁"""
        if not date_str:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        try:
            dt = parser.parse(date_str)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return date_str.replace('T', ' ').replace('Z', '')[:19]

    def _is_new(self, link):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("SELECT link FROM news WHERE link=?", (link,))
        exists = cur.fetchone()
        conn.close()
        return exists is None

    def _save_to_db(self, item, country):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        try:
            cur.execute("INSERT OR IGNORE INTO news (link, title, source, country, date, collected_at) VALUES (?, ?, ?, ?, ?, ?)",
                        (item['link'], item['title'], item['source'], country, item['date'], datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            conn.commit()
        except sqlite3.IntegrityError:
            pass
        finally:
            conn.close()

    def _get_soup(self, url, is_xml=False):
        try:
            res = requests.get(url, headers=self.headers, verify=False, timeout=15)
            if res.status_code == 200:
                features = "xml" if is_xml else "html.parser"
                return BeautifulSoup(res.content, features)
        except Exception as e:
            print(f"📡 접속 에러 ({url}): {e}")
        return None

    def scout_yahoo(self):
        """야후 파이낸스: 필터 없이 전체 수집"""
        sources = [
            {"name": "Yahoo_Index", "url": "https://finance.yahoo.com/news/rssindex"},
            {"name": "Yahoo_Stocks", "url": "https://finance.yahoo.com/rss/stocks"}
        ]
        results = []
        for src in sources:
            soup = self._get_soup(src['url'], is_xml=True)
            if not soup: continue
            items = soup.find_all('item')
            for item in items:
                title = item.title.text.strip()
                link = item.link.text.split('?')[0]
                pub_date = item.pubDate.text if item.pubDate else ""
                results.append({
                    "source": src['name'],
                    "date": self._standardize_date(pub_date),
                    "title": title,
                    "link": link
                })
        return results

    def scout_news_jp(self):
        """일본 뉴스: 사령관님의 35개 키워드로 정밀 필터링"""
        sources = [
            {"name": "Jiji", "url": "https://www.jiji.com/jc/c?g=eco"},
            {"name": "Reuters_JP", "url": "https://jp.reuters.com/markets/japan/"}
        ]
        results = []
        for src in sources:
            soup = self._get_soup(src['url'])
            if not soup: continue
            links = soup.find_all('a', href=True)
            for link in links:
                title = link.get_text().strip()
                if len(title) > 15 and any(kw in title for kw in self.jp_keywords):
                    href = link['href']
                    full_link = href if href.startswith('http') else (
                        "https://www.jiji.com" + href if src['name'] == "Jiji" else "https://jp.reuters.com" + href
                    )
                    results.append({
                        "source": src['name'],
                        "date": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        "title": title,
                        "link": full_link
                    })
        return results

    def run_all_stations(self):
        """전 지역 통합 정찰 실행"""
        print(f"🚀 [{datetime.now().strftime('%H:%M:%S')}] 전 지역 통합 정찰 개시...")
        
        # 1. 미국/글로벌 (Yahoo - 전체 수집)
        y_count = 0
        for item in self.scout_yahoo():
            if self._is_new(item['link']):
                self._save_to_db(item, country="USA/Global")
                y_count += 1
        
        # 2. 일본 (Jiji, Reuters - 키워드 필터링)
        j_count = 0
        for item in self.scout_news_jp():
            if self._is_new(item['link']):
                self._save_to_db(item, country="Japan")
                j_count += 1
        
        print(f"🏁 작전 종료: 신규 첩보 {y_count + j_count}건 확보 (USA: {y_count}, Japan: {j_count})")

if __name__ == "__main__":
    scouter = MacroScouter()
    scouter.run_all_stations()
