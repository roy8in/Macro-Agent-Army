import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime
import urllib3
from dateutil import parser  # 🆕 지능형 날짜 파싱을 위해 필요합니다.

# SSL 경고 무시
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class MacroScouter:
    def __init__(self):
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        }
        # [기존 일본 키워드]
        self.jp_keywords = ['日銀', '金融政策', '金利', '円', '利上げ', '首相', 'GDP', 'CPI'] 
        # [신규 미국/글로벌 키워드]
        self.en_keywords = ['Fed', 'Rate', 'Inflation', 'CPI', 'Treasury', 'Powell', 'Nvidia', 'Stocks', 'Dollar']
        
        self.db_path = "macro_intelligence.db"
        self._init_db()

    def _init_db(self):
        """DB 및 테이블 초기화"""
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS news 
                       (link TEXT PRIMARY KEY, title TEXT, source TEXT, 
                        country TEXT, date TEXT, collected_at TEXT)''')
        conn.commit()
        conn.close()

    def _standardize_date(self, date_str):
        """ 지저분한 날짜 형식을 'YYYY-MM-DD HH:MM:SS'로 세탁합니다. """
        if not date_str:
            return datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        try:
            # ISO 8601, RFC 822 등 다양한 형식을 자동으로 읽어냅니다.
            dt = parser.parse(date_str)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except Exception:
            # 파싱 실패 시 수동 세척 (T, Z 제거)
            clean = date_str.replace('T', ' ').replace('Z', '')
            return clean[:19]

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
        """야후 파이낸스 정찰 및 날짜 정규화"""
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
                
                # 🚨 날짜 세탁기 적용
                standard_date = self._standardize_date(pub_date)
                
                results.append({
                    "source": src['name'],
                    "date": standard_date,
                    "title": title,
                    "link": link
                })
        return results

    def scout_news_jp(self):
        """일본 뉴스 정찰 및 날짜 정규화"""
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
                    # 일본 뉴스는 보통 수집 시점이 발생 시점과 유사하므로 현재 시간 적용
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
        
        # 1. 미국/글로벌 (Yahoo)
        yahoo_items = self.scout_yahoo()
        y_count = 0
        for item in yahoo_items:
            if self._is_new(item['link']):
                self._save_to_db(item, country="USA/Global")
                y_count += 1
        print(f"🇺🇸 야후 파이낸스: {y_count}건 신규 확보")

        # 2. 일본 (Jiji, Reuters)
        jp_items = self.scout_news_jp()
        j_count = 0
        for item in jp_items:
            if self._is_new(item['link']):
                self._save_to_db(item, country="Japan")
                j_count += 1
        print(f"🇯🇵 일본 시장: {j_count}건 신규 확보")

if __name__ == "__main__":
    scouter = MacroScouter()
    scouter.run_all_stations()
