import sqlite3
import os
import time
import requests
from groq import Groq
from dotenv import load_dotenv

load_dotenv()
client = Groq(api_key=os.getenv("GROQ_API_KEY"))


class MacroAnalyst:
    def __init__(self, db_path="macro_intelligence.db"):
        self.db_path = db_path
        self.local_url = "http://localhost:11434/api/generate"

    def _analyze_local(self, prompt):
        """로컬 Ollama(qwen2.5:14b)를 통해 분석 수행"""
        payload = {
            "model": "qwen2.5:14b",  # 사령관님의 강력한 로컬 모델
            "prompt": prompt,
            "stream": False
        }
        try:
            response = requests.post(self.local_url, json=payload, timeout=60)
            return response.json()['response']
        except Exception as e:
            return f"❌ 로컬 분석 실패: {e}"

    def run_analysis(self):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()

        # 분석이 안 된 일본 뉴스 가져오기
        cur.execute(
            "SELECT link, title, source FROM news WHERE analysis_text IS NULL AND country='Japan'")
        pending = cur.fetchall()

        total = len(pending)
        if total == 0:
            print("✅ 모든 뉴스가 분석되었습니다.")
            return

        print(f"🚀 총 {total}건의 하이브리드 분석 작전 개시!")

        for i, (link, title, source) in enumerate(pending, 1):
            prompt = f"""
            당신은 20년 경력의 글로벌 헤지펀드 매크로 분석가입니다. 
            아래 일본 경제 뉴스 제목을 분석하여 투자 전략을 보고하세요.

            뉴스 제목: {title}
            출처: {source}

            요구사항 (한국어로 답변):
            1. [제목 번역]: 한국어로 매끄럽게 번역.
            2. [핵심 요약]: 뉴스의 배경과 의미를 전문가적 시각에서 요약.
            3. [정책 톤]: '매파(인상 지지)', '비둘기파(완화 유지)', '중립' 중 선택하고 이유 기술.
            4. [전략적 시사점]: 금융시장에 미칠 실질적인 영향 1가지.
            5. [추가 리서치]: 추가로 확인해야 할 사항.

            * 주의: '중립'은 지양하고 최대한 시장의 방향성을 해석할 것.
            """

            analysis_result = ""
            try:
                # 1. Groq(클라우드) 시도
                completion = client.chat.completions.create(
                    model="groq/compound",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.1
                )
                analysis_result = completion.choices[0].message.content
                print(f"[{i}/{total}] ☁️ Groq 분석 완료")

            except Exception as e:
                # 2. 리밋 도달 시 로컬 스위칭
                if "429" in str(e) or "limit" in str(e).lower():
                    print(f"[{i}/{total}] ⚠️ Groq 리밋! 로컬 M4(Qwen 14B) 정찰병 투입...")
                    analysis_result = self._analyze_local(prompt)
                    print(f"[{i}/{total}] 💻 로컬 분석 완료")
                else:
                    print(f"❌ 에러 발생: {e}")
                    continue

            # DB 업데이트
            cur.execute(
                "UPDATE news SET analysis_text = ? WHERE link = ?", (analysis_result, link))
            conn.commit()

            # Groq를 쓸 때는 1.5초 휴식, 로컬은 바로 진행
            if "☁️" in locals().get('analysis_result', ''):
                time.sleep(1.5)

        conn.close()
        print("\n🏁 모든 뉴스 분석이 완료되었습니다!")


if __name__ == "__main__":
    analyst = MacroAnalyst()
    analyst.run_analysis()
