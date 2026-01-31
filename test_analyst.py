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
            "model": "qwen2.5:14b",
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
            2. [심층 분석 요약]: 해당 뉴스의 배경, 현재 시장 상황과의 연계성, 그리고 숨겨진 의미를 포함하여 '최소 3문장 이상'의 상세한 분석을 작성하십시오. 
            3. [전략적 시사점]: 금융시장에 미칠 실질적인 영향.
            4. [추가 리서치]: 이 이슈와 관련하여 다음으로 확인해야 할 경제 지표나 이벤트를 제시하십시오.
            """

            analysis_result = ""
            provider = ""  # 어떤 모델이 분석했는지 기록용

            try:
                # 1. 먼저 Groq(클라우드) 시도
                completion = client.chat.completions.create(
                    model="groq/compound",
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0.0
                )
                analysis_result = completion.choices[0].message.content
                provider = "☁️ Groq"

            except Exception as e:
                # 2. 에러 발생 시 즉시 로컬로 전환
                print(f"[{i}/{total}] ⚠️ Groq 에러({e})... 로컬 투입!")

                try:
                    analysis_result = self._analyze_local(
                        prompt)  # 👈 이미 만든 함수 재활용
                    provider = "💻 Local(Qwen)"
                except Exception as local_e:
                    print(f"❌ [{i}] 모든 분석 실패: {local_e}")
                    continue  # 실패하면 다음 기사로 스킵

            # 3. DB 업데이트 (여기서 저장해야 루프가 돌아가도 데이터가 남습니다)
            cur.execute(
                "UPDATE news SET analysis_text = ? WHERE link = ?", (analysis_result, link))
            conn.commit()
            print(f"[{i}/{total}] 분석 완료 ({provider})")

            # 4. Groq를 쓸 때는 1.5초 휴식 (Rate Limit 방지)
            if provider == "☁️ Groq":
                time.sleep(1.5)

        conn.close()
        print("\n🏁 모든 뉴스 분석이 완료되었습니다!")


if __name__ == "__main__":
    analyst = MacroAnalyst()
    analyst.run_analysis()
