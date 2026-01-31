import sqlite3


def generate_full_report():
    conn = sqlite3.connect("macro_intelligence.db")
    cur = conn.cursor()

    # 분석 결과가 있는 모든 데이터를 날짜 역순으로 가져옵니다.
    cur.execute("""
        SELECT date, title, source, analysis_text, link 
        FROM news 
        WHERE analysis_text IS NOT NULL 
        ORDER BY date DESC
    """)
    rows = cur.fetchall()

    if not rows:
        print("❌ 출력할 분석 데이터가 없습니다. 먼저 분석을 진행해 주세요.")
        return

    report_name = "Full_Analysis_Log.md"

    with open(report_name, "w", encoding="utf-8") as f:
        f.write("# 📑 일본 매크로 분석 전체 로그\n")
        f.write(f"**총 {len(rows)}건의 분석 결과가 포함되어 있습니다.**\n\n")
        f.write("---\n\n")

        for date, title, source, analysis, link in rows:
            # 개별 뉴스 시작
            f.write(f"### 📅 {date} | {source}\n")
            f.write(f"**원문:** {title}\n\n")

            # AI가 작성한 분석 전문 (가독성을 위해 인용구 처리)
            f.write("> **AI 분석 결과**\n")
            f.write(f"{analysis}\n\n")

            # 원문 링크
            f.write(f"🔗 [기사 원문 보기]({link})\n")

            # 구분선
            f.write("\n---\n\n")

    print(f"✅ 보고서 생성 완료! '{report_name}' 파일을 확인하세요.")
    conn.close()


if __name__ == "__main__":
    generate_full_report()
