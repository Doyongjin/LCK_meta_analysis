import os
from dotenv import load_dotenv
import psycopg2

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

cur = conn.cursor()
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;")
tables = cur.fetchall()

print(f"[OK] DB 연결 성공: {os.getenv('DB_NAME')}")
print(f"테이블 목록 ({len(tables)}개):")
for t in tables:
    print(f"   - {t[0]}")

cur.close()
conn.close()
