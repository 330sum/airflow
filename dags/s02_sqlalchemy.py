from sqlalchemy import create_engine  # pip install sqlalchemy
import pandas as pd

# sqlalchemy 사용법
# 1. sqlalchemy create_engine(sql db 연결 라이브러리)을 import
# 2. MySQL db 연결: create_engine() 메서드로 engine 객체 생성. connect() 메서드로 db 연결
# 3. DataFrame → MySQL 내보내기: df.to_sql() 메서드 사용
# 4. 종료: close() 메서드로 db 연결 종료

# engine = create_engine('mysql+pymysql://[사용자명]:[비밀번호]@[호스트:포트]/[사용할 데이터베이스]?charset=utf8', encoding='utf-8')
engine = create_engine('mysql+pymysql://root:1234@127.0.0.1:3306/shop')
conn = engine.connect()


# MySQL 가져오기
try:
    query = """select * from Goods"""
    df = pd.read_sql(query, con=engine)

    print("df =========> ", df)
    # df.head()

finally:
    conn.close()
    engine.dispose()



# DataFrame → MySQL 내보내기
try:
    df.to_sql(name="[table_name]", con=engine, if_exists='append', index=False)

finally:
    conn.close()
