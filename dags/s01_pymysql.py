import pymysql  # pip install pymysql
import pandas as pd

# pymysql 사용법
# 1. pymysql 모듈 import
# 2. MySQL db 연결: pymysql.connect() 메서드로 Connection 객체 생성 (conn)
# 3. Connection 객체로부터 cursor() 메서드로 Cursor 객체 생성 (cur)
# 4. Cursor 객체의 execute() 메서드로 sql 쿼리를 mysql db에 전송
# 5. SELECT 쿼리의 경우 fetchall(), fetchone(), fetchmany() 메서드를 사용하여 데이터를 가져옴
#       pd.read_sql() 을 사용하면 데이터를 바로 DataFrame으로 불러올 수 있음
# 6. INSERT/UPDATE/DELETE DML(Data Manipulation Language) 쿼리의 경우 Connection 개체의 commit() 메서드를 사용하여 데이터 확정
# 7. 종료: Connection 객체의 close() 메서드를 사용해 db 연결을 종료


conn = pymysql.connect(host='127.0.0.1', port='3306', user='root', passwd='1234', db='test_db', charset='utf8')

cur = conn.cursor()

# MySQL 데이터 python으로 받기
sql = 'select * from test_tb'
cur.execute(sql)
result = cur.fetchall()  # fetchone(), fetchmany()
print(len(result))



# MySQL 데이터 pandas DataFrame으로 바로 받기
sql = 'SELECT * FROM table_name'
df = pd.read_sql(sql, conn)


# MySQL로 데이터 보내기/수정하기 (수정사항 있으면 commit하기)
sql = '''
{INSERT/UPDATE/DELETE 쿼리}
'''
cur.execute(sql)
conn.commit()
conn.close()
