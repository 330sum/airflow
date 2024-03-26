```sql
CREATE TABLE iceberg.test.testtest (
    id bigint,
    name varchar
) WITH (
    format = 'PARQUET'
    ,partitioning = ARRAY['id']
    ,location = 's3a://hello/hi/ice/test/th_tbl'
)
```

```sql
# 오라클 디비 복사 https://javaoop.tistory.com/94
create table iceberg.is01.it03
	WITH (
       format = 'PARQUET',
       partitioning = ARRAY['id'],
       location = 's3a://iceberg/iceberg/is01/is03'
    ) as select *
		,CAST(AT_TIMEZONE(NOW(), 'Asia/Seoul') AS TIMESTAMP(6)) AS ETL_DT
		from mariadb.ms01.mt01;
```