# SQLExamples
SQL: Query examples with descriptions

I have gathered some SQL function examples from my professional experience. Table and query descriptions were given in the SQL file. 

Some highlighted functions are listed below. 

- GROUP BY, ORDER BY, LEFT JOIN,
- CASE WHEN <Condition> THEN 1 ELSE 0 END AS <ALIAS_NAME>,
- EXTRACT(DATE FROM <DATE COL>),
- TIMESTAMP_DIFF(<DATE_COL1>, <DATE_COL2>),
- ROW_NUMBER() OVER(PARTITION BY <COLMN_1> ORDER BY <COLMN_2>),
- LEAD() OVER(PARTITION BY <COLMN_1> ORDER BY <COLMN_2>),
- LAG() OVER(PARTITION BY <COLMN_1> ORDER BY <COLMN_2>),
- MIN() OVER(PARTITION BY <COLMN_1> ORDER BY <COLMN_2>)

Implementation of some functions may change according to database types such as Impala, BigQuery, Oracle SQL, etc.
