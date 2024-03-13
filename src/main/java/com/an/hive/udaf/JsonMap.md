## JsonMap的使用案例
添加函数
```sparksql
drop function if exists JsonMap;
create function JsonMap as 'com.an.hive.udaf.JsonMap' using jar 'hdfs:/udf/Hive-dome-1.0-SNAPSHOT.jar';
```
### 简单的使用
#### 1. 案例一
```sparksql
select JsonMap("1",2);
```
结果

| \_c0 |
| :--- |
| {"1":"2"} |

#### 2. 案例二
```sparksql
WITH table1 AS (
    SELECT '-7' AS c1, '1' AS c2    , '1' as p1
    UNION ALL
    SELECT '6'  AS c1, '2' AS c2    , '1' as p1
)
SELECT JsonMap(c1,c2),p1 FROM table1 group by p1;
```
结果

| \_c0 | p1 |
| :--- | :--- |
| {"6":"2","-7":"1"} | 1 |

#### 3. 案例三
```sparksql
WITH table1 AS (
    SELECT '-7' AS c1, '1' AS c2    , '1' as p1
    UNION ALL
    SELECT '6'  AS c1, '2' AS c2    , '1' as p1
    UNION ALL
    SELECT '7'  AS c1, '3' AS c2    , '1' as p1
    UNION ALL
    SELECT '5'  AS c1, '4' AS c2    , '2' as p1
    UNION ALL
    SELECT '-8' AS c1, '5' AS c2    , "1" as p1
    UNION ALL
    SELECT '20' AS c1, '6' AS c2    , '2' as p1
    UNION ALL
    SELECT '-6' AS c1, '7' AS c2    , '1' as p1
    UNION ALL
    SELECT '3'  AS c1, '8' AS c2    , '3' as p1
    UNION ALL
    SELECT "2"  AS c1, '9' AS c2    , '1' as p1
    UNION ALL
    SELECT '1'  AS c1, '10' AS c2   , '3' as p1
    UNION ALL
    SELECT '9'  AS c1, '11' AS c2   , '1' as p1
    UNION ALL
    SELECT '-3' AS c1, '12' AS c2   , '4' as p1
    UNION ALL
    SELECT '-2' AS c1, '13' AS c2   , '1' as p1
    UNION ALL
    SELECT '6'  AS c1, '14' AS c2   , '5' as p1
)
SELECT JsonMap(c1,c2) over (partition by p1 order by c2),p1 FROM table1;
```
结果

| JsonMap\_window\_0 | p1 |
| :--- | :--- |
| {"-7":"1"} | 1 |
| {"9":"11","-7":"1"} | 1 |
| {"9":"11","-7":"1","-2":"13"} | 1 |
| {"9":"11","-7":"1","6":"2","-2":"13"} | 1 |
| {"9":"11","-7":"1","6":"2","-2":"13","7":"3"} | 1 |
| {"6":"2","-2":"13","7":"3","9":"11","-7":"1","-8":"5"} | 1 |
| {"6":"2","-2":"13","7":"3","9":"11","-6":"7","-7":"1","-8":"5"} | 1 |
| {"2":"9","6":"2","-2":"13","7":"3","9":"11","-6":"7","-7":"1","-8":"5"} | 1 |
| {"5":"4"} | 2 |
| {"5":"4","20":"6"} | 2 |
| {"1":"10"} | 3 |
| {"1":"10","3":"8"} | 3 |
| {"-3":"12"} | 4 |
| {"6":"14"} | 5 |


