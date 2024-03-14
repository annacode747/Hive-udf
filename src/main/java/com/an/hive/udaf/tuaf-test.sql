show functions like '*udaf*';
drop function if exists HelloUDAFCount;
create function HelloUDAFCount as 'com.an.hive.udaf.HelloUDAFCount' using jar 'hdfs:/udf/Hive-dome-1.0-SNAPSHOT.jar';
drop function if exists SumGenericUDAF;
create function SumGenericUDAF as 'com.an.hive.udaf.SumGenericUDAF' using jar 'hdfs:/udf/Hive-dome-1.0-SNAPSHOT.jar';
-- drop function if exists SumResetUDAF;
-- create function SumResetUDAF as 'com.an.hive.udaf.SumResetUDAF' using jar 'hdfs:/udf/Hive-dome-1.0-SNAPSHOT.jar';

drop function if exists sumgenericudaf;
create function SumResetUDAF as 'com.an.hive.udaf.SumResetUDAF' using jar 'hdfs:/udf/Hive-dome-1.0-SNAPSHOT.jar';

create function SumReset as 'com.an.hive.field.FieldLength' using jar 'hdfs:/udf/Hive-dome-1.0-SNAPSHOT.jar';
create function SumReset2 as 'com.an.hive.udaf.SumReset2' using jar 'hdfs:/udf/Hive-dome-1.0-SNAPSHOT.jar';
create function ExampleUDAF as 'com.an.hive.udaf.ExampleUDAF' using jar 'hdfs:/udf/Hive-dome-1.0-SNAPSHOT.jar';
drop function if exists ExampleUDAF;

create function JsonMap as 'com.an.hive.udaf.JsonMap' using jar 'hdfs:/udf/Hive-dome-1.0-SNAPSHOT.jar';
drop function if exists JsonMap;

SELECT JsonMap("4",'2');


show functions like '*json*';


WITH table1 AS (
    SELECT -7 AS c1, 1 AS c2    , 1 as p1
    UNION ALL
    SELECT 6  AS c1, 2 AS c2    , 1 as p1
    UNION ALL
    SELECT 7  AS c1, 3 AS c2    , 1 as p1
    UNION ALL
    SELECT 5  AS c1, 4 AS c2    , 2 as p1
    UNION ALL
    SELECT -8 AS c1, 5 AS c2    , 1 as p1
    UNION ALL
    SELECT 20 AS c1, 6 AS c2    , 2 as p1
    UNION ALL
    SELECT -6 AS c1, 7 AS c2    , 1 as p1
    UNION ALL
    SELECT 3  AS c1, 8 AS c2    , 3 as p1
    UNION ALL
    SELECT 2  AS c1, 9 AS c2    , 1 as p1
    UNION ALL
    SELECT 1  AS c1, 10 AS c2   , 3 as p1
    UNION ALL
    SELECT 9  AS c1, 11 AS c2   , 1 as p1
    UNION ALL
    SELECT -3 AS c1, 12 AS c2   , 4 as p1
    UNION ALL
    SELECT -2 AS c1, 13 AS c2   , 1 as p1
    UNION ALL
    SELECT 6  AS c1, 14 AS c2   , 5 as p1
)
, t2 as (
    SELECT p1,SumResetUDAF(c1) -- over (partition by p1 order by c2) as cc
    FROM table1
    group by p1
--     order by c2
)
SELECT * FROM t2;
SELECT sum(c1) as c1_cum , sum(if(cc>0,cc,0)) as cc_sum
FROM t2
;