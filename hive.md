## Hive 的内外部表
### 内部表
````hiveql
-- 创建表
CREATE TABLE IF NOT EXISTS t_user (
    id int,
    username string,
    password string,
    gender string,
    age int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n';
-- 载入数据
LOAD DATA INPATH '/bd/user.txt' INTO TABLE t_user;
````
### 外部表
- ``EXTERNAL``外部表关键字 
- ``LOCATION``加载文件夹
````hiveql
-- 外部表的创建 EXTERNAL 一定要要有外部的词 和加载位置 LOCATION 只写到文件夹
-- 案例以1
Create EXTERNAL TABLE if not exists t_user(
    id int,
    username string,
    password string,
    gender string,
    age int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','       -- 字段分割
LOCATION '/bd/user';

-- 案例2
CREATE EXTERNAL TABLE IF NOT EXISTS t_person (
    name string,
    friends array<string>,
    children map<string,int>,
    address struct<street:string ,city:string>
)
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','   -- 字段分割
        COLLECTION ITEMS TERMINATED BY '_'          -- 集合分割
        MAP KEYS TERMINATED BY ':'                  -- map分割
        LINES TERMINATED BY '\n'                    -- 行
    LOCATION '/bd/person';
````
### 载入数据
```hiveql
-- 追加
INSERT INTO t_user1 SELECT id, username FROM t_user;
-- 覆盖
INSERT OVERWRITE TABLE t_user1 SELECT id, username FROM t_user;
```
### 导出数据
#### 通过Sql导出
```hiveql
-- 导出  * 要先mkdir -p 目录 很慢会执行MR
-- 将查询结果导出到本地 LOCAL
INSERT OVERWRITE LOCAL DIRECTORY '/root/user' SELECT * FROM t_user;
-- 按指定的格式将数据导出到 HDFS
INSERT OVERWRITE DIRECTORY '/bd/export/person/'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','   -- 字段用 ，
    COLLECTION ITEMS TERMINATED BY '-'              -- 集合
    MAP KEYS TERMINATED BY ':'                      -- map
    LINES TERMINATED BY '\n'                        -- 行
SELECT * FROM t_person;
```
#### 通过 HDFS 导出
```hiveql
-- 将表结构和数据同时导出 用于备份
EXPORT TABLE t_person TO '/bd/export/person';
-- 测试删除
DROP TABLE t_person;
-- 恢复表结构和数据：
IMPORT FROM '/bd/export/person';
```
## Hive 基本查询
### 基本语法
- Hive 的四种排序
  - 全局排序：ORDER BY 不推荐，会把所有的而数据放入一个 tasks
  - 内部排序：SORT BY
  - 分区排序：DISTRIBUTE BY
  - 组合排序：CLUSTER BY  会把内部和分区结合
```hiveql
-- 局部排序
SELECT * FROM emp SORT BY sal DESC;
```
| empno | ename | job | mgr | hiredate | sal | comm | deptno |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 7839 | KING | PRESIDENT | null | 1981-11-17 | 5000 | null | 10 |
| 7788 | SCOTT | ANALYST | 7566 | 1987-07-13 | 3000 | null | 20 |
| 7698 | BLAKE | MANAGER | 7839 | 1981-05-01 | 2850 | null | 30 |
| 7782 | CLARK | MANAGER | 7839 | 1981-06-09 | 2450 | null | 10 |
| 7844 | TURNER | SALESMAN | 7698 | 1981-09-08 | 1500 | 0 | 30 |
| 7654 | MARTIN | SALESMAN | 7698 | 1981-09-28 | 1250 | 1400 | 30 |
| 7566 | JONES | MANAGER | 7839 | 1981-04-02 | 2975 | null | 20 |
| 7499 | ALLEN | SALESMAN | 7698 | 1981-02-20 | 1600 | 300 | 30 |
| 7934 | MILLER | CLERK | 7782 | 1982-01-23 | 1300 | null | 10 |
| 7521 | WARD | SALESMAN | 7698 | 1981-02-22 | 1250 | 500 | 30 |
| 7876 | ADAMS | CLERK | 7788 | 1987-07-13 | 1100 | null | 20 |
| 7900 | JAMES | CLERK | 7698 | 1981-12-03 | 950 | null | 30 |
| 7902 | FORD | ANALYST | 7566 | 1981-12-03 | 3000 | null | 20 |
| 7369 | SMITH | CLERK | 7902 | 1980-12-17 | 800 | null | 20 |

```hiveql
-- 按照 什么 分区 DISTRIBUTE BY 什么
SELECT * FROM emp DISTRIBUTE BY deptno SORT BY deptno DESC;
```
| empno | ename | job | mgr | hiredate | sal | comm | deptno |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 7654 | MARTIN | SALESMAN | 7698 | 1981-09-28 | 1250 | 1400 | 30 |
| 7900 | JAMES | CLERK | 7698 | 1981-12-03 | 950 | null | 30 |
| 7698 | BLAKE | MANAGER | 7839 | 1981-05-01 | 2850 | null | 30 |
| 7521 | WARD | SALESMAN | 7698 | 1981-02-22 | 1250 | 500 | 30 |
| 7844 | TURNER | SALESMAN | 7698 | 1981-09-08 | 1500 | 0 | 30 |
| 7499 | ALLEN | SALESMAN | 7698 | 1981-02-20 | 1600 | 300 | 30 |
| 7934 | MILLER | CLERK | 7782 | 1982-01-23 | 1300 | null | 10 |
| 7839 | KING | PRESIDENT | null | 1981-11-17 | 5000 | null | 10 |
| 7782 | CLARK | MANAGER | 7839 | 1981-06-09 | 2450 | null | 10 |
| 7788 | SCOTT | ANALYST | 7566 | 1987-07-13 | 3000 | null | 20 |
| 7566 | JONES | MANAGER | 7839 | 1981-04-02 | 2975 | null | 20 |
| 7876 | ADAMS | CLERK | 7788 | 1987-07-13 | 1100 | null | 20 |
| 7902 | FORD | ANALYST | 7566 | 1981-12-03 | 3000 | null | 20 |
| 7369 | SMITH | CLERK | 7902 | 1980-12-17 | 800 | null | 20 |
## 高级查询
### 一行变多行
#### EXPLODE
`EXPLODE()` 可以将 Hive 一行中复杂的 Array 或者 Map 结构拆分成多行，那如何将某个列的数据转为数组呢？可以配置 `SPLIT` 函数一起使用。
`LATERAL VIEW`侧视图使用
```hiveql
SELECT EXPLODE(SPLIT(types,"-")) FROM t_movie1;

-- 　如果我还想查看一下数组中这些电影类型隶属于哪个电影，需要配合侧视图 LATERAL VIEW 一起使用。
-- movie_type 是侧视图别名
SELECT id, name, type
FROM t_movie1,
-- 生成侧视图（表）AS 后面是侧视图的字段
     LATERAL VIEW EXPLODE(SPLIT(types,"-")) movie_type AS type;
```
### 多行变一行
> `COLLECT_SET()` 和 `COLLECT_LIST()` 可以将多行数据转成一行数据，区别就是 `LIST` 的元素可`重复`而 `SET` 的元素是`去重`的。
```hiveql
SELECT id, name,
CONCAT_WS(':', COLLECT_SET(type)) AS type_set,
CONCAT_WS(':', COLLECT_LIST(type)) AS type_list
FROM t_movie2
GROUP BY id, name;
```
| id | name | type\_set | type\_list |
| :--- | :--- | :--- | :--- |
| 4 | 东邪西毒 | 剧情:武侠:古装 | 剧情:剧情:剧情:武侠:古装 |
| 2 | 七武士 | 动作:冒险:剧情 | 动作:冒险:剧情 |
| 1 | 这个杀手不太冷 | 剧情:动作:犯罪 | 剧情:动作:犯罪 |
| 3 | 勇敢的心 | 动作:传记:剧情:历史:战争 | 动作:传记:剧情:历史:战争 |
| 5 | 霍比特人 | 动作:奇幻:冒险 | 动作:奇幻:冒险 |
### URL解析
> `PARSE_URL_TUPLE`
```hiveql
SELECT PARSE_URL_TUPLE(url, 'PROTOCOL', 'HOST', 'PATH', 'QUERY') FROM t_mall;
```
| c0 | c1 | c2 | c3 |
| :--- | :--- | :--- | :--- |
| https | search.jd.com | /Search | keyword=华为&enc=utf-8&wq=华为 |
| https | s.taobao.com | /search | q=苹果 |
```hiveql
SELECT a.id, a.name, b.protocol, b.host, b.path, b.query
FROM t_mall a,
LATERAL VIEW
PARSE_URL_TUPLE(url, 'PROTOCOL', 'HOST', 'PATH', 'QUERY') b AS protocol, host, path, query;
```
| id | name | protocol | host | path | query |
| :--- | :--- | :--- | :--- | :--- | :--- |
| 1 | jingdong | https | search.jd.com | /Search | keyword=华为&enc=utf-8&wq=华为 |
| 2 | taobao | https | s.taobao.com | /search | q=苹果 |
### JSON解析
- 关键字  
  - `GET_JSON_OBJECT`
  - `JSON_TUPLE`
```hiveql
SELECT
GET_JSON_OBJECT(user_json, '$.id') AS id,
GET_JSON_OBJECT(user_json, '$.username') AS username,
GET_JSON_OBJECT(user_json, '$.gender') AS gender,
GET_JSON_OBJECT(user_json, '$.age') AS age
FROM t_user_json;
------------
SELECT
  JSON_TUPLE(user_json, 'id', 'username', 'gender', 'age') AS (id, username, gender, age)
FROM t_user_json;
```
| id | username | gender | age |
| :--- | :--- | :--- | :--- |
| 1 | admin | 男 | 18 |
| 2 | zhangsan | 男 | 23 |
| 3 | lisi | 女 | 16 |
## Hive 分区/分桶
### Hive 数据模型
- 单独的一个表
- 对表进行分区
- 对表进行分区，再分桶； 
- 对表直接分桶。
### Hive 分区
> 使用分区技术，可以避免 Hive 全表扫描，提升查询效率；同时能够减少数据冗余进而提高特定（指定分区）查询分析的效率
#### 静态分区
分区表类型分为静态分区和动态分区。区别在于前者是我们手动指定的，后者是通过数据来判断分区的。根据分区的深度又分为单分区与多分区。
##### 单分区
###### 创建分区表
关键字 `PARTITIONED BY` 不能和表里面的字段名一样
```hiveql
-- 单分区：创建分区表 PARTITIONED BY (分区字段名 分区字段类型)
-- 多分区：创建分区表 PARTITIONED BY (分区字段名 分区字段类型, 分区字段名2 分区字段类型2)
CREATE EXTERNAL TABLE IF NOT EXISTS t_student (
sno int,
sname string
) PARTITIONED BY (grade int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
```
###### 添加/产看/删除分区
```hiveql
-- 添加分区
ALTER TABLE t_student ADD IF NOT EXISTS PARTITION (grade=2);

-- 查看分区
SHOW PARTITIONS t_student;

-- 删除分区
ALTER TABLE t_student DROP PARTITION (grade=2);
```
###### 分区载入数据
```hiveql
LOAD DATA INPATH '/bd/s2.txt' INTO TABLE t_student PARTITION (grade=2);
-- 会把文件内的所有都载入分区包括 grade 内不相等的也会变成 PARTITION (grade=2) 内的
```
##### 动态分区
```hiveql
-- 创建静态分区表语法（静态分区和动态分区的建表语句是一样的）：
CREATE TABLE IF NOT EXISTS t_teacher (
tno int,
tname string
) PARTITIONED BY (grade int, clazz int)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

-- 载入数据 一个分区一个分区的载入
LOAD DATA INPATH '/bd/t_teacher/t1.txt' INTO TABLE t_teacher PARTITION (grade=1, clazz=1);
LOAD DATA INPATH '/bd/t_teacher/t2.txt' INTO TABLE t_teacher PARTITION (grade=1, clazz=2);

-- 　也可以使用分区表的 INSERT 语句插入数据（会执行 MR 任务）：
INSERT INTO TABLE t_teacher PARTITION (grade=2, clazz=3) VALUES (10, 'jueyuan10');
```
#### 动态分区
静态分区的列是在编译时期通过用户传递来决定的；动态分区只有在 `SQL` 执行时才能决定
开启动态分区首先要在 Hive 会话中设置以下参数：
```hiveql
-- 开启动态分区支持（默认 false）
SET hive.exec.dynamic.partition=true;
-- 是否允许所有分区都是动态的，strict 要求至少包含一个静态分区列，nonstrict 则无此要求（默认 strict）
SET hive.exec.dynamic.partition.mode=nonstrict;

-- 其余参数的详细配置如下：
-- 每个 Mapper 或 Reducer 可以创建的最大动态分区个数（默认为 100）
-- 比如：源数据中包含了一年的数据，如果按天分区，即 day 字段有 365 个值，那么该参数就需要设置成大于 365，如果使用默认值 100，则会报错
SET hive.exec.max.dynamic.partition.pernode=100;
-- 一个动态分区创建可以创建的最大动态分区个数（默认为 1000）
SET hive.exec.max.dynamic.partitions=1000;
-- 全局可以创建的最大文件个数（默认为 100000）
SET hive.exec.max.created.files=100000;
-- 当有空分区产生时，是否抛出异常（默认为 false）
SET hive.error.on.empty.partition=false;
-- 是否开启严格模式 strict（严格模式）和 nostrict（非严格模式，默认）
SET hive.mapred.mode=strict;
-- 主要是为了禁止某些查询（这些查询可能会造成意想不到的坏结果），目前主要禁止三种类型的查询
```
##### 创建动态分区 （会跑MR）
> 载入数据 会跑MR ， 不会破坏源文件
```hiveql
-- 和静态分区创建的是一样的
-- 单分区：创建分区表 PARTITIONED BY (分区字段名 分区字段类型)
-- 多分区：创建分区表 PARTITIONED BY (分区字段名 分区字段类型, 分区字段名2 分区字段类型2)
CREATE TABLE IF NOT EXISTS t_teacher_d (
    tno int,
    tname string
) PARTITIONED BY (grade int, clazz int)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';
```
##### 载入数据
```hiveql
-- 可以使用本地 HDFS 查询载入
-- 使用 HDFS 载入
LOAD DATA INPATH '/bd/teacher/teacher.txt' INTO TABLE t_teacher_d;
```
### 分桶
> 　所以就有了分桶，分桶是将数据集分解为更容易管理的若干部分的另一种技术，也就是更为细粒度的数据范围划分，将数据按照字段划分到多个文件中去。
#### 分桶原理
> Hive 采用对列值哈希，然后除以桶的个数求余的方式决定该条记录要存放在哪个桶中。
> 　计算公式： bucket num = hash_function(bucketing_column) mod num_buckets 
#### 分桶实践
##### 开启分桶功能
```hiveql
-- 开启分桶功能
SET hive.enforce.bucketing=true;
-- 设置 Reduce 的个数，默认是 -1，-1 时会通过计算得到 Reduce 个数，一般 Reduce 的数量与表中的 BUCKETS 数量一致
-- 有些时候环境无法满足时，通常设置为接近可用主机的数量即可
SET mapred.reduce.tasks=-1 ;          -- 分桶的时候不修改比较好 他会自己计算

-- 分桶语法
-- CREATE TABLE 表名(字段1 类型1，字段2，类型2 )
-- CLUSTERED BY (表内字段)
-- SORTED BY (表内字段)
-- INTO 分桶数 BUCKETS
```
##### 分桶
```hiveql
-- 1 首选创建一个内部表
-- 创建表并设置分桶，BUCKETS 个数会决定在该表或者该表的分区对应的 HDFS 目录下生成对应个数的文件
-- 桶的个数尽可能多的拥有因数
CREATE TABLE IF NOT EXISTS t_citizen_bucket (
    idcard int,
    username string,
    province int
) CLUSTERED BY (idcard) SORTED BY (username DESC) INTO 12 BUCKETS
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n';

-- 2创建一个外部关联表
CREATE EXTERNAL TABLE IF NOT EXISTS t_citizen_e (
    idcard int,
    username string,
    province int
)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
  LOCATION '/bd/citizen';

-- 3 插入数据 （需要执行MR）
INSERT OVERWRITE TABLE t_citizen_bucket SELECT * FROM t_citizen_e;
```
## 数据数据抽样
### 块抽样
> -- 该方式允许 Hive 随机抽取 N 行数据，数据总量的百分比（n百分比）或 N 字节的数据
SELECT * FROM <Table_Name> TABLESAMPLE(N PERCENT|ByteLengthLiteral|N ROWS) s;
```hiveql
-- 案例
-- 数字与 ROWS 之间要有空格
CREATE TABLE IF NOT EXISTS
t_citizen_block_sample AS SELECT * FROM t_citizen_e TABLESAMPLE(1000 ROWS);
-- 数字与 PERCENT 之间要有空格
CREATE TABLE IF NOT EXISTS
t_citizen_block_sample AS SELECT * FROM t_citizen_e TABLESAMPLE(10 PERCENT);
-- 数字与 M 之间不要有空格
CREATE TABLE IF NOT EXISTS
t_citizen_block_sample AS SELECT * FROM t_citizen_e TABLESAMPLE(1M);
```
缺点：不随机。该方法实际上是按照文件中的顺序返回数据，对分区表，从头开始抽取，可能造成只有前面几个分区的数据。
优点：速度快。
### 分桶抽样
> -- 其中 x 是要抽样的桶编号，桶编号从 1 开始，colname 表示抽样的列，y 表示桶的数量 
> TABLESAMPLE(BUCKET x OUT OF y [ON colname])
```hiveql
-- 案例
-- 假设 Table 总共分了 64 个桶
-- 取一桶且只取第一桶
SELECT * FROM t_citizen_bucket TABLESAMPLE(BUCKET 1 OUT OF 64 ON idcard);
-- 取半桶且只取第一桶的半桶
SELECT * FROM t_citizen_bucket TABLESAMPLE(BUCKET 1 OUT OF 128 ON idcard);
-- 取 16 桶，分别取第 2、6、10、14、18、22、26、30、34、38、42、46、50、54、58、62 桶
SELECT * FROM t_citizen_bucket TABLESAMPLE(BUCKET 2 OUT OF 4 ON idcard);
```
优点：随机且速度最快。
### 随机抽样
> -- 使用RAND()函数和LIMIT关键字来获取样例数据，使用DISTRIBUTE和SORT关键字来保证数据随机分散到Mapper和Reducer 
> -- SORT BY 提供了单个 Reducer 内的排序功能，但不保证整体有序 
> SELECT * FROM <Table_Name> DISTRIBUTE BY RAND() SORT BY RAND() LIMIT <N rows to sample>; 
> -- ORDER BY RAND() 语句可以获得同样的效果，但是性能会有所降低 
> SELECT * FROM <Table_Name> WHERE col=xxx ORDER BY RAND() LIMIT <N rows to sample>;
```hiveql
-- 案例
SELECT * FROM t_citizen_bucket DISTRIBUTE BY RAND() SORT BY RAND() LIMIT 10;
```
优点：提供真正的随机抽样。
缺点 : 速度慢。
## Hive 窗口函数（开窗函数）
### 窗口函数的语法
`over()`，其实计算over句子定于的多行记录
- 分类
  - 聚合型函数
  - 分析型函数
  - 取值型函数
> 语法
> SELECT XX函数() OVER (PARTITION BY 用于分组的列 ORDER BY 用于排序的列 ROWS/RANGE BETWEEN 开始位置 AND 结束位置);
- `XX函数()` ：聚合型窗口函数/分析型窗口函数/取值型窗口函数
- `OVER()` ：窗口函数
  - `PARTITION BY` ：后跟分组的字段，划分的范围被称为窗口
  - `ORDER BY` ：决定窗口范围内数据的排序方式
- 移动窗口
  - 移动范围
  - `ROWS` ：ROWS 后的开始位置是指当前行几位（当前行也参与计算），可以与 BETWEEN 搭配使用表达范围
  - `RANGE` ：数据范围，与 BETWEEN 搭配使用
- 移动方向
  - `CURRENT ROW` ：当前行
  - `PRECENDING` ：向当前行之前移动
  - `FOLLOWING` ：向当前行之后移动
  - `UNBOUNDED` ：起点或终点（一般结合 PRECEDING，FOLLOWING 使用）
    - `UNBOUNDED` PRECEDING ：表示该窗口最前面的行（起点）
    - `UNBOUNDED` FOLLOWING ：表示该窗口最后面的行（终点）
### 基本使用
```hiveql
-- 所有员工的平均薪资，在右侧显示
SELECT 
    ename, deptno,
    AVG(sal) OVER()
FROM emp;

-- 每个部门的平均薪资
SELECT 
    ename, deptno,
    AVG(sal) OVER(PARTITION BY deptno) AS avgsal 
FROM emp;

-- 每个部门的薪水，按照员工进行排序
SELECT 
    ename, deptno, sal, 
    RANK() OVER(PARTITION BY deptno ORDER BY sal DESC ) AS salorder 
FROM emp;
-- ORDER BY 与聚合函数一起使用时，会形成顺序聚合
SELECT ename, deptno, sal,
       SUM(sal) OVER(PARTITION BY deptno ORDER BY sal) AS sumsal 
FROM emp;
```
#### 总结
- 与 GROUP BY 的区别：
  - 结果数据形式
    - 窗口函数可以在保留原表中的全部数据 
    - GROUP BY 只能保留与分组字段聚合的结果
  - 排序范围不同
    - 窗口函数中的 ORDER BY 只是决定着窗口里的数据的排序方式 
    - 普通的 ORDER BY 决定查询出的数据以什么样的方式整体排序 
  - SQL 顺序 
    - GROUP BY 先进行计算 
    - 窗口函数在 GROUP BY 后进行计算
### 窗口函数语法
- 移动范围： 
  - ROWS ：ROWS 后的开始位置是指当前行几位（当前行也参与计算），可以与 BETWEEN 搭配使用表达范围 
  - RANGE ：数据范围，与 BETWEEN 搭配使用 
- 移动方向： 
  - CURRENT ROW ：当前行 
  - PRECENDING ：向当前行之前移动 
  - FOLLOWING ：向当前行之后移动 
  - UNBOUNDED ：起点或终点（一般结合 PRECEDING，FOLLOWING 使用） 
    - UNBOUNDED PRECEDING ：表示该窗口最前面的行（起点） 
    - UNBOUNDED FOLLOWING ：表示该窗口最后面的行（终点）
```hiveql
SELECT ename, deptno, sal,
-- 统计所有人薪资
       SUM(sal) OVER() AS sumsal1,
-- 按部门统计所有人薪资
       SUM(sal) OVER(PARTITION BY deptno) AS sumsal2,
-- 按部门统计所有人薪资，实现累计和的效果
       SUM(sal) OVER(PARTITION BY deptno ORDER BY sal) AS sumsal3,
-- 起点到当前行的窗口聚合，和 sumsal3 一样
       SUM(sal) OVER(PARTITION BY deptno ORDER BY sal ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS
         sumsal4,
-- 前面一行和当前行的窗口聚合
       SUM(sal) OVER(PARTITION BY deptno ORDER BY sal ROWS BETWEEN 1 PRECEDING AND CURRENT ROW) AS sumsal5,
-- 前面一行和当前行和后面一行的窗口聚合
       SUM(sal) OVER(PARTITION BY deptno ORDER BY sal ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS sumsal6,
-- 当前行到终点的窗口聚合
       SUM(sal) OVER(PARTITION BY deptno ORDER BY sal ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS
         sumsal7
FROM emp;
```
| ename | deptno | sal | sumsal1 | sumsal2 | sumsal3 | sumsal4 | sumsal5 | sumsal6 | sumsal7 |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| JAMES | 30 | 950 | 29025 | 9400 | 950 | 950 | 950 | 2200 | 9400 |
| MARTIN | 30 | 1250 | 29025 | 9400 | 3450 | 2200 | 2200 | 3450 | 8450 |
| WARD | 30 | 1250 | 29025 | 9400 | 3450 | 3450 | 2500 | 4000 | 7200 |
| TURNER | 30 | 1500 | 29025 | 9400 | 4950 | 4950 | 2750 | 4350 | 5950 |
| ALLEN | 30 | 1600 | 29025 | 9400 | 6550 | 6550 | 3100 | 5950 | 4450 |
| BLAKE | 30 | 2850 | 29025 | 9400 | 9400 | 9400 | 4450 | 4450 | 2850 |
| MILLER | 10 | 1300 | 29025 | 8750 | 1300 | 1300 | 1300 | 3750 | 8750 |
| CLARK | 10 | 2450 | 29025 | 8750 | 3750 | 3750 | 3750 | 8750 | 7450 |
| KING | 10 | 5000 | 29025 | 8750 | 8750 | 8750 | 7450 | 7450 | 5000 |
| SMITH | 20 | 800 | 29025 | 10875 | 800 | 800 | 800 | 1900 | 10875 |
| ADAMS | 20 | 1100 | 29025 | 10875 | 1900 | 1900 | 1900 | 4875 | 10075 |
| JONES | 20 | 2975 | 29025 | 10875 | 4875 | 4875 | 4075 | 7075 | 8975 |
| SCOTT | 20 | 3000 | 29025 | 10875 | 10875 | 7875 | 5975 | 8975 | 6000 |
| FORD | 20 | 3000 | 29025 | 10875 | 10875 | 10875 | 6000 | 6000 | 3000 |

### 取值型窗口函数
- `LAG(COL, N, DEFAULT_VAL)` ：往前第 N 行数据，没有数据的话用 DEFAULT_VAL 代替 
- `LEAD(COL, N, DEFAULT_VAL) `：往后第 N 行数据，没有数据的话用 DEFAULT_VAL 代替 
- `FIRST_VALUE(EXPR)` ：分组内第一个值，但是不是真正意义上的第一个或最后一个，而是截至到当前行的第一个或最后一个 
- `LAST_VALUE(EXPR)` ：分组内最后一个值，但是不是真正意义上的第一个或最后一个，而是截至到当前行的第一个或最后一个 
- `CUME_DIST()` ：小于等于当前值的行在所有行中的占比，计算某个窗口或分区中某个值的累积分布。假定升序排序，则使用以下公式确定累积分布：小于等于当前值 x 的行数 / 窗口或 PARTITION 分区内的总行数。其中 x 等于ORDER BY 子句中指定的列的当前行中的值 
- `PERCENT_RANK()` ：小于当前值的行在所有行中的占比 
- `NTILE(N)` ：如果把数据按行数分为 n 份，那么该行所属的份数是第几份。注意：N 必须为 INT 类型
```hiveql
SELECT ename, deptno, sal,
FIRST_VALUE(sal) OVER(PARTITION BY deptno ORDER BY sal) AS firstsal,
LAST_VALUE(sal) OVER(PARTITION BY deptno ORDER BY sal) AS lastsal,
LAG(sal, 2, 1) OVER(PARTITION BY deptno ORDER BY sal) AS lagsal,
LEAD(sal, 2,
-1) OVER(PARTITION BY deptno ORDER BY sal) AS leadsal
FROM emp;
```
| ename | deptno | sal | firstsal | lastsal | lagsal | leadsal |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| JAMES | 30 | 950 | 950 | 950 | 1 | 1250 |
| WARD | 30 | 1250 | 950 | 1250 | 1 | 1500 |
| MARTIN | 30 | 1250 | 950 | 1250 | 950 | 1600 |
| TURNER | 30 | 1500 | 950 | 1500 | 1250 | 2850 |
| ALLEN | 30 | 1600 | 950 | 1600 | 1250 | -1 |
| BLAKE | 30 | 2850 | 950 | 2850 | 1500 | -1 |
| MILLER | 10 | 1300 | 1300 | 1300 | 1 | 5000 |
| CLARK | 10 | 2450 | 1300 | 2450 | 1 | -1 |
| KING | 10 | 5000 | 1300 | 5000 | 1300 | -1 |
| SMITH | 20 | 800 | 800 | 800 | 1 | 2975 |
| ADAMS | 20 | 1100 | 800 | 1100 | 1 | 3000 |
| JONES | 20 | 2975 | 800 | 2975 | 800 | 3000 |
| SCOTT | 20 | 3000 | 800 | 3000 | 1100 | -1 |
| FORD | 20 | 3000 | 800 | 3000 | 2975 | -1 |
## Hive 自定义函数
- UDF（User Defined Function）：普通函数，一进一出，比如 UPPER, LOWER 
- UDAF（User Defined Aggregation Function）：聚合函数，多进一出，比如 COUNT/MAX/MIN） 
- UDTF（User Defined Table Generating Function）：表生成函数，一进多出，比如 LATERAL VIEW EXPLODE()

将自定义 UDF 程序打成 jar 包并上传至 HDFS，例如： /bd/hive-demo-1.0-SNAPSHOT.jar 。
在 Hive 中定义自定义函数。
```hiveql
CREATE FUNCTION HELLO_UDF AS 'com.mrhelloworld.udf.HelloUDF'
USING JAR 'hdfs:///bd/hive-demo-1.0-SNAPSHOT.jar';

-- -- 重新加载函数
RELOAD FUNCTIONS;
-- 查看函数详细信息
DESC FUNCTION EXTENDED HELLO_UDF;

-- 　测试函数。
SELECT HELLO_UDF(ename) FROM emp LIMIT 5;

-- 移除函数
--DROP [TEMPORARY] FUNCTION [IF EXISTS] [DBNAME.]FUNCTION_NAME;
DROP FUNCTION HELLO_UDF;
-- 重新加载函数
RELOAD FUNCTIONS;
```
## Hive 压缩/存储
> `压缩`技术能够有效`减少存储系统`的读写字节数，`提高网络带宽和磁盘空间的效率`
> 压缩特性运用的的得当提高性能，运用的不得当降低性能
### 条件
- 为什么使用压缩（优点）：减少存储系统读写字节数、提高网络带宽和磁盘空间的效率。
- 压缩的缺点：使用数据时需要先对文件解压，加重 CPU 负载，压缩算法越复杂，解压时间越长。
- 压缩的条件：空间和 CPU 要充裕。如果机器 CPU 比较紧张，慎用压缩。
- 压缩的技术：
  - 有损压缩（LOSSY COMPRESSION）：压缩和解压的过程中数据有丢失，使用场景：视频。
  - 无损压缩（LOSSLESS COMPRESSION）：压缩和解压的过程中数据没有丢失，使用场景：日志数据。
- 对称和非对称：
  - 对称：压缩和解压的时间一致。
  - 非对称：压缩和解压的时间不一致。
- 基本原则：
  - 计算密集型（CPU-Intensive）作业，少用压缩。
    - 特点：要进行大量的计算，消耗 CPU 资源。比如计算圆周率、对视频进行高清解码等等，全靠 CPU 的运算能力。
    - 计算密集型任务虽然也可以用多任务完成，但是任务越多，花在任务切换的时间就越多，CPU 执行任务的效率就越低，所以，要最高效地利用 CPU，计算密集型任务同时进行的数量应当等于 CPU 的核心数。
    - 计算密集型任务由于主要消耗 CPU 资源，因此，代码运行效率至关重要。Python 这样的脚本语言运行效率很低，完全不适合计算密集型任务。对于计算密集型任务，最好用 C 语言编写。
  - IO 密集型（IO-Intensive）作业，多用压缩。
    - 特点：CPU 消耗很少，任务的大部分时间都在等待 IO 操作完成（因为 IO 的速度远远低于 CPU 和内存的速度）。
    - 涉及到网络、磁盘 IO 的任务都是 IO 密集型任务。对于 IO 密集型任务，任务越多，CPU 效率越高，但也有一个限度。常见的大部分任务都是 IO 密集型任务，比如 Web 应用。
    - IO 密集型任务执行期间，99% 的时间都花在 IO 上，花在 CPU 上的时间很少，因此，用运行速度极快的 C 语言替换 Python 这样运行速度极低的脚本语言，完全无法提升运行效率。对于 IO 密集型任务，最合适的语言就是开发效率最高（代码量最少）的语言，脚本语言是首选，C 语言最差。
  - CPU 核心数是指一个 CPU 由几个核心组成，核心数越多，CPU 运行速度越快，比如处理同一份数据，单核是指一个人处理，双核是指两个人处理，所以核心数越多，CPU 性能越好。

### 压缩实践
#### 压缩支持
使用 hadoop checknative 命令，可以查看是否有相应压缩算法的库，如果显示为 false，则需要额外安装。
```shell
hadoop checknative
```
#### 压缩比较
| 压缩格式    | 算法      | 文件扩展名 | 是否可切分 | 压缩比 | 压缩速度 | 解压速度 |
|---------|---------|-------|-------|-----|------|-----|
| DEFLATE | DEFLATE |.deflate|否|高|低|低|
| Gzip    | DEFLATE |.gz |否 |高| 低 |低|
| BZip2   | BZip2   | .bz2 | 是   | 高  | 低    | 低  |
|LZO| LZO |.lzo |是（需建索引） |低 |高| 高|
|LZ4| LZ4 |.lz4| 否| 低 |高| 高|
|Snappy |Snappy |.snappy| 否 |低 |高 |高|
|Zstd |Zstd |.zst |否| 高| 高 |高|
#### 压缩选择
MR主要三个地方会压缩
- Input：数据来源
  - 从 HDFS 中读取文件进行 MapReuce 作业，如果数据很大，可以使用压缩并且选择支持分片的压缩方式，例如 `BZip2`、`LZO`，这样可以实现并行处理，提高效率，减少磁盘读取时间，同时选择合适的存储格式例如 Sequence Files、RC、ORC 等。
- Transformation：中间计算
  - Map 的输出作为 Reducer的输入，需要经过 Shuffle 这一过程，需要把数据读取到环形数据缓冲区，然后再读取到本地磁盘，所以选择压缩可以减少了存储文件所占空间，提升了数据传输速率，建议使用压缩/解压速度快的压缩方式，例如 `Snappy`、`LZO`、`LZ4`、`Zstd`。
- Output：最后输出
  - 当输出的文件为下一个 `Job 的输入`时，建议：选择可切分的压缩方式例如：`BZip2`。 
  - 当输出的文件直接存到` HDFS 作为归档`时，建议：选择压缩比高的压缩方式。Reduce 阶段数据落盘通常使用 `Gzip` 或 `BZip2` 进行压缩（减少磁盘使用）。
## 案例
### WordCount
- 知识点
  - `LATERAL VIEW EXPLODE` 爆破函数
  - `SPLIT` 切割
  - `REGEXP_REPLACE` 正则
```hiveql
SELECT word, COUNT(*)
FROM t_wordcount
LATERAL VIEW EXPLODE(SPLIT(REGEXP_REPLACE(line, '\[^\\w\'\-\]\+', ' '), '\\s+')) wordtable AS word
GROUP BY word;
```
### 天气信息
每个地区，每天的最高温度、最低温度、平均温度分别是多少？
- 知识点
  - `UNIX_TIMESTAMP` 获取指定时间的 UNIX 时间戳 转换格式看数据
  - `FROM_UNIXTIME` 将时间戳 转换成 时间格式 可自定义
```hiveql
SELECT
  province,city,area_code,
  FROM_UNIXTIME(UNIX_TIMESTAMP(report_time,'d/M/yyyy HH:mm:ss'), 'yyyyMMdd') as ymd,
  max(temperature),
  avg(temperature),
  min(temperature)
FROM t_weather
GROUP BY  province,city,area_code,FROM_UNIXTIME(UNIX_TIMESTAMP(report_time,'d/M/yyyy HH:mm:ss'), 'yyyyMMdd');
```
每个地区，每个月温度最高的三天和温度最低的三天是哪几天？
- 知识点
  - 临时连表: WITH 表名 as (),表面2 as () 后面可以用使用SELECT调用
```hiveql
WITH temp as (
    SELECT province,city,area_code,
        FROM_UNIXTIME(UNIX_TIMESTAMP(report_time,'d/M/yyyy HH:mm:ss'), 'yyyyMMdd') AS ymd,
        MAX(temperature) AS maxT,
        MIN(temperature) AS minT
    FROM
        t_weather
    GROUP BY province,city,area_code,
        FROM_UNIXTIME(UNIX_TIMESTAMP(report_time,'d/M/yyyy HH:mm:ss'), 'yyyyMMdd')
), temp2 as
(SELECT *,count(DISTINCT maxT) over (
    PARTITION BY province,city,area_code,
        FROM_UNIXTIME(UNIX_TIMESTAMP(ymd,'yyyyMMdd'), 'yyyyMM')
    ORDER BY maxT
    ) as p
FROM temp)
SELECT * FROM temp2
WHERE p <= 3;
```
每个省每天最热的城市是哪个？
```hiveql
WITH temp as
  (
     SELECT province,
            FROM_UNIXTIME(UNIX_TIMESTAMP(report_time,'d/M/yyyy HH:mm:ss'), 'yyyyMMdd') as day,
            max(temperature) as tem
     FROM t_weather
     GROUP BY province,  FROM_UNIXTIME(UNIX_TIMESTAMP(report_time,'d/M/yyyy HH:mm:ss'), 'yyyyMMdd')
  )
SELECT distinct temp.province,tw.city,temp.day,temp.tem
FROM temp
         INNER JOIN t_weather tw
                    on temp.province = tw.province and
                       temp.tem = tw.temperature and
                       temp.day =   FROM_UNIXTIME(UNIX_TIMESTAMP(report_time,'d/M/yyyy HH:mm:ss'), 'yyyyMMdd')
ORDER BY temp.province,temp.day;
```
综合每月数据查询出哪个城市的上报日期与创建日期延迟最高？
```hiveql
SELECT 
city, 
MAX(UNIX_TIMESTAMP(create_time, 'd/M/yyyy HH:mm:ss')-UNIX_TIMESTAMP(report_time, 'd/M/yyyy HH:mm:ss')) maxdelay 
FROM t_weather 
GROUP BY 
city,FROM_UNIXTIME(UNIX_TIMESTAMP(report_time, 'd/M/yyyy HH:mm:ss'), 'yyyyMM') 
ORDER BY maxdelay DESC LIMIT 1;
```
查询出 7 月份全国晴天最多的市。
```hiveql
with temp as
     (
         SELECT
             max(province)as province,area_code,FROM_UNIXTIME(UNIX_TIMESTAMP(report_time,'d/M/yyyy HH:mm:ss'), 'yyyyMMdd') as ymd
         FROM t_weather
         WHERE weather='晴' and FROM_UNIXTIME(UNIX_TIMESTAMP(report_time,'d/M/yyyy HH:mm:ss'), 'yyyyMM') = '202007'
         GROUP BY area_code,FROM_UNIXTIME(UNIX_TIMESTAMP(report_time,'d/M/yyyy HH:mm:ss'), 'yyyyMMdd')
     ) SELECT province, count(*) as `count` FROM temp
GROUP BY province
ORDER BY `count` desc
LIMIT 1;
```
### 好友推荐
```hiveql
WITH t as(
    SELECT
        login_user,friend,0 intimacy
    FROM t_friend
             LATERAL VIEW EXPLODE(friends) friendTable AS friend
    WHERE login_user < friend
    union all
-- 间接好友
    SELECT
        friend1 as login_user,friend2 as friend,1 intimacy
    FROM t_friend
             LATERAL VIEW EXPLODE(friends) friendTable AS friend1
             LATERAL VIEW EXPLODE(friends) friendTable AS friend2
    WHERE friend1 < friend2
)  -- 统计 对比关系
SELECT login_user,friend,count(intimacy) as c,sum(intimacy) as s
FROM t
GROUP BY login_user,friend
having c == s
order by login_user
```

 
