# ods
## 获取数据源
- flume 获取的数据源 -> 如果已经导入了 hdfs 直接使用
- mysql 获取的数据源 -> 使用 dataX 导入hdfs
## 使用数据源
获取的元数据会进行分区 `分区格式：[路径]/dt=[分区字段]`
### 创建外部表
```sql
-- 创建外部表
CREATE EXTERNAL TABLE if not EXISTS ods.ods_app_event_log(
    account string,
    appId string,
    appVersion string,
    carrier string,
    deviceId string,
    eventId string,
    ip string,
    latitude double,
    longitude double,
    netType string,
    osName string,
    osVersion string,
    properties map<string,string>, -- 事件属性
    resolution string, -- 分辨率
    sessionId string, -- 会话id
    `timeStamp` bigint -- 事件时间
)
partitioned by (dt string)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'      -- 设置json格式
stored as textfile
location 'hdfs://hdfs-an/an/app/ods/ods_app_event_log';         -- 数据源路径
```
### 关联分区-入库 `就算是json内没有分表字段 主要文件夹上有就行，会自动识别`
```sql
-- 1. 常规入库
load data inpath 'hdfs://hdfs-an/an/app/ods/ods_app_event_log/dt=${start_date}'
into table ods.ods_app_event_log
partition(dt='${start_date}');
    
-- 2. 存储符合分区格式
--  自动分区，首次分区可用，如果是需要后面加分区推荐下面的命令
msck repair table ods.ods_app_event_log;
--      或者  加分区
alter table ods.ods_app_event_log add if not exists partition(dt='${start_date}');
-- 测试案例
alter table ods.ods_app_event_log add if not exists partition(dt='2021-06-01');
-- 删除
alter table ods.ods_app_event_log drop partition(dt='2021-06-01');
alter table ods.ods_app_event_log drop partition(dt='2023-08-17');

SELECT * FROM ods.ods_app_event_log WHERE dt = "2023-08-17" limit 50;
```
### 自动分区脚本开发
  ```shell
  #!/bin/bash
  start_date=$(date -d'-1 day' +'%Y-%m-%d')
  table="ods.ods_app_event_log"
  path="/an/ods/ods_app_event_log"
  if [ $1 ];then
  start_date=$1
  fi
  sql="alter table ${table} add if not exists partition(dt='${start_date}');"
  echo "待执行的sql为：$sql"
  hive -e "$sql"
  
  # 判断任务执行是否成功
  if [ $? -eq 0 ];then
    echo "日志数据日期： $start_date ; 数据源目录：${path}/dt=${start_date} ; 目标表：${table}"
    exit 0
  else
    echo "日志数据日期： $start_date ; 数据源目录：${path}/dt=${start_date} ; 目标表：${table}"
    exit 1
  fi
  ```

