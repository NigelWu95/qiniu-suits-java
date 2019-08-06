# 资源查询元信息

## 简介
对空间中的资源查询 stat 信息。参考：[七牛资源元信息查询](https://developer.qiniu.com/kodo/api/1308/stat)

## 配置文件
**操作通常需要指定数据源，默认表示从七牛空间列举文件执行操作，如非默认或需更多条件，请先[配置数据源](datasource.md)**  

### 配置参数
```
process=stat
ak=
sk=
bucket=
indexes=
```

### 参数字段说明
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=stat| 查询资源元信息时设置为stat| 表示查询 stat 信息操作|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取，当数据源为 qiniu 时无需再设置|  
|bucket| 字符串| 操作的资源原空间，当数据源为 qiniu 时无需再设置|  
|indexes|字符串| 设置输入行中 key 字段的下标（有默认值），参考[数据源 indexes 设置](datasource.md#1-公共参数)|  

## 命令行方式
```
-process=stat -ak= -sk= -bucket=
```

## 备注
stat 操作是 file 源下的操作，从 every line of file 的 key-index 索引获取文件名，当使用 file 源且 parse=tab/csv 时下标必须为整数。
