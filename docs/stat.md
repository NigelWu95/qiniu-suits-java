# 资源查询元信息

## 简介
对空间中的资源查询 stat 信息。参考：[七牛资源元信息查询](https://developer.qiniu.com/kodo/api/1308/stat)

## 配置文件选项

### 配置参数
```
ak=
sk=
bucket=
```

### 参数字段说明
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=stat| 查询资源元信息时设置为stat| 表示查询 stat 信息操作|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取，当数据源方式为 list 时无需再设置|  
|bucket| 字符串| 操作的资源原空间，当数据源为 list 时无需再设置|  

## 命令行方式
```
-process=stat -ak= -sk= 
```

## 备注
stat 操作是 file 源下的操作，从 every line of file 的 key-index 索引获取文件名，当使用 file 源且 parse=tab/csv 时下标必须为整数。
