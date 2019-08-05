# 查询 pfop 转码结果

## 简介
对转码返回的 persistentId 查询 pfopresult 操作。参考：[七牛转码结果查询](https://developer.qiniu.com/dora/manual/1294/persistent-processing-status-query-prefop)

## 配置文件
**操作需指定数据源，请先[配置数据源](../docs/datasource.md)**  

### 配置参数
```
process=pfopresult 
id-index=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=pfopresult| 查询 pfop 转码结果时设置为pfopresult| 表示查询 pfop 转码结果操作|  
|id-index| 字符串| 转码结果查询所需 persistentId 的索引（下标），pfopresult 操作时必须指定 |  

## 命令行方式
```
-process=pfopresult -id-index=
```

## 备注
pfopresult 操作是 file 源下的操作，从 every line of fileinput 的 id-index 索引获取 persistentId。
