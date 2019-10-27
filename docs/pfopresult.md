# 查询 pfop 转码结果

## 简介

如果在提交转码操作时没有设置回调，那么之后通常需要查询所提交的任务处理结果，在经过上一步请求的批量提交之后，会将包含文件名 key 和任务 persistentId
的结果保存在 save-path 中，此时可以通过将 save-path 的路径设置为 path 的 localfile 数据源，进行 pfopresult 操作，参考：[七牛转码结果查询](https://developer.qiniu.com/dora/manual/1294/persistent-processing-status-query-prefop)，查询出任务的处理结果，qsuits 会自动将查询到的处理结果分类，成功的、失败的、正在处理的，以及回调失败的都会单独使用结果文件来保存，这样在查询之后如果有尚未出成功的可以检查错误或重新提交，正在处理的可以进行二次查询，查询结果的保存文件名通常如下：  
`pfopresult_success_xxx.txt` 表示查询到的处理成功的结果  
`pfopresult_error_xxx.txt` 表示查询到的处理失败的结果  
`pfopresult_notify_failed_xxx.txt` 表示查询到的回调失败的结果  
`pfopresult_waiting_xxx.txt` 表示查询到的正在处理中的结果  
`pfopresult_need_retry_xxx.txt` 表示需要重新提交查询的部分任务  

1. **操作需指定数据源，因为该操作是针对 persistentId 进行查询，因此只支持本地 localfile 数据源，请先[配置数据源](datasource.md)**  

## 配置文件

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
