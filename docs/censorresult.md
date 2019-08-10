# 查询 censor 内容审核结果

## 简介
对内容审核返回的 jobId 查询 censorresult 操作。参考：[七牛内容审核结果查询](https://developer.qiniu.com/censor/api/5620/video-censor#4)

## 配置文件
**操作需指定数据源，请先[配置数据源](datasource.md)**  

### 配置参数
```
process=censorresult 
id-index=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=censorresult| 查询censor 内容审核结果时设置为censorresult| 表示查询 censor 内容审核结果操作|  
|id-index| 字符串| 转码结果查询所需 jobId 的索引（下标），censorresult 操作时必须指定 |  

## 命令行方式
```
-process=censorresult -id-index=
```

## 备注
censorresult 操作是 file 源下的操作，从 every line of fileinput 的 id-index 索引获取 jobId。