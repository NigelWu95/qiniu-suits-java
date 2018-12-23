# 查询 pfop 转码结果

# 简介
对转码返回的 persistentId 查询 pfopresult 操作。参考：[七牛转码结果查询](https://developer.qiniu.com/dora/manual/1294/persistent-processing-status-query-prefop)

### 配置文件选项

#### 必须参数
|数据源方式|参数及赋值|  
|--------|-----|  
|source-type=file（文件资源列表）|[file 数据源参数](fileinput.md) <br> process=pfopresult |  

#### 可选参数
```
无
```

### 参数字段说明
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=pfopresult| 查询 pfop 转码结果时设置为pfopresult| 表示查询 pfop 转码结果操作|  

### 命令行方式
```
-process=pfopresult 
```

### 备注
pfopresult 操作是针对文件数据源输入的情况，从 every line of fileinput 的 persistentId-index 索引
获取 persistentId
