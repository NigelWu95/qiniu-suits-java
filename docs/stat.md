# 资源查询元信息

# 简介
对空间中的资源查询 stat 信息。参考：[七牛资源元信息查询](https://developer.qiniu.com/kodo/api/1308/stat)

### 配置文件选项

#### 必须参数
|数据源方式|参数及赋值|  
|--------|-----|  
|source-type=file（文件资源列表）|[file 数据源参数](fileinput.md) <br> process=stat |  

#### 可选参数
```
无
```

### 参数字段说明
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=stat| 查询资源元信息时设置为stat| 表示查询 stat 信息操作|  

### 命令行方式
```
-process=stat 
```

### 备注
stat 操作是针对文件数据源输入的情况，从 every line of fileinput 的 key-index 索引获取文件名
