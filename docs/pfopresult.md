# 查询 pfop 转码结果操作

# 简介
对转码返回的 persistentId 查询 pfopresult 操作。

### 配置文件选项
```
process=pfopresult
```
`process=pfopresult` 表示查询 pfopresult 操作  

### 命令行方式
```
-process=pfopresult 
```

### 备注
pfopresult 操作是针对文件数据源输入的情况，从 every line of fileinput 的 persistentId-index 索
引获取 persistentId
