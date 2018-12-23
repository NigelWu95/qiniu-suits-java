# 持久化结果字段

# 简介
对字段进行选择，持久化时只保留选择的字段。

### 配置文件选项
```
result-path=../result
result-format=
result-separator=
save-total=
key-save=
hash-save=
fsize-save=
putTime-save=
mimeType-save=
endUser-save=
type-save=
status-save=
md5-save=
fops-save=
persistentId-save=
```
`result-path` 表示保存结果的文件路径  
`result-format` 结果保存格式（json/table，将每一条结果记录格式化为对应格式）  
`result-separator` 结果保存为 table 格式时使用的分隔符  
`save-total` 用于选择是否直接保存数据源完整输出结果  
`key-save` true/false 结果保留 key 字段  
`hash-save` true/false 结果保留 hash 字段  
`fsize-save` true/false 结果保留 fsize 字段  
`putTime-save` true/false 结果保留 putTime 字段  
`mimeType-save` true/false 结果保留 mimeType 字段  
`endUser-save` true/false 结果保留 endUser 字段  
`type-save` true/false 结果保留 type 字段  
`status-save` true/false 结果保留 status 字段  
`md5-save` true/false 结果保留 md5 字段  
`fops-save` true/false 结果保留 fops 字段  
`persistentId-save` true/false 结果保留 persistentId 字段  
key-save、hash-save、fsize-save、putTime-save、mimeType-save、endUser-save、
type-save、status-save、md5-save 为 listbucket/stat 的资源信息字段。所有字段选项默认为 true。

### 命令行方式
```
-result-path= -save-total=true -result-format= -result-separator= -key-save= -hash-save= -fsize-save= -putTime-save= -mimeType-save= -endUser-save= -type-save= -status-save= -md5-save= -fops-save= -persistentId-save=
```
