# 结果持久化

# 简介
对于数据源的输出进行转化或过滤并持久化到本地时，对资源信息字段进行选择，持久化时只保留选择的字段。

### 配置文件选项

#### 必须参数
```
无
```

#### 可选参数
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

### 参数字段说明
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|result-path| localfile相对路径字符串| 表示保存结果的文件路径，需为相对路径，默认为 ../result|  
|result-format| json/table| 结果保存格式，将每一条结果记录格式化为对应格式，默认为 json|  
|result-separator| 字符串| 结果保存为 table 格式时使用的分隔符，默认为 tab 键使用 \t|  
|save-total| true/false| 用于选择是否直接保存数据源完整输出结果，针对存在下一步处理过程时是否需要 <br> 保存原始数据，如列举空间文件并修改文件类型时是否保存完整的列举结果，或者 <br> 存在过滤条件时是否保存过滤之前的结果|  
|key-save| true/false| 结果中是否保留 key 字段|  
|hash-save| true/false| 结果中是否保留 hash 字段|  
|fsize-save| true/false| 结果中是否保留 fsize 字段|  
|putTime-save| true/false| 结果中是否保留 putTime 字段|  
|mimeType-save| true/false| 结果中是否保留 mimeType 字段|  
|endUser-save| true/false| 结果中是否保留 endUser 字段|  
|type-save| true/false| 结果中是否保留 type 字段|  
|status-save| true/false| 结果中是否保留 status 字段|  
|md5-save| true/false| 结果中是否保留 md5 字段|  
|fops-save| true/false| 结果中是否保留 fops 字段|  
|persistentId-save| true/false| 结果中是否保留 persistentId 字段|  
|newKey-save| true/false| 结果中是否保留 newKey 字段|  
|url-save| true/false| 结果中是否保留 url 字段|  

**key-save、hash-save、fsize-save、putTime-save、mimeType-save、endUser-save、type-
save、status-save、md5-save 为 listbucket/stat 的资源信息字段，所有选项字段默认值均为 true。**

### 命令行方式
```
-result-path= -save-total=true -result-format= -result-separator= -key-save= -hash-save= -fsize-save= -putTime-save= -mimeType-save= -endUser-save= -type-save= -status-save= -md5-save= -fops-save= -persistentId-save=
```
