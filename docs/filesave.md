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
remove-fields=
```

### 参数字段说明
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|result-path| localfile相对路径字符串| 表示保存结果的文件路径，需为相对路径，默认为 ../result|  
|result-format| json/table| 结果保存格式，将每一条结果记录格式化为对应格式，默认为 json|  
|result-separator| 字符串| 结果保存为 table 格式时使用的分隔符，默认为 tab 键使用 \t|  
|save-total| true/false| 用于选择是否直接保存数据源完整输出结果，针对存在下一步处理过程时是否需要 <br> 保存原始数据，如列举空间文件并修改文件类型时是否保存完整的列举结果，或者 <br> 存在过滤条件时是否保存过滤之前的结果|  
|remove-fields| 字符串列表| 保存结果中去除的字段，为输入行中的实际，用 , 做分隔，如 key,hash，表明从结果中去除 key 和 hash 字段再进行保存，不填表示所有字段均保留|  

**remove-fields 对应的字段名列表应为资源元信息字段 key、hash、fsize、putTime、mimeType、endUser、type、status 加上自定义的字段名中的一个或几个。**

### 命令行方式
```
-result-path= -save-total=true -result-format= -result-separator=
```
