# 结果持久化

## 简介
对于数据源的输出进行转化或过滤并持久化到本地时，对资源信息字段进行选择，持久化时只保留选择的字段。

## 配置文件选项

### 配置参数
```
save-path=
save-format=
save-separator=
save-total=
rm-fields=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|save-path| localfile相对路径字符串| 表示保存结果的文件路径，需为相对路径，默认为 "./result/"|  
|save-format| json/tab/csv| 结果保存格式，将每一条结果记录格式化为对应格式，默认为 tab 格式（减小输出结果的体积）|  
|save-separator| 字符串| 结果保存为 tab 格式时使用的分隔符，结合 save-format=tab 默认为使用 "\t"|  
|save-total| true/false| 用于选择是否直接保存数据源完整输出结果，针对存在下一步处理过程时是否需要保存原始数据，如列举空间文件并修改文件类型时是否保存完整的列举结果，或者存在过滤条件时是否保存过滤之前的结果|  
|rm-fields| 字符串列表| 保存结果中去除的字段，为输入行中的实际，用 "," 做分隔，如 key,hash，表明从结果中去除 key 和 hash 字段再进行保存，不填表示所有字段均保留|  

**rm-fields 对应的字段名列表应为资源元信息字段 key,hash,fsize,putTime,mimeType,type,status,md5endUser 加上自定义的字段名中的一个或几个。**

## 命令行方式
```
-save-path= -save-total= -save-format= -save-separator= -rm-fields=
```
