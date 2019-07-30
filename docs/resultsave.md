# 结果持久化

## 简介
对于数据源的输出进行转化或过滤并持久化到本地时，对资源信息字段进行选择，持久化时只保留选择的字段。

## 配置文件

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
|save-path| local file 相对路径字符串| 表示保存结果的文件路径|  
|save-format| json/tab/csv| 结果保存格式，将每一条结果记录格式化为对应格式，默认为 tab 格式（减小输出结果的体积）|  
|save-separator| 字符串| 结果保存为 tab 格式时使用的分隔符，结合 save-format=tab 默认为使用 "\t"|  
|save-total| true/false| 用于选择是否直接保存数据源完整输出结果，针对存在下一步处理过程时是否需要保存原始数据，如列举空间文件并修改文件类型时是否保存完整的列举结果，或者存在过滤条件时是否保存过滤之前的结果|  
|rm-fields| 字符串列表| 保存结果中去除的字段，为输入行中的实际，用 "," 做分隔，如 key,hash，表明从结果中去除 key 和 hash 字段再进行保存，不填表示所有字段均保留|  

**rm-fields 对应的字段名列表应为资源元信息字段 key,hash,size,datetime,mime,type,status,md5,owner 及自定义字段名中的一个或几个。**  

**默认情况：**  
（1）本地文件数据源时默认如果存在 process 或者 filter 设置则为 false，反之则为 true（说明可能是单纯格式转换）。  
（2）云存储数据源时如果无 process 则为 true，如果存在 process 且包含 filter 设置时为 false，既存在 process 同时包含 filter 设置时为 true。  
（3）保存结果的路径 **默认（save-path）使用 <bucket>（云存储数据源情况下）名称或者 <path>-result 来创建目录**  

**--** 持数据源久化结果的文件名为 "\<source-name\>\_success_\<order\>.txt"：  
（1）qiniu 存储数据源 =》 "qiniu_success_\<order\>.txt"  
（2）local 源 =》 "local_success_\<order\>.txt"  
如果设置了过滤选项或者处理过程，则过滤到的结果文件名为 "filter_success/error_\<order\>.txt"，process 过程保存的结果为文件为 
"\<process\>_success/error_\<order\>.txt"。  
**--** process 结果的文件名为：<process>_success/error_\<order\>.txt 及 <process>_need_retry_\<order\>.txt，error 的结果表明无
法成功处理，可能需要确认所有错误数据和原因，need_retry 的结果为需要重试的记录，包含错误信息。  
**--** rm-fields 可选择去除某些字段，未设置的情况下保留所有原始字段，数据源导出的每一行信息以目标格式保存在 save-path 的文件中。  

## 命令行方式
```
-save-path= -save-total= -save-format= -save-separator= -rm-fields=
```
