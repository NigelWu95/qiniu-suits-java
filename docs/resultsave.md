# 结果持久化

## 简介
对于数据源的输出进行转化或过滤并持久化到本地时，对资源信息字段进行选择，持久化时只保留选择的字段。

## 配置文件

### 配置参数
```
save-total=
save-path=
save-format=
save-separator=
rm-fields=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|save-total| true/false| 是否直接保存数据源完整输出结果，针对存在下一步处理过程时是否需要保存原始数据|  
|save-path| local file 相对路径字符串| 表示保存结果的文件路径|  
|save-format| json/tab/csv| 结果保存格式，将每一条结果记录格式化为对应格式，默认为 tab 格式（减小输出结果的体积）|  
|save-separator| 字符串| 结果保存为 tab 格式时使用的分隔符，结合 save-format=tab 默认为使用 "\t"|  
|rm-fields| 字符串列表| 保存结果中去除的字段，为输入行中的实际字段选项，用 "," 做分隔，如 key,hash，表明从结果中去除 key 和 hash 字

**关于save-total**  
（1）用于选择是否直接保存数据源完整输出结果，针对存在过滤条件或下一步处理过程时是否需要保存原始数据，如 bucket 的 list 操作需要在列举出结果之后再针
    对字段进行过滤或者做删除，save-total=true 则表示保存列举出来的完整数据，而过滤的结果会单独保存，如果只需要过滤之后的数据，则设置为 false，如
    果是删除等操作，通常删除结果会直接保存文件名和删除结果，原始数据也不需要保存。  
（1）本地文件数据源时默认如果存在 process 或者 filter 则设置 save-total=false，反之则设置 save-total=true（说明可能是单纯格式转换）。  
（2）云存储数据源时默认设置 save-total=true。  
（3）保存结果的路径 **默认（save-path）使用 <bucket\>（云存储数据源情况下）名称或者 <path\>-result 来创建目录**。  

**关于持久化文件名** 
（1）持数据源久化结果的文件名为 "<source-name\>\_success_<order\>.txt"，如 qiniu 存储数据源结果为 "qiniu_success_<order\>.txt"，
    local 数据源结果为 "local_success_<order\>.txt"。  
（2）如果设置了过滤选项或者处理过程，则过滤到的结果文件名为 "filter_success/error_<order\>.txt"。
（3）process 过程保存的结果为文件为 "<process\>\_success/error\_<order\>.txt"，<process\>\_success/error\_<order\>.txt 表明无法
    成功处理的结果，<process\>\_need_retry\_<order\>.txt，表明为需要重试的记录，可能需要确认所有错误数据和记录的错误信息。  

**关于 rm-fields** 
rm-fields 可选择持久化结果中去除某些字段，未设置的情况下保留所有原始字段，数据源导出的每一行信息以目标格式 save-format 保存在 save-path 的文件
中。file 数据源输入字段完全取决于 indexes 和其他的一些 index 设置，可参考 [indexes 索引](datasource.md#关于-indexes-索引)，而其他 index
设置与数据处理类型有关，比如 url-index 来输入 url 信息。对于云储存数据源，不使用 indexes 规定输入字段的话默认是保留所有字段，字段定义可参考[关于文件信息字段](datasource.md#关于文件信息字段)   

## 命令行方式
```
-save-path= -save-total= -save-format= -save-separator= -rm-fields=
```
