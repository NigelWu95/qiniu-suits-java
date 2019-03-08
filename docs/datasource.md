# 数据源输入

# 简介
从支持的数据源中读入文件列表，部分数据需要指定行解析方式或所需格式分隔符，读取指定位置的字段作为输入值进行下一步处理。  

### 配置文件选项

#### 公共参数
```
source-type=list/file (v.2.11 及以上版本也可以使用 source=list/file，或者不设置该参数)
path=
threads=30
unit-len=10000
```

#### file 源可选参数
```
parse=
separator=
indexes=0,1,2
```

##### 参数字段说明
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|source-type/source| 字符串 file/list | 选择从[本地路径文件中读取]还是从[七牛空间列举]资源列表|  
|path| 输入源路径字符串| 资源列表路径，file源时填写本地文件或者目录路径，list源时可填写"qiniu://<bucket>"|  
|parse| 字符串 json/tab/csv| 数据行格式，json 表示使用 json 解析，tab 表示使用分隔符（默认 "\t"）分割解析，csv 表示使用 "," 分割解析|  
|separator| 字符串| 当 parse=tab 时，可另行指定该参数为格式分隔符来分析字段|  
|threads| 整型数字| 表示预期最大线程数，当输入文件个数大于该值时其作为线程数，否则文件个数作为线程数|  
|indexes| 字符串列表| 资源元信息字段索引（下标），设置输入行对应的元信息字段下标，默认只有 key 的下标，parse-type=table 时为 0，
parse-type=json 时默认为 "key"|  

####### 关于 indexes 索引
indexes 指输入行中包含的资源元信息字段的映射关系，指定索引的顺序为 key,hash,fsize,putTime,mimeType,type,status,endUser，默认情况下，程
序只从输入行中读取 key 字段数据，parse-type=table 时索引为 0，parse-type=json 时索引为 "key"，需要指定更多字段时可设置为数字：0,1,2,3,5 
等或者 json 的 key 名称列表，长度不超过 8，长度表明取对应顺序的前几个字段。当 parse-type=table 时索引必须均为整数，如果输入行中本身只包含部分
字段，则可以在缺少字段的顺序位置用 -1 索引表示，例如原输入行中不包含 mimeType 和 type 字段，则可以设置 indexes =0,1,2,3,-1,-1,4,5


#### file 源可选参数
```
parse=
separator=
indexes=0,1,2
```


### 命令行方式
```
-path= -parse= -separator= -indexes=
```
