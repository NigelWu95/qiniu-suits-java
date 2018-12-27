# 文件列表输入

# 简介
从本地路径（目录或文件）读入文件列表，指定行解析方式及所需格式分隔符，读取指定位置的字段作为输入
值进行下一步处理。  

### 配置文件选项

#### 必须参数
```
source-type=file
file-path=
parse-type=
```

#### 可选参数
```
separator=
threads=100
indexes=0,1,2
```

### 参数字段说明
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|source-type| 本地文件输入时设置为file | 表示从本地路径文件中读取资源列表|  
|file-path| localfile路径字符串| 资源列表文件路径（相对路径目录或者相对路径文件名）|  
|parse-type| 字符串json/table| 数据行格式，json 表示使用 json 方式来解析，table 表示使用分隔符方式来解析|  
|separator| 字符串| 当 parse-type=table 时，指定格式分隔符来分析字段（默认使用 tab 键 \t 分割）|  
|threads| 整型数字| 表示预期最大线程数，当输入文件个数大于该值时其作为线程数，否则文件个数作为线程数|  
|indexes| 字符串列表| 资源元信息字段索引（下标），设置输入行对应的元信息字段下标，默认只有 key 的下标，parse-type=table 时为 0，parse-type=json 时默认为 "key"|  

#### 关于 indexes 索引
indexes 指输入行中包含的资源元信息字段的映射关系，指定索引的顺序为 key,hash,fsize,putTime,mimeType,endUser,type,status，默认情况下，程
序只从输入行中读取 key 字段数据，parse-type=table 时索引为 0，parse-type=json 时索引为 "key"，需要指定更多字段时可设置为：0,1,2,3,5 等，
长度不超过 8，长度表明取对应顺序的前几个字段。当 parse-type=table 时下标必须为整数。  

### 命令行方式
```
-file-path= -parse-type= -separator= -indexes=
```
