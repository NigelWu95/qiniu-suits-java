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
key-index=
hash-index=
fsize-index=
putTime-index=
mimeType-index=
endUser-index=
type-index=
status-index=
md5-index=
fops-index=
persistentId-index=
newKey-index=
url-index=
```

### 参数字段说明
|参数名|数据类型 | 含义|  
|-----|-------|-----|  
|file-path| | 输入文件路径（相对路径目录或者相对路径文件名）|  
|parse-type| | 数据行格式（json/table），json 表示使用 json 方式来解析，table 表示使用分隔符方式来解析（默认为 json）|  
|separator| | 当 parse-type=table 时，指定格式分隔符来分析字段（默认使用 tab 键 \t 分割）|  
|threads| | 表示预期最大线程数，当输入文件个数大于该值时其作为线程数，否则文件个数作为线程数|  
|key-index| | 资源名索引（下标），parse-type=table 时默认为 0，parse-type=json 时默认为 "key"|  
|hash-index| | 资源 etag hash 值索引（下标），parse-type=table 时默认为 1，parse-type=json 时默认为 "hash"|  
|fsize-index| | 资源大小索引（下标），parse-type=table 时默认为 2，parse-type=json 时默认为 "fsize"|  
|putTime-index| | 资源上传更新时间索引（下标），parse-type=table 时默认为 3，parse-type=json 时默认为 "putTime"|  
|mimeType-index| | 资源 mime 类型索引（下标），parse-type=table 时默认为 4，parse-type=json 时默认为 "mimeType"|  
|endUser-index| | 资源标示符索引（下标），parse-type=table 时默认为 5，parse-type=json 时默认为 "endUser"|  
|type-index| | 资源存储类型索引（下标），parse-type=table 时默认为 6，parse-type=json 时默认为 "type"|  
|status-index| | 资源状态索引（下标），parse-type=table 时默认为 7，parse-type=json 时默认为 "status"|  
|md5-index| | 资源 md5 值索引（下标），parse-type=table 时默认为 8，parse-type=json 时默认为 "md5"|  
|fops-index| | pfop 操作所需命令索引（下标），parse-type=table 时默认为 1，parse-type=json 时默认为 "fops"|  
|persistentId-index| | pfopresult 操作所需的 persistentId 索引（下标），parse-type=table 时默认为 0，parse-type=json 时默认为 "persistentId"|  
|newKey-index| | rename 操作所需要设置的目标文件名索引（下标），parse-type=table 时默认为 1，parse-type=json 时默认为 "newKey"|  
|url-index| | fetch/privateurl/avinfo/qhash 等操作时需要设置的 url 索引（下标），parse-type=table 时默认为 0，parse-type=json 时默认为 "url"|  

#### 关于 index 索引
xxx-index 表示 xxx 字段的索引或者下标值，当 parse-type=json 时，该值可以设置为 json 中对应键，如 json 为：
{"key":"test.txt", "etag":"FkpBug1-0YnnVo_0IT3fFm-op5pv"}，则 key-index=key，hash-index=etag。当 parse-type=table
时，每一行字符串按照分隔符进行分割成字符串数组，index 即为从 0 开始的数组下标，如 test.txt&#8195;kpBug1-
0YnnVo_0IT3fFm-op5pv，则 key-index=0，hash-index=1。  

注：非必要字段可以忽略 index 的设置。json 格式下，索引键与字段名默认相同，table 格式下下标默认为 file
 info: 0-8, fops: 1, persistentId: 0, newKey: 1, url: 0。

### 命令行方式
```
-file-path= -parse-type= -separator= -key-index= -hash-index= -fsize-index= -putTime-index= -mimeType-index= -endUser-index= -type-index= -status-index= -md5-index= -fops-index= -persistentId-index= -newKey-index= -url-index=
```
