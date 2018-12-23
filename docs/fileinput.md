# 文件列表输入

# 简介
从本地路径（目录或文件）读入文件列表，指定行解析方式及所需格式分隔符，读取指定位置的字段作为输入
值进行下一步处理。  

### 配置文件选项
```
file-path=
parse-type=
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
`file-path` 输入文件路径（相对路径目录或者相对路径文件名）
`parse-type` 行格式（json/table）  
`separator` table 格式分隔符  
`threads` 表示最大线程数，当输入文件个数超过该值时该值为线程数，否则文件个数作为线程数  
`key-index` 资源名索引（下标）  
`hash-index` 资源 etag hash 值索引（下标）  
`fsize-index` 资源大小索引（下标）  
`putTime-index` 资源上传更新时间索引（下标）  
`mimeType-index` 资源 mime 类型索引（下标）  
`endUser-index` 资源标示符索引（下标）  
`type-index` 资源存储类型索引（下标）  
`status-index` 资源状态索引（下标）  
`md5-index` 资源 md5 值索引（下标）  
`fops-index` 转码命令索引（下标）  
`persistentId-index` 转码操作的 persistentId 索引（下标）  
`newKey-index` 重命名操作所需要设置的目标文件名索引（下标）  
`url-index` fetch/privateurl/avinfo/qhash 等操作时需要设置的 url 索引（下标）  

### 命令行方式
```
-file-path= -parse-type= -separator= -key-index= -hash-index= -fsize-index= -putTime-index= -mimeType-index= -endUser-index= -type-index= -status-index= -md5-index= -fops-index= -persistentId-index= -newKey-index= -url-index=
```

### 关于格式和索引
1. 文件列表输入格式支持 json 字符串或以一致分隔符分隔的 table 字符串，当 parse-type=json 时无需指定 separator，当 parse-
type=table 时必须指定 separator，如 separator=\t 表示字段分隔符为制表符 tab。  
2. xxx-index 表示 xxx 字段的索引或者下标值，当 parse-type=json 时，该值可以设置为 json 中对应键，如 json 为：
{"key":"test.txt", "etag":"FkpBug1-0YnnVo_0IT3fFm-op5pv"}，则 key-index=key，hash-index=etag。当 parse-type=table
时，每一行字符串按照分隔符进行分割成字符串数组，index 即为从 0 开始的数组下标，如 test.txt&#8195;kpBug1-
0YnnVo_0IT3fFm-op5pv，则 key-index=0，hash-index=1。  

注：非必要字段可以忽略 index 的设置。json 格式下，索引键与字段名默认相同，table 格式下下标默认为 file
 info: 0-7, fops: 1, persistentId: 0。
