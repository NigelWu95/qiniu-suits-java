# 数据源

## 简介
从支持的数据源中读入资源信息列表，部分数据源需要指定行解析方式或所需格式分隔符，读取指定位置的字段作为输入值进行下一步处理。**目前支持的数据源类型分为
几大类型：云存储列举(storage)、本地文件读取(file)**。  

## 配置
数据源分为两种类型：云存储列举(storage)、本地文件读取(file)，可以通过 **path= 来指定数据源地址：  
`path=qiniu://<bucket>` 表示从七牛存储空间列举出资源列表，参考[七牛数据源示例](#1-七牛云存储)  
`path=tencent://<bucket>` 表示从腾讯存储空间列举出资源列表，参考[腾讯数据源示例](#2-腾讯云存储)  
`path=aliyun://<bucket>` 表示从阿里存储空间列举出资源列表，参考[阿里数据源示例](#3-阿里云存储)  
`path=s3://<bucket>` 表示从 aws/s3 存储空间列举出资源列表，参考[S3数据源示例](#4-aws-s3)  
`path=upyun://<bucket>` 表示从又拍云存储空间列举出资源列表，参考[又拍数据源示例](#5-又拍云存储)  
`path=huawei://<bucket>` 表示从华为云存储空间列举出资源列表，参考[华为数据源示例](#6-华为云存储)  
`path=baidu://<bucket>` 表示从百度云存储空间列举出资源列表，参考[百度数据源示例](#7-百度云存储)  
`path=<filepath>` 表示从本地目录（或文件）中读取资源列表，参考[本地文件数据源示例](#8-local-files)  
未设置数据源时则默认从七牛空间进行列举**，配置文件示例可参考 [配置模板](../resources/application.config)，**数据源读取的默认保存路径使用 
\<bucket\> 名称或者 \<path\>-result 来创建目录。**  

### 1 公共参数
> config.txt
```
path=
unit-len=
threads=
indexes=key,etag,fsize
```  
|参数名|参数值及类型 |含义|  
|-----|-------|-----|  
|path| 数据源字符串| 选择从[云存储空间列举]还是从[本地路径文件中读取]资源列表，本地数据源时填写本地文件或者目录路径，云存储数据源时可填写"qiniu://\<bucket\>"、"tencent://\<bucket\>" 等|  
|unit-len| 整型数字| 表示一次读取的文件个数（读取或列举长度，不同数据源有不同默认值），对应与读取文件时每次处理的行数或者列举请求时设置的 limit 参数|  
|threads| 整型数| 表示预期最大线程数，若实际得到的文件数或列举前缀数小于该值时以实际数目为准|  
|indexes| 字符串列表| 资源元信息字段索引（下标），设置输入行对应的元信息字段下标|  

运行参数：`-config=config.txt`

**备注：** indexes、unit-len、threads 均有默认值非必填，indexes 说明及默认值参考下述[ indexes 索引](#关于-indexes-索引)，unit-len 和
threads 说明及默认值参考下述[并发处理](#关于并发处理 )，建议根据需要优化参数配置。  

#### 关于 indexes 索引 
1、`indexes` 是一个配置字段映射关系的参数，即规定用于从输入行中取出所需字段的索引名及映射到目标对象的字段名，程序会解析每一个键值对构成索引表，绝大
多数数据源默认设置为顺序包含 9 个字段：**key,etag,size,datetime,mime,type,status,md5,owner**，参考[文件信息字段](#关于文件信息字段)。但
是对于 file 数据源进行上传操作时或者 `parse=file` 导出文件信息列表时，默认字段为 **key,parent,size,datetime,mime,etag**，key 为解析的文
件名称，字段减少和顺序变化的原因是因为 mime 和 etag 需要经过计算得到，故优先级放低，不是必需的情况下可以舍弃，size 和 datetime 为非关键信息，也
可以选择舍弃，parent 表示该文件所在的父层路径。而文件本身的 filepath 索引则为默认项，字段为 filepath-index。   

2、**indexes 索引的设置方式有三种**  
（1）使用 `indexes=pre-<索引的前几位个数>`，即按照索引的字段顺序需要前几个默认字段，如 `indexes=pre-3`，表示输入字段需要 key,etag,size，如
果是 parse 为分割形式的数据源，那么索引下标会自动使用 `0,1,2` 来填充，如果是字段格式的数据源（如 json 或者文件对象 object），则索引会自动使用 
`key,etag,size` 来填充，`pre-` 后面的数字取几便会顺序填充几个索引，但是数字不能大于默认字段的长度且不能小于 1。  
（2）需要按顺序设置 9 个字段的索引，如 `indexes=0,1,2,3,4,5` 表示取第一个字段为 key，取第二个字段为 etag，以此类推，**列举操作**或者**文件输
入格式为 json**时如 `indexes=key,etag,size,datetime,mime,type` 表示从依次取出 key,etag,size,datetime,mime,type 这些值，后几位不填
写或者中间填写 -1 的表示不需要该部分字段，如 `key,hash,size,-1,timestamp,-1,type` 表示需要 key,hash,size,timestamp,type 这些值，而其
他的不需要。  
（3）不遵循文件信息默认字段的设置方式，此时需要中括号 []，参数格式为 `[key1:index1,key2:index2,key3:index3,...]`，由于命令行终端可能对该参
数格式字符敏感，故可能需要加上 `"` 来设置，例如，输入行以分隔符分割得到字符串数据其中包含三个字段，分别表示<文件名>、<文件大小>、<时间>，那么可以设
置 `indexes="[key:0,size:1,datetime:2]"`，如输入行是包含 key,size,datetime 等字段的 json，则可以设置 `indexes="[key:key,size:key,datetime:datetime]"`，
表示目标对象字段需要 key,size,datetime，且从输入行中进行解析的索引分别为 key,size,key,datetime。因此 `indexes` 可以设置多个键值对，每个键
值对都表示程序中字段与输入行中索引的对应关系，即 `<key>:<index>`。`<key>` 即为表示实际含义的对象字段名，`<index>` 可以为数字或字符串，即将输入
行按照一种格式解析后可以读取对象字段值的索引，为数字且输入格式为 tab/csv 时表示输入行可分隔为 value 数组，采用数组下标的方式读取目标值，为字符串时
可以是 json 行的原始字段名。

3、**默认情况：**  
（1）当数据源为 [storage](#3-storage-云存储列举) 类型时，也可按照[上述 indexes 规范](#关于-indexes-索引)自行设置该参数，用于指定列举结果的
持久化或者下一步 process 操作所需要的字段，默认包含全部下标。对于 datetime 字段，实际上是根据文件对象的 putTime 或者 lastModified 时间戳转
换而来的日期时间字符串，便于了解具体时间信息，默认只能得到 datetime 信息，如果需要原本的时间戳信息，则需要在 indexes 中加入 timestamp 字段，程
序会自动识别并扩展出该字段，如 `indexes=key,etag,size,timestamp,mime,type`，此时持久化结果中或者 process 的输入行中将体现 timestamp，亦
可使两者同时存在，如 `indexes=key,etag,size,datetime,timestamp,mime,type,status,md5,owner`。  

（2）当数据源为 [file-文件列表](#2.1-文本文件行读取)类型时，默认情况下，程序只从输入行中解析 `key` 字段数据，因此当输入格式为 `tab/csv` 时索引
只有 `0`，输入格式为 `json` 时索引只有 `key`，使用默认值时若存在 [filter](filter.md) 过滤字段则会自动添加过滤字段，需要指定更多字段时可按照
[ indexes 规范](#关于-indexes-索引)设置，例如为数字列表:`0,1,2,3,...` 或者 `json` 的 `key` 名称列表，采用默认字段的设置方式时长度不超过 9，
表明取对应顺序的前几个字段，当数据格式为 `tab/csv` 时索引必须均为整数，如果输入行中本身只包含部分字段，则可以在缺少字段的顺序位置用 `-1` 索引表示，
表示跳过该顺序对应的字段，例如原输入行中不包含 mime 字段，则可以设置 `indexes=0,1,2,3,-1,5`。  

（3）对于 file 文件数据源，即扫描 path 下的文件进行上传（`process=qupload`）或者导出列表（`parse=file`），这种数据源参数长度不能超过 6，其 
indexes 的设置通常采用第一种方式，即 `indexes=pre-<个数>`，如果不设置则默认会取得 key，而 filepath 作为必备字段使用单独的 filepath-index
字段来设置索引，但对于 path 下文件的直接扫描不能更改该字段的索引值，默认即为 filepath。  

#### 关于并发处理  
1、云存储数据源，从存储空间中列举文件，可多线程并发列举，用于支持大量文件的加速列举，线程数在配置文件中指定，自动按照线程数检索前缀并执行并发列举。  
2、本地文件数据源，分三种情况：
（1）文本文件的行解析，unit-len 和 threads 默认值分别为 10000 和 50，从本地读取路径下的所有文本文件，一个文件进入一个线程处理，默认情况下最大线
程数由 threads 确定，与输入文件数之间小的值成为实际并发数，因此在需要并发时需要确保目录中存在多个不同的数据源文件。如果对于单文件作为 path 数据源且
希望多线程运行，需要设置 `auto-split=true`，会自动按照线程数模拟分割文件进入多线程处理（极端情况下，如果文件中存在相同的行，是有可能影响完整性的，
虽然发生概率很低，但是建议存在重复行的文件不要使用该模拟分割的方式）。  
（2）文件信息的解析，导出本地文件列表，unit-len 和 threads 默认值分别为 10000 和 50。  
（3）文件的遍历，进行上传操作，unit-len 和 threads 默认值分别为 3 和 50，因为文件上传相对比较耗时，粒度也比较大，unit-len 应该较小，这样断点信
息可信度会比较高。

#### 关于文件信息字段  
文件信息规范字段默认根据七牛存储文件的信息字段进行定义，其释义及各数据源方式对应关系如下：  

|数据源      |key（文件名）    |etag（文件唯一值）|size（文件大小 kb）|datetime（日期时间字符串）|mime（content-type）|type（资源存储类型）|status（资源状态） |md5（文件 md5）|owner（终端标识符）|  
|-----------|---------------|---------------|-----------------|----------------------|-------------------|-----------------|-----------------|-------------|----------------|  
|文件列表输入行|默认 0 或 "key" |默认 1 或 "etag"|默认 2 或 "size"  |默认 3 或 "datetime"    |默认 4 或 "mime"    |默认 5 或 "type"  |默认 6 或 "status"|默认 7 或 "md5"|默认 8 或 "owner"|
|本地文件     |key           |etag            |length          |timestamp 转换          |mime               |无此含义字段       |无此含义字段       |无此含义字段    |无此含义字段      |
|七牛云存储   |key            |eatg（hash）    |fsize            |putTime 转换           |mimeType           |type             |status          |md5           |endUser         |  
|腾讯云存储   |key            |etag           |size             |lastModified 转换      |无此含义字段          |storageClass    |无此含义字段       |无此含义字段    |Owner.displayName|  
|阿里云存储   |key            |etag           |size             |lastModified 转换      |无此含义字段          |storageClass    |无此含义字段       |无此含义字段    |Owner.displayName|  
|AWS云存储/S3|key            |etag           |size             |lastModified 转换      |无此含义字段          |storageClass    |无此含义字段       |无此含义字段    |Owner.displayName|  
|又拍云存储   |name           |无此含义字段     |length           |lastModified 转换      |type/attribute      |无此含义字段       |无此含义字段      |无此含义字段    |无此含义字段       |  
|华为云存储   |key            |metadata.etag  |metadata.contentLength|lastModified 转换 |metadata.contentType|storageClass    |无此含义字段      |metadata.contentMd5|Owner.id    |  
|百度云存储   |key            |etag           |size             |lastModified 转换      |无此含义字段          |storageClass    |无此含义字段       |无此含义字段    |Owner.displayName|  

### 2 file 本地文件读取
本地文件数据源分为**两种情况：（1）读取文件内容为数据列表按行输入（2）读取路径下的文件本身，包括目录遍历，得到文件信息作为输入**  
### 2.1 文本文件行读取  
文件内容为资源列表，可按行读取输入文件的内容获取资源列表，文件行解析参数如下：  
> config.txt
```
parse=tab/json
separator=\t
# 文件内容读取资源列表时一般可能需要设置 indexes 参数（默认只包含 key 字段的解析）
indexes=
auto-split=
add-keyPrefix=
rm-keyPrefix=
uris=
uri-config=
```
|参数名|参数值及类型 |含义|  
|-----|-------|-----|  
|parse| 字符串 json/tab/csv| 数据行格式，json 表示使用 json 解析，tab 表示使用分隔符（默认 `\t`）分割解析，csv 表示使用 `,` 分割解析|  
|separator| 字符串| 当 parse=tab 时，可另行指定该参数为格式分隔符来分析字段|  
|auto-split| true/false|如果输入的 path 为单个文件路径的情况，可以设置该参数为 true，自动分割单文件到多线程中处理，默认为 true，即不分割，单文件仅使用单线程|  
|add-keyPrefix| 字符串|将解析出的 key 字段加上指定前缀再进行后续操作，用于输入 key 可能比实际空间的 key 少了前缀的情况，补上前缀才能获取到资源|  
|rm-keyPrefix| 字符串|将解析出的 key 字段去除指定前缀再进行后续操作，用于输入 key 可能比实际空间的 key 多了前缀的情况，如输入行中的文件名多了 `/` 前缀|  
|uris| 字符串|数据源路径下需要读取的文件名列表，如果只想处理部分文件，可使用参数设置列表的方式，以 `,` 号分割文件名，不设置默认读取 path 下全部文本文件|  
|uri-config| 配置文件路径|配置文件格式为 json，表示 file 数据源中各列表的位置信息，文件名对应的值表示读取该文件的起始位置，可参考[ uri-config 配置](#关于-uri-config)|  

运行参数：`-config=config.txt`

#### 关于 parse
数据源输入的文件列表为每行一条数据，parse 决定了数据的解析方式以及索引的设置方式，以下做简单举例说明，关于具体的其他索引设置参考对应 process 的文档。  
如文件列表为 \t 分割的行：  
```
IMG_0967.MOV	llO8y0gW4GULwF1ry_UcvkA1HXYZ	22257875	2019-01-30T20:15:14.189789900	video/quicktime	0	0	e69957f45a290c0f10b51642b660e6df
IMG_0967.MOVF720.mp4	FoWR3PSyOkXprvMVbQM9SmhDojlO	645768	2019-08-06T18:44:37.193583700	video/mp4	0	0	60618eda5846e623c3cf9028928393bc
IMG_0967F720.mp4	FoWR3PSyOkXprvMVbQM9SmhDojlO	645768	2019-08-06T19:11:13.697041	video/mp4	0	0	60618eda5846e623c3cf9028928393bc
...
```  
则应设置 `parse=tab`，当然 parse=tab 为默认值可以省略，同时可以设置 `indexes=pre-8` 或者 `indexes=0,1,2` 等等，默认值为 `indexes=0`，
或者如在做 copy/move/rename 操作时可以设置 `toKey-index=1`。  
再如文件列表为 json 字符串的行：  
```
{"key":"data/2015/02/01.mp4","id":"NjwAAKG4Gu6bffQV","fops":"avthumb/mp4"}
{"key":"data/2015/02/02.mp4","id":"dUIAAACkEPwNdfQV","fops":"avthumb/mp4"}
```  
则需设置 `parse=json`，indexes 默认值为 `indexes=key`，在做有关 id 的查询时可以 `id-index=id`，或者做 pfop 转码时可以是 `fops-index=fops`，
当然 "id"、"fops" 其实均是对应操作下可以省略的默认值。     

#### 关于 uri-config
uri-config 用来设置要读取的文件路径，在 path 为空的情况下，uri-config 中的文件名必须是完整的路径名，path 为目录时，uri-config 中的文件名可
以采取相对 path 路径下的文件名，因此 uri-config 中的文件名必须存在。配置中每一个文件源对应的值表示在一行文本信息，在实际读取数据源过程中，会参照
该行信息，从之后的位置开始读入数据，即此行文本信息标示文件中的读取位置，可以用于设置断点。

##### uri-config 配置
```json
{
  "/Users/wubingheng/Projects/Github/test/success_1.txt":{
    "start":"test.gif"
  },
  "/Users/wubingheng/Projects/Github/test/success_2.txt":{
    "start":"test.jpg"
  }
}
```  
|选项|含义|  
|-----|-----|  
|key|上述配置文件中的 "/Users/wubingheng/Projects/Github/test/success_1.txt" 等表示文件名或路径，不可重复，重复情况下后者会覆盖前者|  
|value| 表示数据源中某一行的内容，如 "test.gif" 表示 "/Users/wubingheng/Projects/Github/test/success_1.txt" 文件中可能存在某一行包含该信息|  

### 2.2 文件信息读取  
第二种情况，读取文件本身，用于导出本地的文件列表，也可以进行文件上传，解析参数如下：  
```
parse=file
directories=
directory-config=
```  
|参数名|参数值及类型 |含义|  
|-----|-------|-----|  
|parse| 字符串 file| 表示进行文件信息解析格式，解析 path 下文件本身信息时必须设置 parse=file|  
|directories| 字符串|设置数据源路径下需要读取的目录列表，以 `,` 号分割目录名，不设置默认读取 path 下全部目录下的文件|  
|directory-config| 配置文件路径|数据源文件目录及对应已上传的文件名配置，配置中记录已上传的文件在 path 中的位置标识，配置文件格式为 json，可参考[ directory-config 配置](#关于-directory-config)|  

（1）该数据源导出文件列表时默认只包含 filepath 和 key 信息，如果需要 size、date 等其他信息，请参考 [数据源配置](#关于-indexes-索引)。  
（2）用于上传文件的操作时，设置 `process=qupload` 会自动生效，从 `path` 中读取所有文件（除隐藏文件外）执行上传操作，具体配置可参考 [qupload 配置](uploadfile.md)。  

#### 关于 directory-config
directory-config 用来设置要读取的文件目录及位置信息，在 path 为空的情况下，directory-config 中的文件名必须是完整的目录路径，path 为目录时，
directory-config 中的目录名可以采取相对 path 路径下的目录名。配置中每一个目录对应的值表示在一个文件名信息，在实际读取数据源过程中，会参照该文件
名，从之后的文件开始读取，即此文件名信息标示目录中的读取位置，可以用于设置断点。  

##### directory-config 配置
```json
{
  "/Users/wubingheng/Projects/Github/test":{
    "start":"qiniu_success_2.txt"
  }
}
```  
|选项|含义|  
|-----|-----|  
|key|上述配置文件中的 "/Users/wubingheng/Projects/Github/test" 等表示目录名或路径，不可重复，重复情况下后者会覆盖前者|  
|value| 表示数据源中某一行的内容，如 "qiniu_success_1.txt" 表示 "/Users/wubingheng/Projects/Github/test" 目录中可能存在该文件名|  

### 3 storage 云存储列举  
> config.txt
```
<密钥配置>
region=
bucket=
marker=
start=
end=
prefixes=
anti-prefixes=
prefix-left=
prefix-right=
```  
|参数名|参数值及类型 |含义|  
|-----|-------|-----|  
|<密钥配置>|字符串|密钥对字符串|  
|region|字符串|存储区域|
|bucket|字符串| 需要列举的空间名称，通过 "path=qiniu://<bucket\>" 来设置的话此参数可不设置，设置则会覆盖 path 中指定的 bucket 值|  
|prefixes| 字符串| 表示只列举某些文件名前缀的资源，，支持以 `,` 分隔的列表，如果前缀本身包含 `,\=` 等特殊字符则需要加转义符，如 `\,`|  
|prefix-config| 字符串| 该选项用于设置列举前缀的配置文件路径，配置格式为 json，参考[ prefix-config 配置文件](#prefix-config-配置)|
|anti-prefixes| 字符串| 表示列举时排除某些文件名前缀的资源，支持以 `,` 分隔的列表，特殊字符同样需要转义符|  
|prefix-left| true/false| 当设置多个前缀时，可选择是否列举所有前缀 ASCII 顺序之前的文件|  
|prefix-right| true/false| 当设置多个前缀时，可选择是否列举所有前缀 ASCII 顺序之后的文件|  

运行参数：`-config=config.txt`

支持从不同的云存储上列举出空间文件，默认线程数(threads 参数)为 50，千万以内文件数量可以不增加线程，数据源路径等公共参数参考[公共参数配置](#1-公共参数)，
通常云存储空间列举的必须参数包括密钥、空间名(通过 path 或 bucket 设置)及空间所在区域(通过 region 设置，允许不设置的情况下表明支持自动查询)，各数
据源配置参数如下：  

|storage 源|             密钥和 region 字段         |                  对应关系和描述                |  
|------|---------------------------------------|---------------------------------------------|  
|qiniu |`ak=`<br>`sk=`<br>`region=z0/z1/z2/...`|密钥对为七牛云账号的 AccessKey 和 SecretKey<br>region(可不设置)使用简称，参考[七牛 Region](https://developer.qiniu.com/kodo/manual/1671/region-endpoint)|  
|tencent|`ten-id=`<br>`ten-secret=`<br>`region=ap-beijing/...`| 密钥对为腾讯云账号的 SecretId 和 SecretKey<br>region(可不设置)使用简称，参考[腾讯 Region](https://cloud.tencent.com/document/product/436/6224)|  
|aliyun|`ali-id=`<br>`ali-secret=`<br>`region=oss-cn-hangzhou/...`| 密钥对为阿里云账号的 AccessKeyId 和 AccessKeySecret<br>region(可不设置)使用简称，参考[阿里 Region](https://help.aliyun.com/document_detail/31837.html)|  
|aws/s3|`s3-id=`<br>`s3-secret=`<br>`region=ap-east-1/...`| 密钥对为 aws/s3 api 账号的 AccessKeyId 和 SecretKey<br>region(可不设置)使用简称，参考[AWS S3 Region](https://docs.aws.amazon.com/zh_cn/general/latest/gr/rande.html)|  
|upyun |`up-id=`<br>`up-secret=`<br>| 密钥对为又拍云存储空间授权的[操作员](https://help.upyun.com/knowledge-base/quick_start/#e6938de4bd9ce59198)和其密码，又拍云存储目前没有 region 概念|  
|huawei|`hua-id=`<br>`hua-secret=`<br>`region=cn-north-1/...`| 密钥对为华为云账号的 AccessKeyId 和 SecretAccessKey<br>region(可不设置)使用简称，参考[华为 Region](https://support.huaweicloud.com/devg-obs/zh-cn_topic_0105713153.html)|  
|baidu |`bai-id=`<br>`bai-secret=`<br>`region=bj/gz/su...`| 密钥对为百度云账号的 AccessKeyId 和 SecretAccessKey<br>region(可不设置)使用简称，参考[百度 Region](https://cloud.baidu.com/doc/BOS/s/Ojwvyrpgd#%E7%A1%AE%E8%AE%A4endpoint)|  

**支持通过上述参数设置账号，避免使用时需要重复设置或暴露密钥，参考：[账号设置](../README.md#账号设置)**  

#### 数据源完备性和多前缀列举
1、prefix-left 为可选择是否列举所有前缀 ASCII 顺序之前的文件，prefix-right 为选择是否列举所有前缀 ASCII 顺序之后的文件，确保在没有预定义前缀
的情况下仍能列举完整数据。   
2、prefixes 或 prefix-config 用于设置多个 <prefix> 分别列举这些前缀下的文件，如指定多个前缀：[a,c,d]，则会分别列举到这三个前缀下的文件，如果
设置 prefix-config 则 prefixes 配置无效，同时 prefix-config 支持指定列举起始(<start/marker\>)和结束位置(<end\>)，写法如下，配置举例见
[prefix-config 配置](../resources/prefixes.json)。在使用多个前缀列举的同时，可能存在需要列举到**第一个前缀之前**或**最后一个前缀之后**(前
缀会自动按照 ASCII 码排序)的文件，因此设置 prefix-left 和 prefix-right 用于满足该需求。   
**备注：** 又拍云存储强制目录结构以 "/" 作为分隔符，不支持任意前缀列举，列举算法有不同，因此也不支持 prefix-left 和 prefix-right 参数，设置
prefixes 的情况下必须是有效的目录名。  

##### prefix-config 配置
```json
{
  "a":{
    "marker":"",
    "start":"",
    "end":""
  },
  "b":{
    "marker":"",
    "start":"",
    "end":""
  }
}
```  
|选项|含义|  
|-----|-----|  
|key|上述配置文件中的 "a"、"b" 表示文件名前缀，不可重复，重复情况下后者会覆盖前者|  
|marker| 从指定 marker 的位置开始列举，该参数与 start 参数意义相同，同时设置时忽略 start 而使用 marker|  
|start| 从指定文件名开始列举，该参数必须是正确且存在的文件名，否则会产生错误的 marker 从而无法列举|  
|end| 文件名字符串，可以是完整的文件名，也可以是文件名前缀，程序列举时会以该文件信息作为结束位置|  

##### 并发列举
并发列举是为了高效率获取存储空间的文件列表，因此采用经过设计的算法进行并发列举时需要多线程运行，相关使用建议及说明如下： 

1、unit-len 的默认值为 10000 或者 1000（七牛存储空间 unit-len 没有限制，其他云存储一般限制在 1000），由于是多线程算法，
一般即便是列举七牛空间 unit-len 也不需要增大，但是特殊情况下，如空间进行频繁删除且删除数量巨大，或者某个文件前缀下文件数量特别多，出现列举慢的情况，
可以适当加大 unit-len，甚至 100000、200000 都可以，根据空间文件数量而定。  

**2、threads 默认值为 50，该默认值不高，原因是建议根据实际文件数量和机器性能来做配置，多线程运行时一般依赖机器性能，可能会同时读取大量的数据列表在
内存中，因此内存占用率较高，在一般的 4C8G 甚至 4C8G 以下的机器上不建议启用太多线程，如果空间文件数量巨大建议在配置高于 8C16G 的机器上运行，可以提
高线程数，使得列举效率更高，通常可以设置1-3百个线程，8C16G 的机器可以设置 200 线程左右，32C96G 的机器可以到 600 线程甚至更高。**  

3、如果机器性能较佳，如 16C32G，可以设置 threads=300 进行空间列举，在需要考虑机器性能允许（较好或较差）的情况下，超过 10 亿级别文件数量可以设置
threads 大于等于 500，亿级别文件数量可以设置 threads 大于等于 300，千万级别文件数量可以设置 threads 大于等于 200，500 万以内文件数量设置
threads 小于等于 100，100 万左右及以下的文件数量设置 threads 小于等于 50 或使用默认值（则无需设置），10 万及以下文件数量可使用单线程直接列举，
设置 threads=1，如果是空间进行过大量文件删除所剩文件数较少则也需要设置多线程来列举，线程数越高一般情况下相对来说效率会更高，原因是并发列举之前需要分
析空间文件的前缀，线程数多则计算过程中递进到的下级前缀更多，则各线程需要列举的文件数量相对平均，达到比较好的并发效果。  

4、算法描述：使用前缀索引为每一个前缀 (第一级默认为连贯的 ASCII 常见字符) 创建一个列举对象，每个列举对象可以列举直到结束，在获取多个有效的列举对象
之后，分别加入起始（无前缀，但到第一个前缀结束）列举对象和修改终止对象的前缀，随即开始并发执行列举，分别对输出结果进行后续处理。前缀索引个数和起始与终
止列举对象的前缀会随自定义参数 prefixes/prefix-config 和 anti-prefixes 而改变，前者为指定列举的公共前缀，anti-prefixes 表示从所有列举操作
中排除包含该前缀的情况，通常 prefixes 和 anti-prefixes 不同时进行设置。  
<details>
<summary>并发列举算法描述图：点击查看</summary>![云存储文件并发列举算法](云存储文件并发列举算法.jpg)
</details>  

5、部分空间可能存在文件名包含中文等字符，这部分字符没有定义在预定于前缀中，因此在列举时为了保证数据完备性会通过计算进行单独列举，如果空间中存在大量此
类文件名可能会导致并发受到限制，因为此种文件名无法进行更多一级的前缀划分，可能会出现较慢的情况。

6、优化情况：  
(1) 如果空间中某些前缀的文件特别多，则可能出现线程池中大部分线程列举完成了最后还在列举少量前缀的文件，理论上可能会出现在后期列举速度变慢的情况，经过优
化，出现剩余线程数较少时会重新分割前缀再次填充线程池提高并发。  
(2) 公共前缀部分字符串较长，则可能导致计算线程期间列举出来的文件数量无变化（呈现卡住状态，实际是由于计算列举对象时间较长导致），经过优化在产生下级前缀
的过程中，也进行列举，则产生每一级前缀均能得到文件，也优化了内存占用和前缀级数。  
(3) 空间频繁删除过大量文件，经过优化可以消除大量的等待时间和超时情况，同时优化前缀下文件数量较少时尽量少分割下一级。  

### 命令行方式
```
-source= -path= threads= -unit-len= [-<name>=<value>]...
```

## 数据源示例
1、如果已设置账号，则不需要再直接设置密钥，可以通过 `-a=<account-name>`/`-d` 来读取账号，参考：[账号设置](../README.md#账号设置)  
2、如果使用配置文件的方式，假设配置文件名为 `config.txt`，则运行参数为：`-config=config.txt`  

### 1 七牛云存储
命令行参数示例：
```
-path=qiniu://<bucket> -ak= -sk= -threads=300 -prefixes= [-region=]
```  
配置文件示例：
```
path=qiniu://<bucket>
ak=
sk=
threads=300
prefixes=
#region=
```  
**使用七牛数据源除了可以自行指定 region（建议您自行指定 region 较好），也可以单独指定 rsf,rs,api 域名（一般不需要用到），如下所示，一般可用在七
牛私有存储的场景中**：  
```
rsf-domain=rsf-z0.qiniu.com
#rsf-domain=rsf-z1.qiniu.com
rs-domain=rs-z0.qiniu.com
#rs-domain=rs-z1.qiniu.com
api-domain=api.qiniu.com
#api-domain=api-z1.qiniu.com
#是否使用 https 对上述设置的域名进行请求
config-https=true/false
```  

### 2 腾讯云存储
命令行参数示例：
```
-path=tencent://<bucket> -ten-id= -ten-secret= -threads=300 -prefixes= [-region=]
```  
配置文件示例：
```
path=tencent://<bucket>
ten-id=
ten-secret=
threads=300
prefixes=
#region=
```  

### 3 阿里云存储
命令行参数示例：
```
-path=aliyun://<bucket> -ali-id= -ali-secret= -threads=300 -prefixes= [-region=]
```  
配置文件示例：
```
path=aliyun://<bucket>
ali-id=
ali-secret=
threads=300
prefixes=
#region=
```  

### 4 AWS S3
命令行参数示例：
```
-path=s3://<bucket> -s3-id= -s3-secret= -threads=300 -prefixes= [-region=]
```  
配置文件示例：
```
path=s3://<bucket>
s3-id=
s3-secret=
threads=300
prefixes=
#region=
```  
如果是其他基于 S3 实现的数据源，也可以使用 path=s3://<bucket> 方式来列举资源，但是由于 endpoint 与 aws 的不同，需要自行设置 endpoint，
如[七牛的 S3 接口](https://developer.qiniu.com/kodo/manual/4088/s3-access-domainname)华东区域列举 bucket 文件时【使用七牛的 s3 
api 做列举时还需要注意 bucketId（七牛控制台查看） 和 bucketName 的区别，使用 s3 接口必须使用 bucketId 来设置 bucket 参数】，可设置：  
```
# http(s):// 开头可省略
endpoint=s3-cn-east-1.qiniucs.com
```  

### 5 又拍云存储
命令行参数示例：
```
-path=upyun://<bucket> -up-id= -up-secret= -threads=300 -prefixes= [-region=]
```  
配置文件示例：
```
path=upyun://<bucket>
up-id=
up-secret=
threads=300
prefixes=
#region=
```  

### 6 华为云存储
命令行参数示例：
```
-path=huawei://<bucket> -hua-id= -hua-secret= -threads=300 -prefixes= [-region=]
```  
配置文件示例：
```
path=huawei://<bucket>
hua-id=
hua-secret=
threads=300
prefixes=
#region=
```  

### 7 百度云存储
命令行参数示例：
```
-path=baidu://<bucket> -bai-id= -bai-secret= -threads=300 -prefixes= [-region=]
```  
配置文件示例：
```
path=baidu://<bucket>
bai-id=
bai-secret=
threads=300
prefixes=
#region=
```  

### 8 local files

命令行参数示例：
```
-path=<file_path> -parse=json -indexes= [-threads=100]
```  
配置文件示例：
```
#path=<directory_path>
path=<file_path>
#parse=tab
#separator=\t
#indexes=0,1,2
parse=json
indexes=key,etag,fsize
add-keyPrefix=
rm-keyPrefix=
#threads=100
```  