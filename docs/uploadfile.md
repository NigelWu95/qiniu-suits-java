# 文件上传

## 简介
将本地的文件（批量）上传至存储空间。可参考：[上传策略](https://developer.qiniu.com/kodo/manual/1206/put-policy) 和 [上传资源](https://developer.qiniu.com/kodo/manual/1234/upload-types)  
1. **操作需要指定数据源，上传本地文件到七牛空间，故需要配置本地数据源，参考：[配置数据源](datasource.md#2.2-文件信息读取)**  
2. 支持通过 `-a=<account-name>`/`-d` 使用已设置的账号，则不需要再直接设置密钥，参考：[账号设置](../README.md#账号设置)  
2. 单次上传一个文件请参考[ single 操作](single.md)  
3. 交互式操作随时输入filepath 进行上传请参考[ interactive 操作](interactive.md)  

## 配置
> config.txt
```
path=
process=qupload
path=
directories=
directory-config=
anti-prefixes=
ak=
sk=
bucket=
filepath-index=
parent-path=
record=
keep-dir=
keep-path=
add-prefix=
rm-prefix=
expires=
policy.[]=
params.[]=
crc=
threshold=
check=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process|上传资源时设置为 qupload | 表示上传资源操作|  
|path| 本地路径| path 是数据源选项，可以通过设置本地路径来指定要上传的文件，为目录时会遍历目录下（包括内层目录）除隐藏文件外的所有文件|  
|directories| 文件目录列表| 如果希望上传 path 下的几个目录中文件，可设置 path 路径下需要读取的目录列表，以 `,` 号分割目录名，不设置默认读取 path 下全部目录进行文件的上传|  
|directory-config| 配置文件路径|数据源文件目录及对应已上传的文件名配置，配置中记录已上传的文件在 path 中的位置标识，配置格式为 json，参考[ directory-config 配置文件](#directory-config-配置)，该配置不需要自行编写|  
|anti-prefixes| 文件目录列表| 表示上传目录下文件时排除某些名称前缀的子目录，支持以 `,` 分隔的列表，特殊字符同样需要转义符|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取|  
|bucket| 字符串| 上传到的资源原空间名称|  
|parent-path|上级目录| 该参数通常和 filepath-index 同时使用，用于规定文本中的路径值拼接上层目录得到要上传的文件路径，通过 path 自动读取文件的情况下不需要设置该参数|  
|filepath-index| 文件路径索引| 非必填字端，当直接上传 path 路径中的文件时无需设置，如果是通过读取文本文件每一行中的路径信息则需要设置，未设置任何索引和 parent-path 时根据 parse 类型默认为 0 或 "filepath"|  
|record| true/false| 对于大于 4M 的文件会自动使用分片上传，该参数用于规定分片上传是否记录上传进度信息（断点续传作用），默认不开启|  
|keep-path| true/false| 上传到空间的文件名（资源 key）是否保存从 path 开始的完整路径，默认为 true，则使用文件完整路径作为空间的资源 key|  
|keep-dir| true/false| 是否维持目录结构而针对目录产生一条文件名以 / 结尾的文件记录，即使目录为空也会创建该目录，默认为 false|  
|add-prefix| 字符串| 表示为保存的文件名添加指定前缀|  
|rm-prefix| 字符串| 表示将得到的目标文件名去除存在的指定前缀后再作为保存的文件名|  
|expires| 整型数字| 单个文件上传操作的鉴权有效期，单位 s(秒)，默认为 3600|  
|policy.[]| 字符串/整型数字| 可以设置一些上传策略参数，如 policy.deleteAfterDays=7 表示七天之后自动删除文件，其他参数可参考[七牛上传策略](https://developer.qiniu.com/kodo/manual/1206/put-policy)|  
|params.[]| 字符串| 上传时设置的一些变量参数，如 params.x:user=138300 表示 x:user 的信息为 138300，可参考[七牛上传自定义变量](https://developer.qiniu.com/kodo/manual/1235/vars#xvar)|  
|crc| true/false| 是否开启 crc32 来校验文件的上传，默认为 false|  
|threshold| 整型数字| 设置文件分片上传的阈值，超过该阈值时才启用分片上传，小于该阈值的文件都是表单上传，默认阈值是 4M，对于内网上传时可以设置较大的阈值提高上传效率|  
|check|字符串| 进行文件存在性检查，目前可设置为 `stat`，表示通过 stat 接口检查目标文件名是否存在，如果存在则不进行 fetch，而记录为 `file exsits`|  

运行参数：`-config=config.txt`

### 关于 directory-config
directory-config 用来设置要读取的文件目录及位置信息，在 path 为空的情况下，directory-config 中的文件名必须是完整的目录路径，path 为目录时，
directory-config 中的目录名可以采取相对 path 路径下的目录名。配置中每一个目录对应的值表示在一个文件名信息，在实际读取数据源过程中，会参照该文件
名，从之后的文件开始读取，即此文件名信息标示目录中的读取位置，可以用于设置断点。directory-config 需要的断点文件基于上次上传文件操作未完成的情况下
可以产生，理论上只要上传的目录结构没有发生变化，断点文件就能针对上次未完成上传的文件进行上传，而不会所有文件重新上传一遍，断点信息的获取参考[断点续操作](../README.md#10-断点续操作)。  

#### directory-config 配置
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

### 上传超时时间
timeout 参数可以通过全局的 timeout 来设置，参考：[网络设置](../README.md#7-网络设置)  

### 关于上传的文件路径
上传文件的操作可以直接上传通过 path 指定的目录下的文件，也支持传通过文件列表中提供的 filepath 的文件，如：  
```
# 直接上传，如果 test 为一个文件，则直接上传，如果为目录，则读取该目录下所有文件进行上传
path=test

# 上传文件列表中指定的 filepath 文件，从 filelist.txt 中通过 parse,separator,filepath-index 等参数来解析每一行，得到文件路径等信息组织上
# 传操作，当然 path 也可以指定目录，会通过读取文件列表的方式解析目录下所有 text 文件中的每一行进行一次上传操作
path=filelist.txt
parse=tab
separator=\t
filepath-index=0
indexes=1
```  
通过 path 指定的目录直接上传文件时，会自动产生文件 etag,size,timestamp,mimeType 信息，因此也支持通过[过滤器](filter.md) 设置过滤多个条件，
通过筛选的文件才进行上传。

### 关于上传空间保存的文件名
如果是通过 path 下直接上传的文件，文件名采用自动解析，如果通过文件列表和 filepath-index 解析进行上传的文件，文件名可以自动确定也可以根据 key 
(key 下标由 indexes 的第一个参数确定) 下标解析。如：  
```
# 自动获取文件名：文件名为 test 或者 test/ 目录加上目录下的文件路径，比如 test 目录下存在文件 1.txt，则上传文件名默认为 test/1.txt，如果设置
# keep-path=false 则文件名去除父层路径，最终文件名为 1.txt
path=test
keep-path=false
# add-prefix 和 rm-prefix 可以对文件名做进一步自定义处理
# add-prefix=
# rm-prefix=

# 上传文件列表中指定的 filepath 文件，如下所示表示文本中的每一行（以 \t 分割）的第一个字段为 filepath，那么上传 filepath 这个文件的文件名则为
# 第二个字段确定的 key，同时可以使用 add-prefix 和 rm-prefix 对文件名做进一步自定义处理
parse=tab
separator=\t
filepath-index=0
indexes=1
```  
**注意**：七牛存储空间不支持文件名以 `../`, `./` 开头或者包含 `/../`, `/./` 这种情况，会造成无法访问，因此设置文件名时请注意，如果 path 是以
`../` 或 `./` 开头的路径进行直接上传时文件名会解析时会去掉该开头。filepath 值也尽量不要携带这些部分，因为在 keep-path=true 时路径名即作为文件名。   

### 关于 filepath-index
当使用 file 源且 parse=tab/csv 时 [xx-]index(ex) 设置的下标必须为整数。filepath-index 表示输入行含 filepath 形式的文件路径，未设置的情
况下则使用 key 字段加上 parent-path 的方式访问文件路径，key 下标用 indexes 参数设置，默认会根据 parse 类型设置为 0 或 "key"，参见[ indexes 索引](datasource.md#关于-indexes-索引)
及[关于 parse 和索引](datasource.md#关于-parse)。  

### 命令行参数方式
```
-path= -process=qupload -ak= -sk= -bucket= -path= ...
```

