[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.qiniu/qsuits/badge.svg)](https://search.maven.org/artifact/com.qiniu/qsuits/2.20/jar)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

# qiniu-suits (qsuits)
qiniu-suits-java 是一个多线程的云存储 api tools (base-qiniu)，通过设计的[前缀并发算法](docs/datasource.md#并发列举)能够高效**并发列举**
云存储空间的资源列表(支持**七牛云/阿里云/腾讯云/AWS S3/又拍云/华为云/百度云等**，支持 S3 接口的均可以通过 S3 数据源的方式来导出)，同时支持对包含
资源列表的本地数据源并发进行批量处理，处理功能主要包括对七牛云存储资源进行增/删/改/查/转码、以及云存储迁移和公网资源内容审核等，以及本地数据源下大规模
文件上传或者导出文件列表，非常适合大量文件处理和存储空间资源直接管理的场景，同时也支持[交互模式](docs/interactive.md)和[单行模式](docs/single.md)
（直接调用接口处理命令行的一次输入）运行。该 tools 基于 Java8 编写，可基于 jdk8（及以上）环境在命令行或 ide 中运行，命令行运行推荐使用执行器 [qsuits](#命令行执行器-qsuits)）。  

### 功能列表：
- [x] 云存储[资源列举](docs/datasource.md#3-storage-云存储列举)，支持并发、过滤及指定前缀、开始及最大结束文件名或 marker 等参数  
- [x] 文件[迁移/备份](docs/datamigration.md)，针对不同数据源（云存储空间、http 链接列表）向七牛存储空间导入文件  
- [x] 资源文件[过滤](docs/filter.md)，按照日期范围、文件名(前缀、后缀、包含)、mime 类型等字段正向及反向筛选目标文件  
- [x] 检查云存储资源文件后缀名 ext 和 mime-type 类型是否匹配 [check](#特殊特征匹配过滤-f-check)，过滤可疑文件列表  
- [x] 上传文件到存储空间 [qupload 配置](docs/uploadfile.md)  
- [x] 同步 url 内容直传到存储空间 [syncupload 配置](docs/syncupload.md)  
- [x] 删除空间资源 [delete 配置](docs/delete.md)  
- [x] 复制资源到指定空间 [copy 配置](docs/copy.md)  
- [x] 移动资源到指定空间 [move 配置](docs/move.md)  
- [x] 对空间资源进行重命名 [rename 配置](docs/rename.md)  
- [x] 查询空间资源的元信息 [stat 配置](docs/stat.md)  
- [x] 修改空间资源的存储类型（低频/标准/归档）[type 配置](docs/type.md)  
- [x] 对空间的归档文件进行解冻 [restorear 配置](docs/restorear.md)  
- [x] 修改空间资源的状态（启用/禁用）[status 配置](docs/status.md)  
- [x] 修改空间资源的生命周期 [lifecycle 配置](docs/lifecycle.md)  
- [x] 对设置了镜像源的空间资源进行镜像更新或拉取 [mirror 配置](docs/mirror.md)  
- [x] 异步抓取资源到指定空间 [asyncfetch 配置](docs/asyncfetch.md)  
- [x] 同步抓取资源到指定空间 [fetch 配置](docs/fetch.md)  
- [x] 查询资源的 hash & size [qhash 配置](docs/qhash.md)  
- [x] 查询空间资源的视频元信息 [avinfo 配置](docs/avinfo.md)  
- [x] 根据音视频资源的 avinfo 信息来生成转码指令 [pfopcmd 配置](docs/pfopcmd.md)  
- [x] 对空间资源执行 pfop 请求 [pfop 配置](docs/pfop.md)  
- [x] 通过 persistentId 查询 pfop 的结果 [pfopresult 配置](docs/pfopresult.md)  
- [x] 对私有空间资源进行签名，导出私有 url [privateurl 配置](docs/privateurl.md)  
- [x] 对资源生成 url，导出公开 url 或添加参数 [publicurl 配置](docs/publicurl.md)  
- [x] 对 m3u8 的 http 资源进行读取导出其中的 ts 文件列表 [exportts 配置](docs/exportts.md)  
- [x] 通过 http 下载资源到本地 [download 配置](docs/downloadfile.md)  
- [x] 图片类型资源内容审核 [imagecensor 配置](docs/censor.md#图片审核)  
- [x] 视频类型资源内容审核 [videocensor 配置](docs/censor.md#视频审核)  
- [x] 内容审核结果查询 [censorresult 配置](docs/censorresult.md)  
- [x] 修改资源的 mimeType [mime 配置](docs/mime.md)  
- [x] 修改资源的 metadata [metadata 配置](docs/metadata.md)  
- [x] CDN 资源提交刷新、提交预取的操作 [cdn 提交刷新预取操作配置](docs/cdn.md)  
- [x] CDN 查询刷新、查询预取结果的操作 [cdn 查询刷新预取操作配置](docs/cdn.md#CDN-刷新/预取结果查询)  
- [x] 查询七牛存储空间绑定的域名 [domainsofbucket 操作配置](docs/domainsofbucket.md)  

*【部分 process 属于危险操作（如文件删除/禁用等），需要在启动后根据提示输入 y/yes 确认，如果不希望进行 verify 验证则需要在命令行加入 -f 参数】*  

### 支持特性：
- [x] 多数据源账户密钥（加密）设置：[账号设置](#账号设置)  
- [x] 多种模式运行：[程序运行过程](#1-程序运行过程)  
- [x] 中间状态保持：[断点续操作](#10-断点续操作)  
- [x] 手动分布式执行任务：[分布式任务方案](#11-分布式任务方案)  
- [x] 按照时间计划延迟或周期性暂停任务执行：[时间计划](#12-时间计划)  

### 账号设置  
（7.73 及以上版本）支持预先设置好账号的密钥（经过加密），在后续执行中只需使用 account name 即可读取对应账号密钥进行操作，定义不同的 account name 
则可设置多对密钥，亦可设置不同数据源的账号密钥，同一数据源的账号名相同时会覆盖该账号的历史密钥，命令行操作如下所示（配置文件也可以进行账户设置和使用，
去掉命令行参数开头的 `-` 符号且每项参数成一行即可，与后面程序运行方式的配置文件用法相同），密钥参数名参考[各存储数据源配置参数](#storage-云存储列举)。  
#### 1、设置 account：  
命令格式：`-account=<source>-<name> -<source>-id= -<source>-secret= [-d]`，如：  
`-account=test/qiniu-test -ak= -sk=` 设置七牛账号，账号名为 test，没有数据源标识时默认设置七牛账号  
`-account=ten-test -ten-id= -ten-secret=` 设置腾讯云账号，账号名为 test  
`-account=ali-test -ali-id= -ali-secret=` 设置阿里云账号，账号名为 test  
`-account=s3-test -s3-id= -s3-secret=` 设置 AWS/S3 账号，账号名为 test  
`-account=up-test -up-id= -up-secret=` 设置又拍云账号，账号名为 test  
`-account=hua-test -hua-id= -hua-secret=` 设置华为云账号，账号名为 test  
`-account=bai-test -bai-id= -bai-secret=` 设置百度云账号，账号名为 test  
`-d` 表示默认账号选项，此时设置的账号将会成为全局默认账号，执行操作时 -d 选项将调取该默认账号。如果当前已存在多个账号，使用 `-account=<已存在的账号名> -d` 可以修改默认账号。
#### 2、使用 account 账号：  
`-a=test` 表示使用 test 账号，数据源会自动根据 path 参数判断  
`-d` 表示使用默认的账号，数据源会自动根据 path 参数判断  
#### 3、查询 account 账号：
命令格式：`-getaccount=<source>-<name> [-dis] [-d]`，默认只显示 id 的明文而隐藏 secret，`-dis` 参数表示选择明文显示 secret，如：  
`-getaccount -d` 表示查询设置的默认账号的密钥  
`-getaccount=test -dis` 表示查询设置的所有账号名为 test 的密钥，并显示 secret 的明文  
`-getaccount=s3-test` 表示查询设置的 S3 账号名为 test 的密钥  
`-getaccount=ten-test` 表示查询设置的腾讯账号名为 test 的密钥  
`-getaccount=qiniu-test` 表示查询设置的七牛账号名为 test 的密钥  
#### 4、删除 account 账号：  
命令格式：`-delaccount=<source>-<name>`，删除账号只允许一次删除一条，如：  
`-delaccount=s3-test` 表示删除设置的 S3 账号名为 test 的密钥  
`-delaccount=ten-test` 表示删除设置的腾讯账号名为 test 的密钥  
`-delaccount=test/qiniu-test` 表示删除设置的七牛账号名为 test 的密钥  

### 1 程序运行过程  
##### （1）批处理模式：[读取[数据源](docs/datasource.md)] => [选择[过滤器](docs/filter.md)] => [数据[处理过程](#5-处理过程)] => [[结果持久化](docs/resultsave.md)]   
##### （2）交互模式：从命令行输入数据时，process 支持[交互模式](docs/interactive.md)运行，一次启动，可无限次命令行输入 data，输入一次处理一次并返回结果。  
##### （3）单行模式：从命令行输入数据时，process 支持[单行模式](docs/single.md)运行，一次启动，指定 data 参数，直接一次处理并返回结果。  

### 2 运行方式  
提供命令行运行工具 [qsuits](#命令行执行器-qsuits)（或可执行 jar 包）和 maven artifact，使用时建议直接使用或者更新到最新版本。
以下的 x.x.x 表示版本号，最新版本见 [Release](https://github.com/NigelWu95/qiniu-suits-java/releases)  

#### 命令行直接运行 jar 包  
在 [Release](https://github.com/NigelWu95/qiniu-suits-java/releases) 页面下载[最新 jar 包](https://github.com/NigelWu95/qiniu-suits-java/releases/download/v8.0.11/qsuits-8.0.11.jar)
（**maven 仓库中的 \<version\>.jar 包不支持命令行运行，请下载 \<version\>-jar-with-dependencies.jar 包**），使用命令行参数 
[-config=\<filepath\>] 指定配置文件路径，运行命令形如：
```
java -jar qsuits-x.x.x.jar -config=config.txt
```  
配置文件中可设置形如\<属性名\>=\<属性值\>，每行一个参数：  
```
path=qiniu://<bucket>
ak=
sk=
```  
**备注1**：可以通过默认路径的配置文件来设置参数值，默认配置文件路径为 `resources/application.config` 或 `resources/application.properties`，
properties 方式需要遵循 java 的转义规则，两个文件存在任意一个均可作为默认配置文件来设置参数（优先使用 resources/application.properties），
此时则不需要通过 `-config=` 指定配置文件路径，指定 `-config=` 时则默认文件路径无效。  
**备注2**：直接使用命令行传入参数（较繁琐），不使用配置文件的情况下全部所需参数可以完全从命令行指定，形式为：**`-<key>=<value>`**，**请务必在参
数前加上 `-`**，如果参数值中间包含空格，请使用 `-<key>="<value>"` 或者 `-<key>='<value>'` 如  
```
java -jar qsuits-x.x.x.jar -path=qiniu://<bucket> -ak=<ak> -sk=<sk>
```  
**备注3**：7.72 及以下版本中命令行参数与配置文件参数不可同时使用，指定 -config=<path> 或使用默认配置配置文件路径时，需要将所有参数设置在配置文件
中，而在 7.73 开始的版本中命令行参数与配置文件参数可同时使用，参数名相同时命令行参数值会覆盖配置文件参数值，且为默认原则。**【推荐使用配置文件方式，
一是安全性，二是参数历史可保留且修改方便；推荐使用 -account 提前设置好账号，安全性更高，使用时 -a=\<account-name\> 即可，不必再暴露密钥】**  

#### 命令行执行器 qsuits  
由于 qsuits-java 基于 java 编写，命令行运行时需要使用 `java -jar` 命令，为了简化操作运行方式及增加环境和版本管理，提供直接的命令行可执行工具
[qsuits 执行器](https://github.com/NigelWu95/qsuits-exec-go)（使用 golang 编写和编译）来代理 qsuits-java 的功能，支持 qsuits-java
所有参数配置，命令和配置文件用法完全相同，工具下载地址如下：  

|操作系统|程序名|地址|
|---|-----|---|
|windows 32 位|qsuits_windows_386.exe|[下载](https://github.com/NigelWu95/qsuits-exec-go/raw/master/bin/qsuits_windows_386.exe)|
|windows 64 位|qsuits_windows_amd64.exe|[下载](https://github.com/NigelWu95/qsuits-exec-go/raw/master/bin/qsuits_windows_amd64.exe)|
|linux 32 位|qsuits_linux_386|[下载](https://github.com/NigelWu95/qsuits-exec-go/raw/master/bin/qsuits_linux_386)|
|linux 64 位|qsuits_linux_amd64|[下载](https://github.com/NigelWu95/qsuits-exec-go/raw/master/bin/qsuits_linux_amd64)|
|mac 32 位|qsuits_darwin_386|[下载](https://github.com/NigelWu95/qsuits-exec-go/raw/master/bin/qsuits_darwin_386)|
|mac 64 位|qsuits_darwin_amd64|[下载](https://github.com/NigelWu95/qsuits-exec-go/raw/master/bin/qsuits_darwin_amd64)|

下载执行器后可直接以 `qsuits <parameters>` 方式运行，支持所有 
qsuits-java 提供的处理参数，且用法一致。如：
```
qsuits -config=config.txt
```  
或  
```
qsuits -path=qiniu://<bucket> -ak=<ak> -sk=<sk>
```   
**注意**：qsuits 执行器依然依赖 java 环境（8 或以上），但是执行器会去检测本地 java 环境，在无匹配的 java 环境时会提示推荐的安装方法，并且该执行
器可以选择更新到 qsuits.jar 的最新版本再运行，其他选项参考 qsuits-exec-go 的文档：https://github.com/NigelWu95/qsuits-exec-go  

#### 程序依赖 jar  
引入 jar 包（[下载 jar 包](https://search.maven.org/search?q=a:qsuits)或者 [使用 maven 仓库](https://mvnrepository.com/artifact/com.qiniu/qsuits)），
可以重写或新增 processor 接口实现类进行自定义功能，maven:
```
<dependency>
  <groupId>com.qiniu</groupId>
  <artifactId>qsuits</artifactId>
  <version>x.x.x</version>
</dependency>
```  

### 3 数据源
数据源分为两种类型：**云存储列举(storage)**、**本地文件读取(file)**，可以通过 `path=` 来指定数据源地址：  
`path=qiniu://<bucket>` 表示从七牛存储空间列举出资源列表，参考[七牛数据源示例](docs/datasource.md#1-七牛云存储)  
`path=tencent://<bucket>` 表示从腾讯存储空间列举出资源列表，参考[腾讯数据源示例](docs/datasource.md#2-腾讯云存储)  
`path=aliyun://<bucket>` 表示从阿里存储空间列举出资源列表，参考[阿里数据源示例](docs/datasource.md#3-阿里云存储)  
`path=s3://<bucket>` 表示从 aws/s3 存储空间列举出资源列表，参考[S3数据源示例](docs/datasource.md#4-aws-s3)  
`path=upyun://<bucket>` 表示从又拍云存储空间列举出资源列表，参考[又拍数据源示例](docs/datasource.md#5-又拍云存储)  
`path=huawei://<bucket>` 表示从华为云存储空间列举出资源列表，参考[华为数据源示例](docs/datasource.md#6-华为云存储)  
`path=baidu://<bucket>` 表示从百度云存储空间列举出资源列表，参考[百度数据源示例](docs/datasource.md#7-百度云存储)  
`path=<path>` 表示从本地目录（或文件）中读取资源列表，参考[本地文件数据源示例](docs/datasource.md#8-local-files)  
未设置数据源时则默认从七牛空间进行列举，数据源详细参数配置和说明及可能涉及的高级用法见：[数据源配置](docs/datasource.md)，配置文件示例可参考[配置模板](resources/application.config)  

#### storage 云存储列举  
支持从不同的云存储上列举出空间文件，默认线程数(threads 参数)为 50，千万以内文件数量通可以不增加线程，建议阅读[并发列举](docs/datasource.md#并发列举)
参考参数设置优化列举效率，通常云存储空间列举的必须参数包括密钥、空间名（通过 path 或 bucket 设置）及空间所在区域(通过 region 设置，允许不设置的情
况下表明支持自动查询)：  

|storage 源|             密钥和 region 字段         |                  对应关系和描述               |  
|---------|---------------------------------------|---------------------------------------------|  
|qiniu    |`ak=`<br>`sk=`<br>`region=z0/z1/z2/...`|密钥对为七牛云账号的 AccessKey 和 SecretKey<br>region使用简称(可不设置)，参考[七牛 Region](https://developer.qiniu.com/kodo/manual/1671/region-endpoint)|  
|tencent  |`ten-id=`<br>`ten-secret=`<br>`region=ap-beijing/...`| 密钥对为腾讯云账号的 SecretId 和 SecretKey<br>region使用简称(可不设置)，参考[腾讯 Region](https://cloud.tencent.com/document/product/436/6224)|  
|aliyun   |`ali-id=`<br>`ali-secret=`<br>`region=oss-cn-hangzhou/...`| 密钥对为阿里云账号的 AccessKeyId 和 AccessKeySecret<br>region使用简称(可不设置)，参考[阿里 Region](https://help.aliyun.com/document_detail/31837.html)|  
|aws/s3   |`s3-id=`<br>`s3-secret=`<br>`region=ap-east-1/...`| 密钥对为 aws/s3 api 账号的 AccessKeyId 和 SecretKey<br>region使用简称(可不设置)，参考[ AWS Region](https://docs.aws.amazon.com/zh_cn/general/latest/gr/rande.html)|  
|upyun    |`up-id=`<br>`up-secret=`<br>| 密钥对为又拍云存储空间授权的[操作员](https://help.upyun.com/knowledge-base/quick_start/#e6938de4bd9ce59198)和其密码，又拍云存储目前没有 region 概念|  
|huawei   |`hua-id=`<br>`hua-secret=`<br>`region=cn-north-1/...`| 密钥对为华为云账号的 AccessKeyId 和 SecretAccessKey<br>region(可不设置)使用简称，参考[华为 Region](https://support.huaweicloud.com/devg-obs/zh-cn_topic_0105713153.html)|  
|baidu    |`bai-id=`<br>`bai-secret=`<br>`region=bj/gz/su...`| 密钥对为百度云账号的 AccessKeyId 和 SecretAccessKey<br>region(可不设置)使用简称，参考[百度 Region](https://cloud.baidu.com/doc/BOS/s/Ojwvyrpgd#%E7%A1%AE%E8%AE%A4endpoint)|  

#### file 本地文件读取  
本地文件数据源分为**两种情况：（1）读取文件内容为数据列表按行输入（2）读取路径下的文件本身，包括目录遍历，得到文件信息作为输入**  
1、第一种情况，文件内容为资源列表，可按行读取输入文件的内容获取资源列表，文件行解析参数如下：  
`parse=tab/json` 表示输入行的格式  
`separator=\t` 表示输入行的格式分隔符（非 json 时可能需要）  
`add-keyPrefix=` 数据源中每一行的文件名添加前缀  
`rm-keyPrefix=` 数据源中每一行的文件名去除前缀  
`uris=` 设置数据源路径下需要读取的文件名列表，以 `,` 号分割文件名，不设置默认读取 path 下全部文本文件  
`uri-config=` 数据源文件路径及对应文本读取的起始行配置  
**数据源详细参数配置和说明及可能涉及的高级用法见：[数据源配置](docs/datasource.md)**  

2、第二种情况，读取文件本身，用于导出本地的文件列表，也可以进行文件上传，解析参数如下：  
`parse=file` 表示进行文件信息解析格式  
`directories=` 设置数据源路径下需要读取的目录列表，以 `,` 号分割目录名，不设置默认读取 path 下全部目录下的文件  
`directory-config=` 数据源文件目录及对应已上传的文件名配置，配置中记录已上传的文件在 path 中的位置标识  
（1）该数据源导出文件列表时默认只包含 filepath 和 key 信息，如果需要 size、date 等其他信息，请参考 [数据源配置](docs/datasource.md#关于-indexes-索引)。  
（2）用于上传文件的操作时，设置 `process=qupload` 会自动生效，从 `path` 中读取所有文件（除隐藏文件外）执行上传操作，具体配置可参考 [qupload 配置](docs/uploadfile.md)。  

### 4 过滤器功能  
从数据源输入的数据通常可能存在过滤需求，如过滤指定规则的文件名、过滤时间点或者过滤存储类型等，可通过配置选项设置一些过滤条件，目前支持两种过滤条件：
1、**基本字段过滤**和 2、**特殊特征匹配过滤**  
#### 基本字段过滤  
根据设置的字段条件进行筛选，多个条件时需同时满足才保留，若存在记录不包该字段信息时则正向规则下不保留，反正规则下保留，字段包含：  
`f-prefix=` 表示**选择**文件名符合该前缀的文件  
`f-suffix=` 表示**选择**文件名符合该后缀的文件  
`f-inner=` 表示**选择**文件名包含该部分字符的文件  
`f-regex=` 表示**选择**文件名符合该正则表达式的文件，所填内容必须为正则表达式  
`f-mime=` 表示**选择**符合该 mime 类型的文件  
`f-type=` 表示**选择**符合该存储类型的文件，参下述[关于 f-type](#关于-f-type)|  
`f-status=` 表示**选择**符合该存储状态的文件, 为 0（启用） 或 1（禁用）  
`f-date-scale` 设置过滤的时间范围，格式为 [\<date1\>,\<date2\>]，\<date\> 格式为：yyyy-MM-DD HH:MM:SS，如 `f-date-scale="[0,2018-08-01 12:30:00]"` [特殊规则](#f-date-scale)  
`f-anti-prefix=` 表示**排除**文件名符合该前缀的文件  
`f-anti-suffix=` 表示**排除**文件名符合该后缀的文件  
`f-anti-inner=` 表示**排除**文件名包含该部分字符的文件  
`f-anti-regex=` 表示**排除**文件名符合该正则表达式的文件，所填内容必须为正则表达式  
`f-anti-mime=` 表示**排除**该 mime 类型的文件  
`f-strict-error=` true/false，是否使用严格错误模式，默认为 false，为 true 表示对基础字段过滤到不匹配的行抛出异常或记录为 not_match 的结果（filter_not_match_xxx.txt）  

#### 关于 f-type
|存储源|type 参数类型|具体值                   |
|-----|-----------|------------------------|
|七牛  | 整型      |0 表示标准存储；1 表示低频存储；2 表示归档存储|
|其他  | 字符串     |如：Standard 表示标准存储  |  

#### 特殊字符
特殊字符包括: `, \ =` 如有参数值本身包含特殊字符需要进行转义：`\, \\ \=`  

#### f-date-scale
\<date\> 中的 00:00:00 为默认值可省略，无起始时间则可填 [0,\<date2\>]，结束时间支持 now 和 max，分别表示到当前时间为结束或无结束时间。如果使
用命令行来设置，注意日期值包含空格的情况（date 日期和时刻中间含有空格分隔符），故在设置时需要使用引号 `'` 或者 `"`，
如 `-f-date-scale="[0,2018-08-01 12:30:00]"`，配置文件则不需要引号。  

#### 特殊特征匹配过滤 f-check
根据资源的字段关系选择某个特征下的文件，目前支持 `ext-mime` 检查，程序内置的默认特征配置见：[check 默认配置](resources/check.json)，运行
参数选项如下：  
`f-check=ext-mime` 表示进行**后缀名 ext** 和 **mimeType**（即 content-type）匹配性检查，不符合规范的疑似异常文件将被筛选出来  
`f-check-config` 自定义资源字段规范对应关系列表的配置文件，格式为 json，自定义规范配置 key 字段必填，其元素类型为列表 [], 否则无效，如
`ext-mime` 配置时后缀名和 mimeType 用 `:` 组合成字符串成为一组对应关系，写法如下：  
```
{
  "ext-mime": [
    "mp5:video/mp5"
  ]
}
```  
配置举例：[check-config 配置](resources/check-config.json)  
`f-check-rewrite` 是否覆盖默认的特征配置，为 false（默认）表示将自定义的规范对应关系列表和默认的列表进行叠加，否则程序内置的规范对应关系将失效，
只检查自定义的规范列表。  
设置了过滤条件的情况下，后续的处理过程会选择满足过滤条件的记录来进行，或者对于数据源的输入进行过滤后的记录可以直接持久化保存结果，如通过 qiniu 源获
取文件列表过滤后进行保存，可设置 `save-total=true/false` 来选择是否将列举到的完整记录进行保存。  
filter 详细配置可见[filter 配置说明](docs/filter.md)  

### 5 处理过程
处理过程表示对由数据源输入的每一条记录进行处理，所有处理结果保存在 save-path 路径下，具体处理过程由处理类型参数指定，如 **process=type/status
/lifecycle/copy** (命令行方式则指定为 **-process=xxx**) 等，同时 process 操作支持设置公共参数：  
`retry-times=` 操作失败（可重试的异常情况下，如请求超时）需要进行的重试次数，默认为 5 次  
`batch-size=` 支持 batch 操作时设置的一次批量操作的文件个数（支持 batch 操作：type/status/lifecycle/delete/copy/move/rename/stat/cdnrefresh/cdnprefetch，
其他操作请勿设置 batchSize 或者设置为 0），当响应结果较多 429/573 等状态码时可能是超过并发限制需要降低 batch-size，或者直接使用非 batch 方式：batch-size=0/1  
**处理操作类型：**  
`process=qupload` 表示上传文件到存储空间 [qupload 配置](docs/uploadfile.md)  
`process=syncupload` 表示同步 url 内容直传到存储空间 [syncupload 配置](docs/syncupload.md)  
`process=delete` 表示删除空间资源 [delete 配置](docs/delete.md)  
`process=copy` 表示复制资源到指定空间 [copy 配置](docs/copy.md)  
`process=move` 表示移动资源到指定空间 [move 配置](docs/move.md)  
`process=rename` 表示对指定空间的资源进行重命名 [rename 配置](docs/rename.md)  
`process=stat` 表示查询空间资源的元信息 [stat 配置](docs/stat.md)  
`process=type` 表示修改空间资源的存储类型（低频/标准）[type 配置](docs/type.md)  
`process=status` 表示修改空间资源的状态（启用/禁用）[status 配置](docs/status.md)  
`process=lifecycle` 表示修改空间资源的生命周期 [lifecycle 配置](docs/lifecycle.md)  
`process=mirror` 表示对设置了镜像源的空间资源进行镜像更新 [mirror 配置](docs/mirror.md)  
`process=asyncfetch` 表示异步抓取资源到指定空间 [asyncfetch 配置](docs/asyncfetch.md)  
`process=fetch` 表示同步抓取资源到指定空间 [fetch 配置](docs/fetch.md)  
`process=qhash` 表示查询资源的 qhash [qhash 配置](docs/qhash.md)  
`process=avinfo` 表示查询空间资源的视频元信息 [avinfo 配置](docs/avinfo.md)  
`process=pfopcmd` 表示根据音视频资源的 avinfo 信息来生成转码指令 [pfopcmd 配置](docs/pfopcmd.md)  
`process=pfop` 表示对空间资源执行 pfop 请求 [pfop 配置](docs/pfop.md)  
`process=pfopresult` 表示通过 persistentId 查询 pfop 的结果 [pfopresult 配置](docs/pfopresult.md)  
`process=privateurl` 表示对私有空间资源进行私有签名 [privateurl 配置](docs/privateurl.md)  
`process=exportts` 表示对 m3u8 的资源进行读取导出其中的 ts 文件列表 [exportts 配置](docs/exportts.md)  
`process=download` 表示通过 http 下载资源到本地 [download 配置](docs/downloadfile.md)  
`process=imagecensor` 表示图片类型资源内容审核 [imagecensor 配置](docs/censor.md#图片审核)  
`process=videocensor` 表示视频类型资源内容审核 [videocensor 配置](docs/censor.md#视频审核)  
`process=censorresult` 表示内容审核结果查询 [censorresult 配置](docs/censorresult.md)  
`process=mime` 表示修改资源的 mimeType [mime 配置](docs/mime.md)  
`process=metadata` 表示修改资源的 metadata [metadata 配置](docs/metadata.md)  
`process=cdnrefresh/cdnprefetch` 表示 CDN 资源的刷新预取操作 [cdn 操作配置](docs/cdn.md)  

**注意**：  
1、云存储数据源 + process 操作的情况下通常会涉及两对密钥，数据源一对，process 操作一对，如果是 delete、status 等操作则这两对密钥相同，使用一个密
钥设置或者一个 account (`-a=<account-name>`) 即可，copy、move 要求针对同一个账号操作或者采用空间授权，因此也只需要一堆密钥，但如果是其他存储
数据源的数据备份操作 asyncfetch，就需要两对不同的密钥，而 account 只支持设置一个，这时第二对的七牛密钥可以通过同一个 account-name 的设置来获得，
因为同一个 account-name 可以为不同数据源做密钥设置，如：`-account=ali-test -ali-id= -ali-secret=` 设置了阿里云 test 名称的账号，同时
`-account=qiniu-test -ak= -sk=` 设置了七牛 test 名称的账号，则通过 `-a=test` 可以同时拿到阿里云和七牛云的 test 账号，因此可以直接通过同一
个 account-name 来进行操作。但是如果明确指定了另外的 ak，sk，则会使用您设置的这一对七牛密钥。  
2、也真是因为不同数据源的 account-name 可同名特性，以及支持主动设置密钥来覆盖 account 的密钥，在具体操作时需要注意账号和密钥的使用，以免对另外一
个账号执行了操作。  

### 6 结果持久化
对数据源输出（列举）结果进行持久化操作（目前支持写入到本地文件），持久化选项：  
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
|save-format| json/tab/csv/yaml| 结果保存格式，将每一条结果记录格式化为对应格式，默认为 tab 格式（减小输出结果的体积）|  
|save-separator| 字符串| 结果保存为 tab 格式时使用的分隔符，结合 save-format=tab 默认为使用 "\t"|  
|rm-fields| 字符串列表| 保存结果中去除的字段，为输入行中的实际字段选项，用 "," 做分隔，如 key,hash，表明从结果中去除 key 和 hash 字段再进行保存，不填表示所有字段均保留|  

#### 关于 save-total  
（1）用于选择是否直接保存数据源完整输出结果，针对存在过滤条件或下一步处理过程时是否需要保存原始数据，如 bucket 的 list 操作需要在列举出结果之后再针
    对字段进行过滤或者做删除，save-total=true 则表示保存列举出来的完整数据，而过滤的结果会单独保存，如果只需要过滤之后的数据，则设置为 false，如
    果是删除等操作，通常删除结果会直接保存文件名和删除结果，原始数据也不需要保存则设置 save-total=false。  
（2）如果存在 process 或者 filter 则默认设置 save-total=false，反之则 save-total=true（说明可能是单纯列举云存储资源或者本地数据格式转换）。  
（3）保存结果的路径 **默认（save-path）使用 <bucket\>（云存储数据源情况下）名称或者 <path\>-result 来创建目录**。  

#### 关于 save-format  
（1）json 将数据源的信息导出保存为 json 格式  
（2）tab 将数据源的信息导出保存为 table 格式，以 tab 键 `\t` 来分割各项值，顺序按照默认标准字段的顺序  
（3）csv 将数据源的信息导出保存为 table 格式，以 `,` 来分割各项值，顺序按照默认标准字段的顺序  
（4）yaml 将数据源的信息导出保存为类 yaml 格式，目录下的子目录或文件采用比上一级多一个缩进（`\t`）的形式，用于文件列表的层级输出展示  

#### 关于持久化文件名  
（1）持数据源久化结果的文件名为 "<source-name\>\_success_<order\>.txt"，如 qiniu 存储数据源结果为 "qiniu_success_<order\>.txt"，
    local 数据源结果为 "local_success_<order\>.txt"。  
（2）如果设置了过滤选项或者处理过程，则过滤到的结果文件名为 "filter_success/error_<order\>.txt"。
（3）process 过程保存的结果为文件为 "<process\>\_success/error\_<order\>.txt"，<process\>\_success/error\_<order\>.txt 表明无法
    成功处理的结果，<process\>\_need_retry\_<order\>.txt，表明为需要重试的记录，可能需要确认所有错误数据和记录的错误信息。  

#### 关于 rm-fields  
rm-fields 可选择持久化结果中去除某些字段，未设置的情况下保留所有原始字段，数据源导出的每一行信息以目标格式 save-format 保存在 save-path 的文件
中。file 数据源输入字段完全取决于 indexes 和其他的一些 index 设置，可参考 [indexes 索引](docs/datasource.md#关于-indexes-索引)，而其他 index
设置与数据处理类型有关，比如 url-index 来输入 url 信息。对于云储存数据源，不使用 indexes 规定输入字段的话默认是保留所有字段，字段定义可参考[关于文件信息字段](docs/datasource.md#关于文件信息字段)。  

详细配置说明见 [持久化配置](docs/resultsave.md)。  

### 7 网络设置
多数数据源或者操作涉及网络请求，因此提供超时时间和协议设置，默认设置一般能够满足要求，特殊需要的情况下可以修改各超时时间和协议：  
`connect-timeout=60` 网络连接超时时间，程序默认 60s  
`read-timeout=120` socket 读取超时时间，程序默认 120s  
`request-timeout=60` 网络请求超时时间，程序默认 60s  
`config-https=true/false` 对数据源或 process 涉及的公共 api 是否使用 https 来请求，七牛云/华为云数据源或者七牛的 process 均默认使用 https  

### 8 错误及异常
1、一般情况下，终端输出异常信息如 socket timeout 超时为正常现象，如：
```
list prefix:<prefix> retrying...
...
java.net.SocketTimeoutException: timeout
```
程序会自动重试，如果比较频繁则可以修改[超时配置](#7-网络设置)重新运行程序，超过重试次数或者其他非预期异常发生时程序会退出，可以将异常信息反馈在 
[ISSUE列表](https://github.com/NigelWu95/qiniu-suits-java/issues) 中。  
2、常见错误信息：  
（1）java.lang.UnsupportedClassVersionError: Unsupported major.minor version ...  
请使用 java 8 或以上版本的 jdk（jre） 环境来运行该程序。  
（2）java.lang.OutOfMemoryError: GC overhead limit exceeded  
表示可能是内存中加载了过多的资源导致 java 的 gc 内存溢出，需要关闭程序重新运行，更换高配置机器或者降低线程数 threads 或者 unit-len。  
（3）java.lang.OutOfMemoryError: unable to create new native thread   
与（1）类似，内存溢出导致无法继续创建更多线程或对象，降低线程数 threads 重新运行。  
（4）java.lang.OutOfMemoryError: Java heap space   
运行过程中 jvm 堆内存（一般默认为系统内存的 1/4）不足，可以通过 -Xms 和 -Xmx 来增加堆内存，用法: `java -Xms2g -Xmx2g -jar qsuits.jar ...`。  

### 9 程序日志
7.7 版本开始引入了 slf4j+log4j2 来记录运行日志，日志产生在当前路径的 logs 目录下，说明如下：  
1、数据源位置记录信息 =\> procedure.log，记录行格式为 json，数据源读取位置打点数据，每一行都是一次数据源位置记录，最后一行即为最后记录下
的位置信息，如果信息为 `{}` 表明程序运行完整，没有断点需要再次运行，如果信息中包含具体的字符串，说明这是程序留下的断点，则该行信息可以取出作为断点操
作的配置内容，具体参考：[断点操作](#10-断点续操作)  
2、程序运行过程输出及异常信息，通过终端 Console 和 qsuits.info、qsuits.error 输出。  
3、日志输出的默认文件名为 procedure.log、qsuits.info 和 qsuits.error，每次运行前会检查当前路径下是否存在历史日志文件，如果存在则会将文件名加
上数字，如 procedure0.log、qsuits0.info、qsuits0.error 或 procedure1.log、qsuits1.info、qsuits1.error（自动修改日志文件名在 8.0.4
 以上版本支持）。  

### 10 断点续操作
7.1 版本开始支持断点记录，在程序运行后出现异常导致终止或部分数据源路径错误或者是 INT 信号(命令行 Ctrl+C 中断执行)终止程序时，会记录数据导出中断的
位置，记录的信息可用于下次直接从未完成处继续导出数据，而不需要全部重新开始。尤其在对云存储空间列举文件列表时，特大量文件列表导出耗时可能会比较长，可能
存在断点续操作的需求，续操作说明：  
1、如果存在续操作的需要，程序终止时会输出续操作的记录信息路径，如存储空间文件列举操作终止时可能输出：  
`please check the prefixes breakpoint in <filename>.json, it can be used for one more time listing remained files.`  
表示在 \<filename\>.json 文件（json 格式）中记录了断点信息，断点文件位于 save-path 同级路径中，\<filename\> 表示文件名。
2、对于云存储文件列表列举操作记录的断点可以直接作为下次续操作的配置来使用完成后续列举，如断点文件为 \<filename\>.json，则在下次列举时使用断点文件
作为前缀配置文件: prefix-config=<breakpoint_filepath> 即可，参见：[prefix-config 配置](docs/datasource.md#prefix-config-配置)。  
3、对于 file 数据源读取文件列表时，产生的断点文件记录了读取的文本行，可以直接作为下次续操作的配置来使用完成后续列举，如断点文件为 \<filename\>.json，
则在下次继续读 file 数据源操作时使用断点文件作为行配置文件: uri-config=<breakpoint_filepath> 即可，参见：[uri-config 配置](docs/datasource.md#uri-config-配置)。 
4、对于 file 数据源进行上传的情况，断点信息记录的是目录下已经上传到的文件名位置，产生的断点文件亦可以直接作为下次续操作的配置来使用完成后续上传，如
断点文件为 \<filename\>.json，则在下次继续上传该 path 目录的文件时使用断点文件作为行配置文件: directory-config=<breakpoint_filepath> 
即可（注意是 directory-config），参见：[directory-config 配置](docs/uploadfile.md#directory-config-配置)。  
5、断点续操作时建议修改下 save-path，便于和上一次保存的结果做区分（7.72 及以下版本中断点参数请和其他参数保持一致放在命令行或配置文件中，7.72 以上
版本无此限制，只要提供断点参数无论是否与其他参数同在命令行或配置文件中均可生效）。  

**注意：  
（1）如果是系统宕机、断电或者强制关机或者进程强行 kill 等情况，无法得到输出的断点文件提示，因此只能通过[<位置记录日志>](#9-程序日志)来查看最后
的断点信息，在 8.2.1 版本以上设置了 log 参数可用于启用日志记录的断点，即取出运行路径下 logs 目录中的 procedure[x].log 日志，将该日志文件设置
为 `-log=<procedure[x].log's path>` 再运行可完成断点续操作。  
（2）如果原任务包含 process 过程（只有数据源读取不包含 process 操作可以不考虑该问题），执行断点操作时，由于断点日志粒度按照 unit-len 来记录的，
当 unit-len 比较大（默认一般都是 1000 以上，甚至是 10000）时，可能存在记录的断点比实际 process 的进度要滞后很多条记录，因此对于一些不希望存在数
据重复执行的 process，如 qupload/syncupload/mirror/fetch/asyncfetch 等，数据重复执行会影响效率和增加流量消耗，那么建议操作断点时设置一些
check 参数，如 check=stat（参考：[过滤和检查](docs/datamigration.md#过滤和检查)），或者对于其他操作有这个需求的话可以自行对 process 的
[结果输出文件](#关于持久化文件名)进行检查，查看执行到的数据行位置并对断点设置的 json 文件进行调整。**  

### 11 分布式任务方案
对于不同账号或空间可以直接在不同的机器上执行任务，对于单个空间资源数量太大无法在合适条件下使用单台机器完成作业时，可分机器进行作业，如对一个空间列举完
整文件列表时，可以按照连续的前缀字符分割成多段分别执行各个机器的任务，建议的前缀列表为:  
```!"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\]^_`abcdefghijklmnopqrstuvwxyz```，将该列表任意分成 n 段，如：
```
prefixes=!,",#,$,%,&,',(,),*,+,\,,-,.,/,0,1
prefixes=2,3,4,5,6,7,8,9,:,;
prefixes=<,=,>,?,@,A,B,C,D,E,F,G,H,I,J,K,L,M,N,O
prefixes=P,Q,R,S,T,U,V,W,X,Y,Z,[,\\,],^,_,`
prefixes=a,b,c,d,e,f,g,h,i,j,k,l,m
prefixes=n,o,p,q,r,s,t,u,v,w,x,y,z
```
（**`,`，`\` 需要转义**）将前缀分为上述几段后，设置 prefixes 参数可以分做六台机器执行，同时因为需要列举空间全部文件，需要分别在第一段 prefixes
设置 `prefix-left=true`，在最后一段 prefixes 设置 `prefix-right=true`（其他段 prefixes 不能同时设置 prefix-left 或 prefix-right，
且仅能第一段设置 prefix-left 和最后一段设置 prefix-right，参数描述见[数据源完备性](docs/datasource.md#数据源完备性和多前缀列举)  

### 12 时间计划
在 8.0.0 及以上版本支持时间计划参数，控制任务按照时间调度来执行，包含延迟执行和周期性（周期为 1 天）暂停策略，参数如下（不设置则无时间计划）：  
```
start-time=2019-10-07 08:00:00
pause-delay=36000
pause-duration=50400
```  
|参数           |含义                    |  
|--------------|-----------------------|  
|start-time    |任务正式开始执行的时间点，时间格式为：yyyy-MM-DD HH:MM:SS，HH:MM:SS 为 00:00:00（缺省值）时可省略|  
|pause-delay   |任务开始执行后经过多少秒（默认 0s）进行第一次暂停，单位 s(秒)，之后每天的该时间点执行暂停|  
|pause-duration|任务每天的暂停持续时间，单位 s(秒)，每次暂停经过该段时间后恢复任务的执行|  

**注意**：如果命令行使用该参数的话 `start-time` 的值请加上引号，如 `-start-time="2019-10-07 08:00:00"`。  

比如上述参数表示在 2019-10-07 08:00:00 开始运行，运行 10 小时后在 2019-10-07 18:00:00 对任务进行暂停，暂停 14 个小时，第二天的 08:00:00
恢复运行，因此该配置的含义表示任务在每天的 08:00:00-18:00:00 期间运行（18:00:00-第二天08:00:00 期间暂停）。start-time 不允许超出到一周之后，
pause-duration 的最小暂停时间为 1800s（0.5 小时）最大暂停时间为 84600s（23.5 小时）。pause-delay 的默认值为 0，小于 0 时表示不执行时间计划，
或者 pause-duration 小于 0 时同样表示不执行时间计划。  

### 13 暂停和恢复
暂停和恢复是操作系统特性，如果对系统熟悉也可以基于此来制作时间计划。Linux/Mac 下支持以下操作来暂停和恢复进程：  
暂停（Ctrl + Z 命令）：  
```shell
^Z
[1]  + 38835 suspended  java -jar qsuits.jar
```  
恢复（fg 命令，注意该命令需要在暂停时的同个 terminal 下执行，同时建议不要做路径切换）：  
```shell
[1]  + 38835 continued  java -jar qsuits.jar
```  
