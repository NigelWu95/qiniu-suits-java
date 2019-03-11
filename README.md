[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.qiniu/qsuits/badge.svg)](https://search.maven.org/artifact/com.qiniu/qsuits/2.20/jar)
[![License](https://img.shields.io/badge/license-Apache%202-4EB1BA.svg)](https://www.apache.org/licenses/LICENSE-2.0.html)

# qiniu-suits (qsuits)
七牛云接口使用套件（可以工具形式使用），主要针对七牛云存储资源的批量处理进行功能的封装，提供更为简洁的操作方式。
基于 Java 编写，可基于 JDK（1.8 及以上）环境在命令行或 IDE 运行。  
#### **高级功能列表：**
- [x] 七牛空间文件并发列举，自定义多前缀、列举开始和结束位置、线程数、结果格式和所需字段等  
- [x] 七牛空间文件按照字段过滤，按照日期、文件名、mime 类型等字段筛选目标文件  
- [x] 批量进行七牛存储文件的 API 操作，批量修改或查询文件、异步抓取文件、转码及查询转码结果等等  

## 使用介绍
### 1 程序运行过程  
读取数据源 =》[过滤器 =》] [按指定过程处理数据 =》] 结果持久化  

### 2 运行方式  
(1) 命令行: java -jar qsuits-<x.x>.jar [-config=<config-filepath>]  
(2) Java 工程中，引入 jar 包，可以自定义 processor 接口实现类或者重写实现类来实现自定义功能  
jar 包下载地址：https://search.maven.org/search?q=a:qsuits
maven 引入:
```
<dependency>
  <groupId>com.qiniu</groupId>
  <artifactId>qsuits</artifactId>
  <version>2.20</version>
</dependency>
```   


### 3 命令行运行配置
(1) 自定义配置文件路径，使用命令行参数 `-config=<config-filepath>` 指定配置文件路径，命令为：  
```
java -jar qsuits-x.x.jar -config=config.txt
```
配置文件中参数可设置形如：  
```
source-type=list (v.2.11 及以上版本也可以使用 source=list，或者不设置该参数)
ak=
sk=
bucket=
```
*source-type=list 可选择放置在命令行或者配置文件中*  

(2) 可以通过默认路径的配置文件来设置参数值，默认的配置文件需要放置在与 jar 包同路径下的 
resources 文件夹中，文件名为 `qiniu.properties` 或 `.qiniu.properties`，运行命令为：  
```
java -jar qsuits-x.x.jar [-source-type=list]
```
*配置参数同上述方式*  

(3) 直接使用命令行传入参数（较繁琐），不使用配置文件的情况下所有参数可以完全从命令行指定，形式为：  
 **`-<property-name>=<value>`**，如  
```
java -jar qsuits-x.x.jar [-source=list] -ak=<ak> -sk=<sk> -path=<path>
```
*在 v2.11 及以上版本，source 效果与 source-type 相同，也可以不设置该输入参数，由程序自动判断*  

### 4 数据源
【说明】**在 v2.11 及以上版本，取消了设置该参数的强制性，可以进行指定，或者使用简化参数名 source=<source>**  
支持从不同数据源读取到数据进行后续处理, 通过 **source-type** 来指定数据源方式:  
**source-type=list/file** (命令行方式则指定为 **-source-type=list/file**)  
`source-type=list` 表示从七牛存储空间列举出资源列表，配置文件示例可参考 [配置模板](templates/list.config)  
`source-type=file` 表示从本地文件按行读取资源列表，配置文件示例可参考 [配置模板](templates/file.config)  
##### 常用参数
```
source-type=list/file (v.2.11 及以上版本也可以使用 source=list/file，或者不设置该参数)
path=
threads=30
unit-len=10000
```  
##### list 参数：
```
ak=
sk=
bucket=
```
##### file 参数：
```
parse=
separator=
indexes=0,1,2
```  
#### 获取数据源中的资源列表更多的参数配置和详细解释及可能涉及的高级用法见：[数据源配置](docs/datasource.md)

##### *关于并发处理*：  
```
(1) list 源，从存储空间中列举文件，可多线程并发列举，用于支持大量文件的加速列举，线程数在配置文件中指定，自动按照线程数检索前缀并执行并发列举。  
(2) file 源，从本地读取目录下的所有文件，一个文件进入一个线程处理，最大线程数由配置文件指定，与输入文件数之间小的值作为并发数。    
```
**并发处理效果依赖机器性能，由于处理时多线程会同时读取大量的数据列表在内存中（默认的单个列表 size 是 10000，用 unit-len 参数设置），因此会占用较
大的内存，线程数过高时可能内存溢出，故程序默认线程数为 30。可以参考机器性能适当提高这两项参数，32CPU96G 的机器甚至可以达到 600 线程，通常可以设置
1-3百个线程，8CPU32G 的机器最好不要超过200线程。unit-len 一般不需要调整，但可视情况而定，如果增加 unit-len 的话，建议设置的线程数参考可能的最
大线程数相应减小，例如设置 unit-len=20000 的话建议线程数参考最大值减半。**  

#### 1. 过滤器功能
从数据源输入的数据（针对七牛空间资源）通常可能存在过滤需求，如过滤指定规则的文件名、过滤时间点或者过滤存储类型
等，qsuits 支持通过配置选项设置一些过滤条件，目前支持的过滤条件包含：  
`f-prefix` 表示**选择**文件名符合该前缀的文件  
`f-suffix` 表示**选择**文件名符合该后缀的文件  
`f-inner` 表示**选择**文件名包含该部分字符的文件  
`f-regex` 表示**选择**文件名符合该正则表达式的文件，所填内容必须为正则表达式  
`f-mime` 表示**选择**符合该 mime 类型的文件  
`f-type` 表示**选择**符合该存储类型的文件, 为 0（标准存储） 或 1（低频存储）  
`f-status` 表示**选择**符合该存储状态的文件, 为 0（启用） 或 1（禁用）  
`f-date-scale` 设置过滤的时间范围，格式为 [\<date1\>,\<date2\>]，\<date\> 格式为："2018-08-01 00:00:00"  
`f-anti-prefix` 表示**排除**文件名符合该前缀的文件  
`f-anti-suffix` 表示**排除**文件名符合该后缀的文件  
`f-anti-inner` 表示**排除**文件名包含该部分字符的文件  
`f-anti-regex` 表示**排除**文件名符合该正则表达式的文件，所填内容必须为正则表达式  
`f-anti-mime` 表示**排除**该 mime 类型的文件  
`f-check` 是否进行**后缀名**和**mimeType**（即 content-type）匹配性检查，不符合规范的疑似异常文件将被筛选出来|  
`f-check-config` 自定义资源字段规范对应关系列表的配置文件，格式为 json，配置举例：[check-config 配置](../resources/check-config.json)|  
`f-check-rewrite` 是否完全使用自定义的规范列表进行检查，默认为 false，程序包含的默认字段规范对应关系配置见：[check 默认配置](../resources/check.json)|  
[filter 配置说明](docs/filter.md) 设置了过滤条件的情况下，后续的处理过程会选择满足过滤条件的记录来进行，或者对于数据源的输入进行过滤后的记录可
以直接持久化保存结果，如通过 list/file 源获取列表并过滤后进行保存，并且可设置 save-total=true/false 来选择是否将过滤之前的记录进行完整保存。  

#### 2. 输出结果持久化
对数据源输出（列举）结果进行持久化操作（目前支持写入到本地文件），持久化选项：  
`save-path=` 表示保存结果的文件路径  
`save-format=` 结果保存格式（json/tab），默认为 tab  
`save-separator=` 结果保存分隔符，结合 save-format=tab 默认使用 "\t" 分隔  
`save-total=` 是否保存数据源的完整输出结果，用于在设置过滤器的情况下选择是否保留原始数据。如 list bucket 操作需要在列举出结果之后再针对条件进行
过滤，save-total=true 则表示保存列举出来的完整数据，而过滤的结果会单独保存，如果只需要过滤之后的数据，则设置 save-total=false。file 源时默认
不保存原始输出数据，list 源默认保存原始输出数据。   
**--** 所有持久化参数均为可选参数，未设置的情况下保留所有字段：key、hash、fsize、putTime、mimeType、type、status、endUser，可选择去除某些
字段，每一行信息以 json 格式保存在 ./result 路径（当前路径下新建 result 文件夹）下。详细参数见 [持久化配置](docs/resultsave.md)。  
**持久化结果的文件名为 "\<source-name\>_success_\<order\>.txt"：  
（1）list 源 =》 "listbucket_success_\<order\>.txt"  
（2）file 源 =》 "fileinput_success_\<order\>.txt"  
如果设置了过滤参数，则过滤到的结果文件名为 "filter_success_\<order\>.txt"**  

### 5 处理过程
处理过程表示对由数据源输入的每一条记录进行处理，所有处理结果保存在 save-path 路径下，具体处理过程由处理类型参数指定:  
**process=type/status/lifecycle/copy** (命令行方式则指定为 **-process=xxx**) 等  
`process=type` 表示修改空间资源的存储类型（低频/标准）[type 配置](docs/type.md)  
`process=status` 表示修改空间资源的状态（启用/禁用）[status 配置](docs/status.md)  
`process=lifecycle` 表示修改空间资源的生命周期 [lifecycle 配置](docs/lifecycle.md)  
`process=delete` 表示删除空间资源 [delete 配置](docs/delete.md)  
`process=copy` 表示复制资源到指定空间 [copy 配置](docs/copy.md)  
`process=move` 表示移动资源到指定空间 [move 配置](docs/move.md)  
`process=rename` 表示对指定空间的资源进行重命名 [rename 配置](docs/rename.md)  
`process=asyncfetch` 表示异步抓取资源到指定空间 [asyncfetch 配置](docs/asyncfetch.md)  
`process=pfop` 表示对空间资源执行 pfop 请求 [pfop 配置](docs/pfop.md)  
`process=pfopresult` 表示通过 persistentId 查询 pfop 的结果 [pfopresult 配置](docs/pfopresult.md)  
`process=stat` 表示查询空间资源的元信息 [stat 配置](docs/stat.md)  
`process=avinfo` 表示查询空间资源的视频元信息 [avinfo 配置](docs/avinfo.md)  
`process=qhash` 表示查询资源的 qhash [qhash 配置](docs/qhash.md)  
`process=privateurl` 表示对私有空间资源进行私有签名 [privateurl 配置](docs/privateurl.md)  
`process=pfopcmd` 表示根据音视频资源的 avinfo 信息来生成转码指令 [pfopcmd 配置](docs/pfopcmd.md)  

### 补充
1. 命令行方式与配置文件方式不可同时使用，指定 -config=<path> 或使用 qiniu.properties 时，需要将所有参数设置在该配置文件中。
2. 一般情况下，命令行输出异常信息如 socket time 超时为正常现象，程序会自动重试，如：
```
list prefix:<prefix> retrying...
...
java.net.SocketTimeoutException: timeout
```
超过重试次数或者其他非预期异常发生时程序会退出，可以将异常信息反馈在 
[ISSUE列表](https://github.com/NigelWu95/qiniu-suits-java/issues) 中。
3. 常见错误信息：  
（1）java.lang.OutOfMemoryError: GC overhead limit exceeded  
表示内存中加载了过多的资源导致 java 的 gc 内存溢出，需要关闭程序重新运行，降低线程数 threads 或者 unit-len。  
（2）java.lang.OutOfMemoryError: unable to create new native thread   
与（1）类似，内存溢出导致无法继续创建更多线程。  
（3）java.lang.UnsupportedClassVersionError: Unsupported major.minor version ...  
请使用 java 8 或以上版本的 jdk（jre） 环境来运行该程序。  