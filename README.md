[![Software License](https://img.shields.io/badge/license-MIT-brightgreen.svg)](LICENSE)

# qiniu-suits (qsuits)
七牛云接口使用套件（可以工具形式使用），主要针对七牛云存储资源的批量处理进行功能的封装，提供更为简洁的操作方式。
基于 Java 编写，可基于 JDK（1.8 及以上）环境在命令行或 IDE 运行。  

# 使用介绍
### 1 程序运行过程  
读取数据源 =》[过滤器 =》] [按指定过程处理数据 =》] 结果持久化  

### 2 运行方式  
(1) 命令行: java -jar qsuits-<x.x>.jar [-config=<config-filepath>]  
(2) Java 工程中，引入 jar 包，可以自定义 processor 接口实现类或者重写实现类来实现自定义功能  
*所有参数设置，无论命令行或胖配置文件方式均无需加引号*

### 3 命令行运行配置
(1) 自定义配置文件路径，使用命令行参数 `-config=<config-filepath>` 指定配置文件路径，命令为：  
```
java -jar qsuits-x.x.jar -config=config.txt
```
配置文件中参数可设置形如：  
```
source-type=list (v.2.11 及以上版本也可以使用 source=file，或者不设置该参数)
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

(3) 直接使用命令行传入参数（较繁琐），不使用配置文件的情况下所有参数可以完全从命令行指定，形式为 `-<property-name>=<value>`，如  
```
java -jar qsuits-x.x.jar [-source=list] -ak=<ak> -sk=<sk> -bucket=<bucket>
```
*在 v2.11 及以上版本，source 效果与 source-type 相同，也可以不设置该输入参数，由程序自动判断*  

### 4 数据源
**在 v2.11 及以上版本，取消了设置该参数的强制性，可以进行指定，或者使用简化参数名 source=<source>**
支持从不同数据源读取到数据进行后续处理, 通过 **source-type** 来指定数据源方式:  
**source-type=list/file** (命令行方式则指定为 **-source-type=list/file**)  
`source-type=list` 表示从七牛存储空间列举出资源 [listbucket 配置](docs/listbucket.md)，配置文件示例可参考 [配置模板](templates/list.config)  
`source-type=file` 表示从本地读取文件获取资源列表 [fileinput 配置](docs/fileinput.md)，配置文件示例可参考 [配置模板](templates/file.config)  

##### *关于并发处理*：  
```
(1) list 源，从存储空间中列举文件，可多线程并发列举，用于支持大量文件的列举加速，线程数在配置文件中指定，自动按照线程数并发，
    文件数目较少时不建议使用较多线程，否则反而会增加耗时，如 100 万左右及以下的文件建议线程数少于 50，甚至更少文件时可使用单
    线程直接列举（设置 threads=1）   
(2) file 源，从本地读取目录下的所有文件，一个文件进入一个线程处理，最大线程数由配置文件指定，与输入文件数之间小的值作为并发数  
```

#### 1. 过滤器功能
从数据源输入的数据（针对七牛空间资源）通常可能存在过滤需求，如过滤指定规则的文件名、过滤时间点或者过滤存储类型
等，qsuits 支持通过配置选项设置一些过滤条件，目前支持的过滤条件包含：  
`f-prefix` 表示**选择**文件名符合该前缀的文件  
`f-suffix` 表示**选择**文件名符合该后缀的文件  
`f-inner` 表示**选择**文件名包含该部分字符的文件  
`f-regex` 表示**选择**文件名符合该正则表达式的文件，所填内容必须为正则表达式  
`f-mime` 表示**选择**符合该 mime 类型的文件  
`f-type` 表示**选择**符合该存储类型的文件, 为 0（） 或 1  
`f-status` 表示**选择**符合该存储状态的文件, 为 0 或 1  
`f-date, f-time` 设置过滤的时间节点  
`f-direction` 表示时间节点过滤方向，0 表示选择**时间点以前**更新的文件，1 表示选择**时间点以后**更新的文件  
`f-anti-prefix` 表示**排除**文件名符合该前缀的文件  
`f-anti-suffix` 表示**排除**文件名符合该后缀的文件  
`f-anti-inner` 表示**排除**文件名包含该部分字符的文件  
`f-anti-regex` 表示**排除**文件名符合该正则表达式的文件，所填内容必须为正则表达式  
`f-anti-mime` 表示**排除**该 mime 类型的文件  
[filter 配置说明](docs/filter.md) 设置了过滤条件的情况下，后续的处理过程会选择满足过滤条件的记录来进行，或者对于数据源的输入进行过滤后
的记录可以直接持久化保存结果，如对于 listbucket/fileinput 的结果过滤后进行保存，此时可通过 save-total 选项来选择是否将过
滤之前的记录进行完整保存。

#### 2. 输出结果持久化
对数据源输出（列举）结果进行持久化操作（目前支持写入到本地文件），持久化选项：  
`result-path=` 表示保存结果的文件路径  
`result-format=` 结果保存格式（json/table）  
`result-separator=` 结果保存分隔符  
所有持久化参数均为可选参数，未设置的情况下保留所有字段，以 json 格式保存在 ../result 路径下，详细参数见 [result 配置](docs/filesave.md)。

### 5 处理过程
处理过程表示对由数据源输入的每一条记录进行处理，具体处理过程由处理类型参数指定:  
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
rename、qhash、stat、pfop、pfopresult、avinfo 一般为对 file 输入方式进行处理，所有处理结果保存在 result-path 下。  

### 补充
1. 命令行方式与配置文件方式不可同时使用，指定 -config=<path> 或使用 qiniu.properties 时，需要将所有参数设置在该配置文件中。
2. 一般情况下，命令行输出异常信息如 socket time 超时为正常现象，程序会自动重试，如：
```
list prefix:<prefix>\tlast 3/2/1 times retrying...
java.net.SocketTimeoutException: timeout
```
超过重试次数或者其他非预期异常发生时程序会退出，可以将异常信息反馈在 
[ISSUE列表](https://github.com/NigelWu95/qiniu-suits-java/issues) 中。
