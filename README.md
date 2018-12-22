# qiniu-suits (qsuits)
七牛云接口使用套件（可以工具形式使用），主要针对七牛云存储资源的批量处理进行功能的封装，提供更为简洁的操作方式。  
基于 Java 编写，可基于 JDK（1.8 及以上）环境在命令行或 IDE 运行。  

# 使用介绍
#### 1. 程序运行过程为：读取数据源 =》[按指定过程处理数据 =》] 结果持久化  
#### 2. 运行配置
(1) 自定义配置文件路径，使用命令行参数 `-config=<config-filepath>` 指定配置文件路径  
(2) 可以通过默认路径的配置文件来设置参数值，默认的配置文件需要放置在与 jar 包同路径下的 
resources 文件夹中，文件名为 `qiniu.properties` 或 .qiniu.properties  
(3) 直接使用命令行传入参数（较繁琐），不使用配置文件的情况下可以完全从命令行指定参数，形式为 `-<property-name>=<value>`  
#### 3. 运行方式  
(1) 命令行: java -jar qsuits-<x.x>.jar [-config=<config-filepath>]  
(2) Java 工程中，引入 jar 包，可以自定义 processor 接口实现类或者重写实现类来实现自定义功能  

### 1 数据源
支持从不同数据源读取到数据进行后续处理, 通过 **source-type** 来指定数据源方式:  
**source-type=list/file** (命令行方式则指定为 **-source-type=list/file**)  
`source-type=list` 表示从七牛存储空间列举出资源 [listbucket 配置](docs/listbucket.md)，list 方式 [配置模板](templates/list.config)  
`source-type=file` 表示从本地读取文件获取资源列表 [fileinput 配置](docs/fileinput.md)，file 方式 [配置模板](templates/file.config)  

###### *关于并发处理*：  
```
(1) list 源，从存储空间中列举文件，可多线程并发列举，用于支持大量文件的列举加速，线程数在配置文件中指定，自动按照线程数并发，
    少量文件时不建议使用并发方式，反而会增加耗时，如 300 万左右及以下的文件可使用直接列举（multi-false） 
(2) file 源，从本地读取目录下的所有文件，一个文件进入一个线程处理，最大线程数由配置文件指定，与输入文件数之间小的值作为并发数  
```

### 2 处理过程
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
rename、qhash、stat、pfop、pfopresult、avinfo 一般对 file 输入方式进行处理

### 3 结果持久化
对上一步输出的结果（包括数据源输出结果）进行持久化操作（目前支持写入到本地文件），持久化选项：
`result-path=` 表示保存结果的文件路径  
`result-format=` 结果保存格式（json/table）  
`result-separator=` 结果保存分隔符  
[result 详细配置](docs/result-save.md)

### 补充
1. 命令行方式与配置文件方式不可同时使用，指定 -config=<path> 或使用 qiniu.properties 时，需
要将所有参数设置在该配置文件中。
2. 一般情况下，命令行输出异常信息如 socket time 超时为正常现象，程序会自动重试，如：
```
listV2 xxx:|:null:1:null null, last 3 times retry...
listV2 xxx:|:null:1:null null, last 2 times retry...
java.net.SocketTimeoutException: timeout
```
超过重试次数或者其他非预期异常发生时程序会退出，可以将异常信息反馈在 
[ISSUE列表](https://github.com/NigelWu95/qiniu-suits-java/issues) 中。
