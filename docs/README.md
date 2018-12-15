# qiniu-suits (qsuits)
七牛云接口使用套件（可以工具形式使用），主要针对七牛云存储资源的批量处理进行功能的封装，提供更为简洁的操作方式。  
基于 Java 编写，可基于 JDK（1.8 及以上）环境在命令行或 IDE 运行。  

# 使用介绍
#### (1) 程序运行过程为：读取数据源 =》[按指定过程处理数据 =》] 结果持久化  
#### (2) 运行方式  
可以通过默认路径的配置文件来设置参数值，默认的配置文件需要放置在与 jar 包同路径下的 resources 文件夹中，文件名为  
`qiniu.properties` 或 .qiniu.properties，或者使用命令行参数 `-config=<config-filepath>` 指定配置文件路径。不使用配  
置文件的情况下可以完全从命令行指定参数，形式为 `-<property-name>=<value>`。  


### 1 数据源
支持从不同数据源读取到数据进行后续处理, 通过 **source-type** 来指定数据源方式:  
**source-type=list/file** (命令行方式则指定为 **-source-type=list/file**)  
`list` 表示从七牛存储空间列举出资源，`file` 表示从本地路径读取文件内容中的资源列表  

###### *关于并发处理*：  

```
(1) list 源，从存储空间中列举文件，支持多线程并发列举，线程数在配置文件中指定，自动按照线程数并发  
(2) file 源，从本地读取目录下的所有文件，一个文件进入一个线程处理，最大线程数由配置文件指定，与输入文件数之间小的值作为并发数  
```

### 2 处理过程
处理过程表示对由数据源输入的每一条记录进行一次方法调用，具体调用过程由处理类型参数指定:  
**process=type/status/lifecycle/copy/asyncfetch** (命令行方式则指定为 **-process=xxx**) 等  
`type` 表示修改空间资源的存储类型（低频/标准）  
`status` 表示修改空间资源的状态（启用/禁用）  
`lifecycle` 表示修改空间资源的生命周期  
`copy` 表示复制资源到指定空间  
`asyncfetch` 表示异步抓取资源到指定空间  

### 3 结果持久化
对上一步输出的结果（包括数据源输出结果）进行持久化操作（目前支持写入到本地文件），持久化选项：
```
result-path=../result
result-format=
result-separator=
save-total=
```
`result-path` 表示保存结果的文件路径  
`result-format` 结果保存格式（json/table，将每一条结果记录格式化为对应格式）  
`result-separator` 结果保存为 table 格式时使用的分隔符  
`save-total` 用于选择是否直接保存数据源输出结果  

###### *命令行方式*
```
-result-path= -save-total=true -result-format= -result-separator=
```
