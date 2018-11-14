# qiniu-suits-java
七牛接口使用套件（jdk1.8）

### command
* list bucket and process per parameter（字段含义和下述 properties 中各字段的释义一致）
```
java -jar qiniu-suits.jar -source-type=file -ak= -sk= -bucket=temp -result-format=json -result-path=../result
 -multi=true -max-threads=100 -version=2 -level=1 -unit-len=10000 -prefix= -anti-prefix= -f-key-prefix= -f-key-suffix= 
 -f-key-regex= -f-mime= -f-type= -f-date=2018-08-01 -f-time=00:00:00 -f-direction=1 -anti-f-key-prefix= 
 -anti-f-key-suffix= -anti-f-key-regex= -anti-f-mime= -file-path=../test -separator=\t -key-index= -process=
 -process-batch=true -process-ak= -process-sk= -bucket1=bucket1 -bucket2=bucket2 -keep-key=true -add-prefix=video/ 
 -type=1 -status=0 -days=0
```

### property file
* 不通过命令行传递参数时可以通过默认路径的配置文件来设置参数值，默认的配置文件需要放置在与 jar 包同路径下的 resources 文件夹中，
  文件名为 qiniu.properties 或 .qiniu.properties，或者使用 -config=<config-filepath> 指定配置文件路径，具体操作参数
  设置如下：
```
# 数据源方式，通过 list bucket 或者 local file 输入，输入的数据为文本行内容，可针对每一行数据进行处理，不同输入源方式对应不同的配置参数
#source-type=list
source-type=file

# 数据源为 list 方式的参数配置，list bucket 操作的 parameters，ak、sk 为账号的密钥对字符串，bucket 为空间名称，result-format 为列举结果
# 的保存格式，默认为 json（可选项为 csv）result-path 为保存列举和处理结果的相对路径（默认为 ../result），multi 表示是否开启并发列举（默认开
# 启），max-threads 表示可使用的最大线程数，实际线程会通过程序计算得到（小于等于该值），version 使用的列举接口版本，level 为列举并发级别（1 或
# 2, 默认为 2），unit-len 为每次列举请求列举的文件个数，prefix 表示只列举某个前缀下的文件，anti-prefix 表示过滤掉某个字符前缀（列举不包含该字
# 符作为前缀的文件，只支持设置常见的 ascii 字符）。
ak=
sk=
bucket=temp
result-format=json
result-path=../result
multi=true
max-threads=100
version=2
level=1
unit-len=10000
prefix=
anti-prefix=

# 进行列举时的过滤条件，f-key-prefix 表示过滤的文件名前缀，f-key-suffix 表示过滤的文件名后缀，f-key-regex 表示按正则表达式过滤文件名，
# f-mime 表示过滤文件的 mime 类型，f-type 表示过滤的文件类型，为 0 或 1。date、time 为判断是否进行 process 操作的时间点。direction 0 表
# 示向时间点以前，1 表示向时间点以后。过滤条件中，prefix、suffix、regex、mime 可以为列表形式，如 param1,param2,param3。prefix、suffix、
# regex 三者为针对文件名 key 的过滤条件，(filter(prefix) || filter(suffix)) && filter(regex) 组成 key 的过滤结果 true/false，结合
# 其他的过滤条件时为 &&（与）的关系得到最终过滤结果。anti-xx 的参数表示反向过滤条件，设置过滤条件会从全部列举结果中过滤出对应文件列表单独保存成结果
# list_other<index>.txt。
f-key-prefix=
f-key-suffix=
f-key-regex=
f-mime=
f-type=
f-date=2018-08-01
f-time=00:00:00
f-direction=1
anti-f-key-prefix=
anti-f-key-suffix=
anti-f-key-regex=
anti-f-mime=

# 数据源为 local file 方式的参数配置，即输入源为本地文件列表，列表必须包含的信息是文件名，一行匹配一个文件名，file-path 为相对目录路径或相对文件
# 路径，separator 表示每一行的多个字段间的分割符，key-index 为文件名字段的下标位置（如第一个字段为 key，则 index 为 0），其他参数含义同上，设
# 置 ak、sk、bucket 是对空间中资源进行处理的必须参数，同时，process 也为必要参数。
file-path=../test
separator=\t
key-index=
#result-path=../result
#max-threads=100
#ak=
#sk=
#bucket=temp
#process=copy

# 对每条记录进行什么操作，目前支持 fileCopy/changeLifecycle(deleteAfterDays)/changeType/changeStatus, process-batch 表示是否使用
# batch（批量请求）方式处理（默认为 true）。file copy 操作前提是两个空间在同一账号下，或者被复制的空间授权可读权限到目标账号，process-ak 和
# process-sk 可以设置目标账号的 ak、sk，bucket1 和 bucket2 表示被复制空间和目标空间的名称，keep-key 表示是否保留原文件名（默认为 true），
# add-prefix 表示复制到目标空间后添加固定文件名前缀。type 操作表示修改文件类型，1 表示低频存储，0 表示标准存储。status 操作表示修改文件状态，1
# 表示文件禁用，0 表示文件启用。lifecycle 操作表示修改文件生命周期，为 0 时表示永久的生命周期
process=
process-batch=true
process-ak=
process-sk=
bucket1=bucket1
bucket2=bucket2
keep-key=true
add-prefix=video/
type=1
status=0
days=0
```

* main dynamic parameters and description  
```
[version] -- 使用的 list 接口版本。并发列举是通过 ASCII 排序的字符作为前缀进行列举检测该前缀下是否存在文件，如果存在同时将得到
             的第一个文件名和开始的 marker 进行记录。多线程列举时遍历通过前缀列举得到的 Map，得到起始 marker 和结束 file 
             key 来启动段落列举。
[level] -- 使用前缀索引的级别（1|2，代表获取分段阶段使用的 prefix 长度）。level 为 1 时只遍历依次 ASCII 字符，得到的前缀为 
           1 个字符，level 为 2 时在第一次前缀列举的基础上再对 ASCII 字符依次进行一次拼接得到新的前缀字符，长度为 2，检测该前
           缀是否存在文件后得到分段的 Map。
[unit-len] -- 一次列举请求的 item 长度（单次文件个数限制），list v1 接口限制 1000，list v2 可设置 10000 甚至更多。
[prefix] -- 指定前缀进行列举，为空时完整列举

```

### list result
* 列举记录，spent time 为列举（或者同时进行 process 的操作）所花费的时间，running threads 为根据前缀列举启动的线程数。    

|version|level|unit-len| process |  filter  | file counts |spent time| machine | running threads |  
|-------|-----|--------|---------|----------|-------------|----------|---------|-----------------|  
|   2   |  2  |  10000 |  null   |  false   |  94898690   |   2h18m  | 16核32G |      50         |
|   2   |  1  |  10000 |  null   |  false   |  1893275    |  7minxxs | 8核16G  |      16         | 
|   2   |  2  |  20000 |  null   |  false   |  293940625  |   1h8m   | 16核32G |      200        |
|   2   |  1  |  20000 |  null   |  false   |  1526657    |  5minxxs | 8核16G  |      4          |
|   2   |  1  |  10000 |  null   |  false   |  911559     |  2minxxs | 8核16G  |      15         |

### list process parameter
```
默认值为空，不进行任何处理，直接列举得到文件列表。
[check] 检查空间文件包含的前缀，存在几个前缀表明可进行列举的最多并发数，结果文件保存每个前缀的第一个文件信息（用于分段列举的节点）。
[copy] 将列举出的文件 copy 至另一个 bucket，需要设置 copy 的账号及空间等参数。
[lifecycle] 将列举出的文件生命周期进行修改。
[status] 将列举出的文件状态进行修改，可指定某个时间点之前或之后的进行处理。
[type] 将列举出的文件存储类型进行修改，转换为低频存储或者高频存储，可指定某个时间点之前或之后的进行处理。
```
与 process 同时使用的参数还有 process-batch，通常情况下会选择（true）此操作方式，用集中的请求做批量处理，效果更好，但特殊操作
不支持 batch 的情况下不能使用。

### multi list suggestions
```
1、level 1 理论最大分段为 94+1 个，level 2 理论最大分段为 8836+1 个，但实际情况空间的文件可能只有几个统一的前缀，则只会分成几个段，根据前缀计
   算的分段数决定了实际线程数目。
2、通常情况下建议将 end-file 设置为 false（默认值）。
3、空间有大量删除时使用 list v1 可能会超时，直接使用 list v2，即 version=2，实际情况也是 list v2 效率更高。
4、推荐用法：version=2，unit-len=20000（version 2 的时候 unit-len 值可以调高，但是不建议过大，通常不超过 100000），500 万以内文件
   level=1，500 万以上文件 level=2，max-threads=100（level 1 的情况下实际最大线程数只能达到 95 个；如果使用 level 2，在机器配置较高时可
   以选择更高的值，如16核32G的机器可选择 200 个以上最大线程）。文件数量非常大时，可以先通过 process=check 方式来检查一下前缀个数，再考虑调整参
   数配置。
```

### extra comments
在起始阶段，由于需要通过前缀做列举检测，会比较耗时，同时，可能出现如下的超时异常，可忽略，原因是因为特殊前缀"|"服务端会超时响应，可
以通过 add-prefix=| 将该前缀过滤掉不进行列举。
<pre><code>
listV2 xxx:|:null:1:null null, last 3 times retry...
listV2 xxx:|:null:1:null null, last 2 times retry...
java.net.SocketTimeoutException: timeout
...
</code></pre>