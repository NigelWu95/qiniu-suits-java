# qiniu-suits-java
七牛接口使用套件（jdk1.8）

### command
* list bucket and process per parameter（字段含义和下述 properties 中各字段的释义一致）
```
java -jar qiniu-suits.jar -ak= -sk= -bucket= -result-path=../result -max-threads=30 -version=2 -level=2
 -end-file=true -unit-len=1000 -prefix= -process=type -process-batch=true -date=2018-08-01 -time=00:00:00
 -direction=0 -days=0 -type=1 -status=0 -access-key= -secret-key= -from= -to= -keep-key=true -add-prefix=
```

### property file
* 不通过命令行传递参数时可以通过默认路径的配置文件来设置参数值，默认的配置文件需要放置在与 jar 包同路径下的 resources 文件夹中，
  文件名为 .qiniu.properties，参数设置如下：
```
# list bucket 操作的 parameters，ak、sk 为账号的密钥对字符串，bucket 为空间名称，result-path 为保存列举和处理结果的相对
# 路径，max-threads 为最大线程数，version 使用的列举接口版本，level 为列举并发级别（1 或 2），end-file 为是否使用结束文件
# 名做分段标志，unit-len 为每次列举请求列举的文件个数，prefix 表示只列举某个前缀，filter 表示是否进行一些条件过滤。
ak=
sk=
bucket=temp
result-path=../result
max-threads=100
version=2
level=1
end-file=true
unit-len=1000
prefix=
filter=true
# 进行列举时的过滤条件，f-key-prefix 表示过滤的文件名前缀，f-key-suffix 表示过滤的文件名后缀，f-key-regex 表示按正则表达式
# 过滤文件名，f-mime 表示过滤文件的 mime 类型，f-type 表示过滤的文件类型，为 0 或 1。date、time 为判断是否进行 process 操
# 作的时间点。direction 0 表示向时间点以前，1 表示向时间点以后。过滤条件中，prefix、suffix、regex、mime 可以为列表形式，如 
# param1,param2,param3。prefix、suffix、regex 三者为针对文件名 key 的过滤条件，(filter(prefix) || filter(suffix))
# && filter(regex) 组成 key 的过滤结果 true/false，存在其他的过滤条件时为 &&（与）的关系得到最终过滤结果。
f-key-prefix=
f-key-suffix=
f-key-regex=
f-mime=
f-type=
f-date=2018-08-01
f-time=00:00:00
f-direction=1
# 反向过滤条件
anti-f-key-prefix=
anti-f-key-suffix=
anti-f-key-regex=
anti-f-mime=

# 对每条记录设置操作，目前支持 changeLifecycle(deleteAfterDays)/changeTyep/changeStatus/fileCopy, process-batch
# 表示是否使用 batch（批量请求）方式处理。
process=copy
process-batch=true
# type 操作的 parameter，1 表示低频存储，0 表示标准存储
type=1
# status 操作的 parameter，1 表示文件禁用，0 表示文件启用
status=0
# lifecycle 操作的 parameter，为 0 时表示永久的生命周期
days=0
# file copy 操作的 parameters，复制操作前提是两个空间在同一账号下，或者是被复制的空间为授权可读权限的空间，access-key 和
# secret-key 为目标空间的 ak、sk，from 和 to 表示源和目标 bucket 名称，keep-key 为是否保留原文件名，add-prefix 为附加
# 的目标文件前缀。
access-key=
secret-key=
from=bucket1
to=bucket2
keep-key=true
add-prefix=video/
```

* main dynamic parameters and description  
```
[version] -- 使用的 list 接口版本。并发列举是通过 ASCII 排序的字符作为前缀进行列举检测该前缀下是否存在文件，如果存在同时将得到
             的第一个文件名和开始的 marker 进行记录。多线程列举时遍历通过前缀列举得到的 Map，得到起始 marker 和结束 file 
             key 来启动段落列举。
[level] -- 使用前缀索引的级别（1|2，代表获取分段阶段使用的 prefix 长度）。level 为 1 时只遍历依次 ASCII 字符，得到的前缀为 
           1 个字符，level 为 2 时在第一次前缀列举的基础上再对 ASCII 字符依次进行一次拼接得到新的前缀字符，长度为 2，检测该前
           缀是否存在文件后得到分段的 Map。
[end-file] -- enable true|false，使用使用文件名判断一段是否列举结束，为 false 时直接利用 prefix 来列举直到 marker 为空。
              使用 end-file 来判断列举结束需要取出每一条记录中的 key 进行比较，依靠指定 prefix 进行分段列举时通过 marker 
              为空判断列举结束。理论上使用 end-file 会比 marker 来判断结束耗时更久。因为 list v1 的 marker 直接通过列举结
              果计算一次可得到，而取出每一个 key 要每一行计算。list v2 的每一行均包含 marker，得到 marker 经过一次 json 
              计算，得到 key 需要经过两次 json 计算。
[unit-len] -- 一次列举请求的 item 长度（单次文件个数限制），list v1 接口限制 1000，list v2 可设置 10000 甚至更多。
[prefix] -- 指定前缀进行列举，为空时完整列举

```

### list result
* 列举记录，spent time 为列举（或者同时进行 process 的操作）所花费的时间，running threads 为根据前缀列举启动的线程数。    

|version|level|end-file|unit-len| process |  filter  | file counts |spent time| machine | running threads |  
|-------|-----|--------|--------|---------|----------|-------------|----------|---------|-----------------|  
|   2   |  2  | false  |  10000 |  null   |  false   |  94898690   |   2h18m  | 16核32G |      50         |
|   2   |  1  | false  |  10000 |  null   |  false   |  1893275    |   7min   | 8核16G  |      16         | 
|   2   |  2  | false  |  20000 |  null   |  false   |  293940625  |   1h8m   | 16核32G |      200        |
|   2   |  1  | false  |  20000 |  null   |  false   |  1526657    |   5min   | 8核16G  |      4          |
|   2   |  1  | false  |  10000 |  null   |  false   |  911559     |   2min   | 8核16G  |      15         |

### list process parameter
```
默认值为空，不进行任何处理，直接列举得到文件列表。
[check] 检查空间文件包含的前缀，存在几个前缀表明可进行列举的最多并发数，结果文件保存每个前缀的第一个文件信息（用于分段列举的节点）。
[lifecycle] 将列举出的文件生命周期进行修改。
[status] 将列举出的文件状态进行修改，可指定某个时间点之前或之后的进行处理。
[type] 将列举出的文件存储类型进行修改，转换为低频存储或者高频存储，可指定某个时间点之前或之后的进行处理。
[copy] 将列举出的文件 copy 至另一个 bucket，需要设置 copy 的账号及空间等参数。
```
与 process 同时使用的参数还有 process-batch，通常情况下会选择（true）此操作方式，用集中的请求做批量处理，效果更好，但特殊操作
不支持 batch 的情况下不能使用。

### multi list suggestions
```
1、level 1 理论最大分段为 94+1 个，level 2 理论最大分段为 8836+1 个，但实际情况空间的文件可能只有几个统一的前缀，则只会分成几
   个段，分段数决定了实际线程数目。
2、通常情况下建议将 end-file 设置为 false（默认值）。
4、空间有大量删除时直接使用 list v2，即 version=2。
5、推荐用法：version=2，end-file=false，unit-len=20000（version 2 的时候 unit-len 值可以调高，但是不建议过大，通常不超
   过 100000），500 万以内文件 level=1，500 万以上文件 level=2，max-threads=100（level 1 的情况下实际最大线程数只能达到
   95 个；如果使用 level 2，在机器配置较高时可以选择更高的值，如16核32G的机器可选择 200 个以上最大线程）。文件数量非常大时，可
   以先通过 process=check 方式来检查一下前缀个数，再考虑调整参数配置。
```

### extra comments
在起始阶段，由于需要通过前缀做列举检测，会比较耗时，同时，可能出现如下的超时异常，可忽略，原因是因为特殊前缀"|"服务端会超时响应。
<pre><code>
listV2 xxx:|:null:1:null null, last 3 times retry...
listV2 xxx:|:null:1:null null, last 2 times retry...
java.net.SocketTimeoutException: timeout
...
</code></pre>