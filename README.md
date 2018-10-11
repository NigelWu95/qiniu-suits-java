# qiniu-java-suits
七牛接口使用套件

### command
* list bucket and process per item
```
java -jar qiniu-java-suits-1.0.jar -ak= -sk= -bucket= -result-path=../result -max-threads=30 -version=2
 -level=2 -end-file=true -parallel=true -unit-len=1000 -process=copy -type=1 -status=0 -date=2018-08-01
  -time=00:00:00 -direction=0 -access-key= -secret-key= -from= -to= -keep-key=true -add-prefix=
```

### property file
* 不通过命令行传递参数时可以通过默认路径的配置文件来设置参数值，默认的配置文件需要放置在与 jar 包同路径下的 resources 文件夹
  中，文件名为 .qiniu.properties，参数设置如下：
```
# list bucket
ak=
sk=
bucket=temp
# 相对路径
result-path=../result
max-threads=30
version=2
end-file=true
parallel=true
level=2
process=copy
unit-len=1000

# process
# 1 表示低频存储，0 表示标准存储
type=1
# 1 表示文件禁用，0 表示文件启用
status=0
# 判断是否进行 process 操作的时间点
date=2018-08-01
time=00:00:00
# 0 表示向时间点以前，1 表示向时间点以后
direction=0

# file copy
access-key=
secret-key=
# 源和目标 bucket 名称
from=bucket1
to=bucket2
keep-key=true
add-prefix=video/
```

### list test result
* spent time 为本地测试列举 157330 个文件所花费的时间，根据前缀启动的线程 level 为 1 时为 2 个，level 为 2 时为 3 个。  

|version|level|end-file|parallel|unit-len|spent time|  
|-------|-----|--------|--------|--------|----------|  
|   1   |  1  |  true  |  false |  1000  |   77s    | 
|   1   |  1  |  false |  false |  1000  |   55s    | 
|   1   |  2  |  true  |  false |  1000  |   110s   | 
|   1   |  2  |  false |  false |  1000  |   111s   | 
|   2   |  1  |  true  |  true  |  1000  |   73s    | 
|   2   |  1  |  false |  true  |  1000  |   59s    | 
|   2   |  1  |  true  |  true  |  10000 |   84s    | 
|   2   |  1  |  false |  true  |  10000 |   37s    | 
|   2   |  2  |  true  |  true  |  1000  |   126s   | 
|   2   |  2  |  false |  true  |  1000  |   120s   | 
|   2   |  2  |  true  |  true  |  10000 |   57s    | 
|   2   |  2  |  false |  true  |  10000 |   62s    |

* 列举记录。  

|version|level|end-file|parallel|unit-len| process | file counts |spent time| machine | running threads |  
|-------|-----|--------|--------|--------|---------|-------------|----------|---------|-----------------|  
|   2   |  2  | false  |  false |  10000 |  null   |  94898690   |   2h18m  | 16核32G |      50         |
|   2   |  1  | false  |  false |  10000 |  null   |  1893275    |   7min   | 8核16G  |      16         | 
 

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
[parallel] -- list v2 使用流式处理，选择是否使用 parallel 方式。
[unit-len] -- 一次列举请求的 item 长度（单次文件个数限制），list v1 接口限制 1000，list v2 可设置 10000 甚至更多。

```

### multi list suggestions
```
1、level 1 理论最大分段为 88 个，level 2 理论最大分段为 7744 个，但实际情况空间的文件可能只有几个统一的前缀，通常只会分成几个
   段，分成的段数决定了实际线程数目。
2、对于 100 万以内的文件，建议不要使用 level 2。
3、通常情况下建议将 end-file 设置为 false（默认值）。
4、空间有大量删除时直接使用 list v2，使用 list v1 时 parallel 无效，使用 list v2 时 parallel 默认为 true，同时 v2 的 
   unit-len 可视总文件数进行调整。
5、推荐用法：version=2 end-file=false unit-len=10000，100 万以内文件 level=1，100 万以上文件 level=2。
```

### list process parameter
```
默认值为空，不进行任何处理，直接列举得到文件列表。
[copy] 将列举出的文件 copy 至另一个 bucket，需要设置 copy 的账号及空间等参数。
[status] 将列举出的文件状态进行修改，可指定某个时间点之前或之后的进行处理。
[type] 将列举出的文件存储类型进行修改，转换为低频存储或者高频存储，可指定某个时间点之前或之后的进行处理。
```