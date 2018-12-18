# 资源文件过滤

# 简介
对文件信息进行过滤，作为一个 process 过程来处理，接收 source-type 数据源输入的信息字段按条件过滤后输出符合条件的记  
录，目前支持七牛存储资源的元信息字段。

### 配置文件选项
```
f-key-prefix=
f-key-suffix=
f-key-regex=
f-mime=
f-type=
f-date=2018-08-01
f-time=00:00:00
f-direction=
anti-f-key-prefix=
anti-f-key-suffix=
anti-f-key-regex=
anti-f-mime=
```
`f-key-prefix` 表示过滤的文件名前缀  
`f-key-suffix` 表示过滤的文件名后缀  
`f-key-regex` 表示按正则表达式过滤文件名  
`f-mime` 表示过滤文件的 mime 类型  
`f-type` 表示过滤的文件类型, 为 0 或 1  
`date, time` 为判断是否进行 process 操作的时间节点  
`direction` 表示过滤时间方向，0 表示向时间点以前，1 表示向时间点以后  

**备注：**  
过滤条件中，prefix,suffix,regex,mime 可以为列表形式，如 param1,param2,param3。prefix,suffix,regex 三者为针对文件名 key  
的过滤条件，(filter(prefix) || filter(suffix)) && filter(regex) 组成 key 的过滤结果 true/false，结合其他的过滤条件时为 &&（与）的  
关系得到最终过滤结果。anti-xx 的参数表示反向过滤条件，即排除符合该特征的记录。

### 命令行方式
```
-f-key-prefix= -f-key-suffix= -f-key-regex= -f-mime= -f-type= -f-date= -f-time= -f-direction= -anti-f-key-prefix= -anti-f-key-suffix= -anti-f-key-regex= -anti-f-mime=
```
