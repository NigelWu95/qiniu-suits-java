# 资源过滤

# 简介
对资源信息进行过滤，作为一个 process 过程来处理，接收 source-type 数据源输入的信息字段按
条件过滤后输出符合条件的记录，目前支持七牛存储资源的元信息字段。  

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
cf-key-prefix=
anti-f-key-suffix=
anti-f-key-regex=
anti-f-mime=
```
`f-key-prefix` 表示选择**符合**该前缀的文件  
`f-key-suffix` 表示选择**符合**该后缀的文件  
`f-key-regex` 表示选择**符合**该正则表达式的文件  
`f-mime` 表示选择**符合**该 mime 类型的文件  
`f-type` 表示选择**符合**该存储类型的文件, 为 0 或 1  
`date, time` 设置过滤的时间节点  
`direction` 表示时间节点过滤方向，0 表示选择**时间点以前**更新的文件，1 表示选择**时间点以后**更新的文件  
`anti-f-key-prefix` 表示**排除**符合该前缀的文件  
`anti-f-key-suffix` 表示**排除**符合该后缀的文件  
`anti-f-key-regex` 表示选择**排除**该正则表达式的文件  
`anti-f-mime` 表示选择**排除**该 mime 类型的文件  

**备注：**  
过滤条件中，prefix,suffix,regex,mime 可以为列表形式，如 param1,param2,param3。prefix,suffix,regex 三者为针对文件名 key  
的过滤条件，(filter(prefix) || filter(suffix)) && filter(regex) 组成 key 的过滤结果 true/false，结合其他的过滤条件时为 &&（与）的  
关系得到最终过滤结果。anti-xx 的参数表示反向过滤条件，即排除符合该特征的记录。

### 命令行方式
```
-f-key-prefix= -f-key-suffix= -f-key-regex= -f-mime= -f-type= -f-date= -f-time= -f-direction= -anti-f-key-prefix= -anti-f-key-suffix= -anti-f-key-regex= -anti-f-mime=
```
