# 多媒体资源数据处理

## 简介
对空间中的资源请求 pfop 持久化数据处理。参考：[七牛数据处理 pfop 文档](https://developer.qiniu.com/dora/manual/3686/pfop-directions-for-use)  
1. **操作需要指定数据源，默认表示从七牛空间列举文件执行操作，如非默认或需更多条件，请先[配置数据源](datasource.md)**  
2. 支持通过 `-a=<account-name>`/`-d` 使用已设置的账号，则不需要再直接设置密钥，参考：[账号设置](../README.md#账号设置)  
3. 单次请求一个 pfop 操作请参考[ single 操作](single.md)  
4. 交互式操作随时输入 key 进行 pfop 请参考[ interactive 操作](interactive.md)  

## 配置
```
process=pfop 
ak= 
sk= 
bucket=
indexes=
pipeline=
force-public=
fops-index=
pfop-config=
#cmd=
#saveas=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=pfop| 数据处理时设置为pfop| 表示数据处理操作|  
|ak、sk|长度40的字符串|七牛账号的ak、sk，通过七牛控制台个人中心获取，当数据源为 qiniu 时无需再设置|  
|bucket| 字符串| 操作的资源原空间，当数据源为 qiniu 时无需再设置|  
|indexes|字符串| 设置输入行中 key 字段的下标（有默认值），参考[数据源 indexes 设置](datasource.md#1-公共参数)|  
|pipeline| 字符串| 进行持久化数据处理的队列名称|  
|force-public| true/false| 是否强制使用共有队列（会有性能影响）|  
|fops-index| 字符串| 转码命令索引（下标），pfop 操作时指定，明确指定文件名对应的转码命令，建议命令中携带 saveas 重命名指令否则使用默认名|  
|pfop-config| 文件路径字符串| 进行转码和另存规则设置的 json 配置文件路径，可设置多个转码条件和指令，该配置会覆盖 fops-index 设置的转码命令，[配置写法](#pfop-config-配置文件)|  
|cmd| 字符串| pfop 命令，如 avthumb/mp4，和 pfop-config 中的 cmd 相同，如果不想设置 pfop-config 配置文件则可采用 cmd + saveas 设置的方式|  
|saveas| 字符串| saveas 的格式，如 bucket:$(key)F720.mp4，和 pfop-config 中的 saveas 相同，如果不想设置 pfop-config 配置文件则可采用 cmd + saveas 设置的方式|  

### 关于 fops-index
指定输入行中对应转码的命令字段下标，不设置为则无法进行解析。由于转码必须参数包含 key 和 fops，因此输入行中也必须包含 key 字段的值，使用 indexes 
参数设置 key 下标，同时 key 下标不能与 fops 下标相同。  

### pfop-config 配置文件
```json
{
  "pfop":[
    {
      "cmd":"avthumb/mp4/s/1280x720/autoscale/1",
      "saveas":"bucket:$(key)F720.mp4"
    },
    {
      "cmd":"avsmart/mp4",
      "saveas":"bucket:$(key)-avsmart.mp4"
    }
  ]
}
```  
如上所示，pfop 操作的配置名称为 "pfop"，配置项为 json array，可参见 [pfop-config 配置](../resources/process.json)  

|必须选项|含义|  
|-----|-----|  
|key|上述配置文件中的 "F720" 为转码项名称，设置为 json key，key 不可重复，重复情况下后者会覆盖前者|  
|cmd| 需要指定的转码指令 |  
|saveas| 转码结果另存的格式，写法为：`<bucket>:<key>`，其中 `<key>` 支持[魔法变量](#魔法变量)|  

### 关于 saveas  
##### 魔法变量  
`$(name)` 表示完整的原始文件名（如 a.jpg/a.png 的 $(name) 分别为为 a.jpg/a.png）  
`$(key)` 表示去除后缀的原始文件名（如 a.jpg/a.png/a 的 $(key) 均为 a）  
`$(ext)` 表示文件名的后缀部分（如 a.jpg/b.jpg 的 $(ext) 均为 jpg，c 的 $(ext) 为空字符串）  
##### 格式拼接  
格式需要遵循 `<bucket>:<key>`，允许只有 `<bucket>`，此时表示由七牛自动生成文件名，但是不允许缺少 `<bucket>`，且不允许以 `:` 开头或结尾的格式。  

### 命令行方式
```
-process=pfop -ak= -sk= -bucket= -pipeline= -force-public=
```
