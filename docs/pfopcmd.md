# 生成音视频转码指令

## 简介
根据已经获取到的音视频资源的 avinfo 信息，来设置规则判断进行某种自定义的转码并自定义转码后的文件名。参考：[七牛数据处理 pfop 文档](https://developer.qiniu.com/dora/manual/3686/pfop-directions-for-use)

## 配置文件
**操作需指定数据源，请先[配置数据源](datasource.md)**  

### 配置参数
```
process=pfopcmd
indexes=
pfop-config=
duration=
size=
avinfo-index=
```  
|参数名|参数值及类型 | 含义|  
|-----|-------|-----|  
|process=pfopcmd| 该操作设置为pfopcmd| 表示根据 avinfo 生成音视频转码指令|  
|indexes|字符串| 设置输入行中 key 字段的下标（有默认值），参考[数据源 indexes 设置](datasource.md#1-公共参数)|  
|pfop-config| 文件路径字符串| 进行转码和另存规则设置的 json 配置文件路径，可设置多个转码条件和指令，[配置写法](#pfop-config-配置文件内容写法如下：)|  
|duration| true/false| 得到的结果行中是否需要保存 duration（音视频时长）信息，会放在转码指令字段之后 |  
|size| true/false| 得到的结果行中是否需要保存 size（音视频时长）信息，会放在 duration 字段之后|  
|avinfo-index| 字符串| 读取 avinfo 信息时需要设置的 avinfo 字符串索引（下标），必须指定才能进行处理|  

#### pfop-config 配置文件内容写法如下：
```
{
  "pfopcmd":[
    {
      "scale":[1000,1280],
      "cmd":"avthumb/mp4/s/1280x720/autoscale/1",
      "saveas":"bucket:$(key)F720.mp4"
    },
    {
      "scale":[1280],
      "cmd":"avthumb/mp4/s/1280x720/autoscale/1",
      "saveas":"bucket:$(key)F1080"
    }
  ]
}
```  
如上所示，pfopcmd 操作的配置名称为 "pfopcmd"，配置项为 json array，可参见 [pfop-config 配置](../resources/process.json)  
|必须选项|含义|  
|-----|-----|  
|key|上述配置文件中的 F720 为转码项名称，设置为 json key，key 不可重复，重复情况下后者会覆盖前者|  
|scale| 表示视频分辨率 width 的范围，只设置了一个值则表示目标范围大于该值，程序根据 avinfo 判断宽度范围对在此区间的文件生成转码指令|  
|cmd| 需要指定的转码指令 |  
|saveas| 转码结果另存的格式，写法为：<bucket>:<key>，其中 <key> 支持[魔法变量](#魔法变量)|  

##### 关于 saveas  
###### 魔法变量  
`$(name)` 表示完整的原始文件名（如 a.jpg/a.png 的 $(name) 分别为为 a.jpg/a.png）  
`$(key)` 表示去除后缀的原始文件名（如 a.jpg/a.png/a 的 $(key) 均为 a）  
`$(ext)` 表示文件名的后缀部分（如 a.jpg/b.jpg 的 $(ext) 均为 jpg，c 的 $(ext) 为空字符串）  
###### 格式拼接  
格式需要遵循 <bucket>:<key>，允许只有 <bucket>，此时表示由七牛自动生成文件名，但是不允许缺少 <bucket>，且不允许以 : 开头或结尾的格式。  

## 命令行方式
```
-process=pfopcmd -pfop-config= -duration= -size=
```
