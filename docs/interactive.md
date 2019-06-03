# 命令行交互式运行

## 简介
针对 process 功能提供的交互式命令行方式，输入初始条件参数后进行交互模式，每输入一行数据则根据初始参数来执行一次 process 并输出结果，然后可进行下一
次的数据输入，直到无数据输入按下【回车】键时则退出。

## 使用方式
#### 交互式命令行指令 -i/--interactive
在指定 process 和对应所需参数的情况下加上 `-i` 或者 `--interactive` 则进入交互模式，后续输入的每一行数据将根据初始参数来执行 process 操作，
输入的参数与各 process 提供的参数用法一致，具体可参见 process 的文档。如：
`java -jar qsuits.jar -i -process=privateurl -ak=ajkhsfgd -sk=akjdhsdfg -url-index=0`

### process 举例
###### 1 由于命令行作为输入读取时字符串长度存在限制，不支持很长的数据信息输入，如 avinfo 信息可能超过限制的长度，因此不建议在交互模式下进行 pfopcmd 的操作
###### 2 对空间资源执行 pfop 请求 [pfop 配置](pfop.md)  
```
➜ ~ java -jar qsuits-6.20.jar -i -process=pfop -ak=ajkhsfgd -sk=akjdhsdfg -bucket=temp -force-public=true -fops-index=1
please input line data to process: 
10.mp4	avthumb/mp4
java -jar qsuits-6.20.jar -i -process=pfop -ak=----- -sk=----- -bucket=temp -pipeline=audio-video -fops-index=1
please input line data to process: 
10.mp4  avthumb/mp4
z0.5cf4e0b138b9f31ea670c97e
```
###### 3 查询空间资源的视频元信息 [avinfo 配置](avinfo.md)  
```
➜ ~ java -jar qsuits-6.20.jar -i -process=avinfo -url-index=0
please input line data to process: 
http://p3l1d5mx4.bkt.clouddn.com/10.mp4
{"streams":[{"index":0,"codec_name":"h264","codec_long_name":"H.264 / AVC / MPEG-4 AVC / MPEG-4 part 10","profile":"High","codec_type":"video","codec_time_base":"1/60","codec_tag_string":"avc1","codec_tag":"0x31637661","width":720,"height":486,"coded_width":720,"coded_height":496,"has_b_frames":2,"sample_aspect_ratio":"1:1","display_aspect_ratio":"40:27","pix_fmt":"yuv420p","level":30,"chroma_location":"left","refs":1,"is_avc":"true","nal_length_size":"4","r_frame_rate":"30/1","avg_frame_rate":"30/1","time_base":"1/15360","start_pts":0,"start_time":"0.000000","duration_ts":92160,"duration":"6.000000","bit_rate":"695088","bits_per_raw_sample":"8","nb_frames":"180","disposition":{"default":1,"dub":0,"original":0,"comment":0,"lyrics":0,"karaoke":0,"forced":0,"hearing_impaired":0,"visual_impaired":0,"clean_effects":0,"attached_pic":0,"timed_thumbnails":0},"tags":{"creation_time":"2011-09-15T17:31:45.000000Z","language":"eng","handler_name":"VideoHandler"}}],"format":{"nb_streams":1,"nb_programs":0,"format_name":"mov,mp4,m4a,3gp,3g2,mj2","format_long_name":"QuickTime / MOV","start_time":"0.000000","duration":"6.000000","size":"524167","bit_rate":"698889","probe_score":100,"tags":{"major_brand":"isom","minor_version":"512","compatible_brands":"isomiso2avc1mp41","creation_time":"2011-09-15T17:31:45.000000Z","encoder":"Lavf57.71.100"}}}
please input line data to process: 
```
###### 4 查询资源的 qhash [qhash 配置](qhash.md)  
```
➜ ~ java -jar qsuits-6.20.jar -i -process=qhash -url-index=0
please input line data to process: 
http://p3l1d5mx4.bkt.clouddn.com/10.mp4
{"hash":"dc7a26a67763b478f0b05ec38b769349","fsize":524167}
```
###### 5 通过 persistentId 查询 pfop 的结果 [pfopresult 配置](pfopresult.md)  
```
➜ ~ java -jar qsuits-6.20.jar -i -process=pfopresult -persistentId-index=0
please input line data to process: 
z0.5cf4e0b138b9f31ea670c97e
{"code":0,"desc":"The fop was completed successfully","id":"z0.5cf4e0b138b9f31ea670c97e","inputBucket":"temp","inputKey":"10.mp4","items":[{"cmd":"avthumb/mp4","code":0,"desc":"The fop was completed successfully","hash":"FpBw7VMk5raxi-MG0ooVuPUSMNEs","key":"UAA-4hndfVc5V6DJX0EvslAUBBI=/FhbkdU00yYIPg13-Qu6jZzLVYqvT","returnOld":0}],"pipeline":"0.default","reqid":"5nIAAJCijWpxpKQV"}
please input line data to process:
```
###### 6 异步抓取资源到指定空间 [asyncfetch 配置](asyncfetch.md)  
```
➜ ~ java -jar qsuits-6.20.jar -i -process=asyncfetch -ak=------ -sk=------- -to-bucket=temp -url-index=0
please input line data to process: 
http://p3l1d5mx4.bkt.clouddn.com/10.mp4
{"code":200,"message":"{"id":"eyJ6b25lIjoiejAiLCJxdWV1ZSI6IlNJU1lQSFVTLUpPQlMtVjMiLCJwYXJ0X2lkIjozMCwib2Zmc2V0IjoxMjI3NTUwN30=","wait":14}"}
```

```
➜ ~  java -jar qsuits-6.20.jar -i -process=privateurl -ak=ajkhsfgd -sk=akjdhsdfg -url-index=0
please input line data to process: 
http://test.com/test.gif
http://test.com/test.gif?e=1559554823&token=ajkhsfgd:SlUz_s4-rN2J2hJJoLIVP_o3fBE=
please input line data to process: 
http://test.com/1.txt           
http://test.com/1.txt?e=1559554841&token=ajkhsfgd:Z2JveTVHLoOyAnhAOT0a9YfvAyg=
```

```
➜ ~  java -jar qsuits-6.20.jar -i -process=privateurl -ak=ajkhsfgd -sk=akjdhsdfg -url-index=0
please input line data to process: 
http://test.com/test.gif
http://test.com/test.gif?e=1559554823&token=ajkhsfgd:SlUz_s4-rN2J2hJJoLIVP_o3fBE=
please input line data to process: 
http://test.com/1.txt           
http://test.com/1.txt?e=1559554841&token=ajkhsfgd:Z2JveTVHLoOyAnhAOT0a9YfvAyg=
```

```
➜ ~  java -jar qsuits-6.20.jar -i -process=privateurl -ak=ajkhsfgd -sk=akjdhsdfg -url-index=0
please input line data to process: 
http://test.com/test.gif
http://test.com/test.gif?e=1559554823&token=ajkhsfgd:SlUz_s4-rN2J2hJJoLIVP_o3fBE=
please input line data to process: 
http://test.com/1.txt           
http://test.com/1.txt?e=1559554841&token=ajkhsfgd:Z2JveTVHLoOyAnhAOT0a9YfvAyg=
```

```
➜ ~  java -jar qsuits-6.20.jar -i -process=privateurl -ak=ajkhsfgd -sk=akjdhsdfg -url-index=0
please input line data to process: 
http://test.com/test.gif
http://test.com/test.gif?e=1559554823&token=ajkhsfgd:SlUz_s4-rN2J2hJJoLIVP_o3fBE=
please input line data to process: 
http://test.com/1.txt           
http://test.com/1.txt?e=1559554841&token=ajkhsfgd:Z2JveTVHLoOyAnhAOT0a9YfvAyg=
```

```
➜ ~ java -jar qsuits-6.20.jar -i -process=privateurl -ak=ajkhsfgd -sk=akjdhsdfg -url-index=0
please input line data to process: 
http://test.com/test.gif
http://test.com/test.gif?e=1559554823&token=ajkhsfgd:SlUz_s4-rN2J2hJJoLIVP_o3fBE=
please input line data to process: 
http://test.com/1.txt           
http://test.com/1.txt?e=1559554841&token=ajkhsfgd:Z2JveTVHLoOyAnhAOT0a9YfvAyg=
```
