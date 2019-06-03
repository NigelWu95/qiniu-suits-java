# 单行命令模式

## 简介
针对 process 功能提供的单行命令模式，为需要对单个资源执行一次操作提供方便，程序生命周期只有一次，从输入行指定 process 及 data 参数，直接执行并输
出结果。【此功能不需要数据源配置】

## 使用方式
#### 单行模式指令 -s/--single/-line=\<data-line\>
在指定 process 的情况下加上 `-s` 或者 `--single` 则表示使用单行模式，会直接执行 process 操作，因此必须包含该 process 所需的最少参数（兼容各 
process 提供的参数用法，具体可参见 process 的文档）包括要处理的 data 参数，如 key 或 url 等。

### process 用法
###### 1 根据音视频资源的 avinfo 信息来生成转码指令 [pfopcmd 配置](pfopcmd.md)  
```
➜ ~ java -jar qsuits-6.20.jar \
> -process=pfopcmd \
> -scale=[999] \
> -avinfo={\"streams\":[{\"index\":0,\"codec_name\":\"h264\",\"codec_long_name\":\"H.264 / AVC / MPEG-4 AVC / MPEG-4 part 10\",\"profile\":\"Main\",\"codec_type\":\"video\",\"codec_time_base\":\"1/50\",\"codec_tag_string\":\"avc1\",\"codec_tag\":\"0x31637661\",\"width\":1920,\"height\":1080,\"coded_width\":1920,\"coded_height\":1088,\"has_b_frames\":2,\"pix_fmt\":\"yuv420p\",\"level\":40,\"chroma_location\":\"left\",\"refs\":1,\"is_avc\":\"true\",\"nal_length_size\":\"4\",\"r_frame_rate\":\"25/1\",\"avg_frame_rate\":\"25/1\",\"time_base\":\"1/12800\",\"start_pts\":0,\"start_time\":\"0.000000\",\"duration_ts\":2506240,\"duration\":\"195.800000\",\"bit_rate\":\"4829438\",\"bits_per_raw_sample\":\"8\",\"nb_frames\":\"4895\",\"disposition\":{\"default\":1,\"dub\":0,\"original\":0,\"comment\":0,\"lyrics\":0,\"karaoke\":0,\"forced\":0,\"hearing_impaired\":0,\"visual_impaired\":0,\"clean_effects\":0,\"attached_pic\":0,\"timed_thumbnails\":0},\"tags\":{\"language\":\"und\",\"handler_name\":\"VideoHandler\"}},{\"index\":1,\"codec_name\":\"aac\",\"codec_long_name\":\"AAC (Advanced Audio Coding)\",\"profile\":\"LC\",\"codec_type\":\"audio\",\"codec_time_base\":\"1/48000\",\"codec_tag_string\":\"mp4a\",\"codec_tag\":\"0x6134706d\",\"sample_fmt\":\"s16p\",\"sample_rate\":\"48000\",\"channels\":2,\"channel_layout\":\"stereo\",\"bits_per_sample\":0,\"r_frame_rate\":\"0/0\",\"avg_frame_rate\":\"0/0\",\"time_base\":\"1/48000\",\"start_pts\":0,\"start_time\":\"0.000000\",\"duration_ts\":9399264,\"duration\":\"195.818000\",\"bit_rate\":\"128006\",\"max_bit_rate\":\"128006\",\"nb_frames\":\"9181\",\"disposition\":{\"default\":1,\"dub\":0,\"original\":0,\"comment\":0,\"lyrics\":0,\"karaoke\":0,\"forced\":0,\"hearing_impaired\":0,\"visual_impaired\":0,\"clean_effects\":0,\"attached_pic\":0,\"timed_thumbnails\":0},\"tags\":{\"language\":\"und\",\"handler_name\":\"SoundHandler\"}}],\"format\":{\"nb_streams\":2,\"nb_programs\":0,\"format_name\":\"mov,mp4,m4a,3gp,3g2,mj2\",\"format_long_name\":\"QuickTime / MOV\",\"start_time\":\"0.000000\",\"duration\":\"195.852000\",\"size\":\"121481511\",\"bit_rate\":\"4962175\",\"probe_score\":100,\"tags\":{\"major_brand\":\"isom\",\"minor_version\":\"512\",\"compatible_brands\":\"isomiso2avc1mp41\",\"encoder\":\"Lavf56.18.101\"}}}        
```
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
###### 3 通过 persistentId 查询 pfop 的结果 [pfopresult 配置](pfopresult.md)  
```
➜ ~ java -jar qsuits-6.20.jar -i -process=pfopresult -persistentId-index=0
please input line data to process: 
z0.5cf4e0b138b9f31ea670c97e
{"code":0,"desc":"The fop was completed successfully","id":"z0.5cf4e0b138b9f31ea670c97e","inputBucket":"temp","inputKey":"10.mp4","items":[{"cmd":"avthumb/mp4","code":0,"desc":"The fop was completed successfully","hash":"FpBw7VMk5raxi-MG0ooVuPUSMNEs","key":"UAA-4hndfVc5V6DJX0EvslAUBBI=/FhbkdU00yYIPg13-Qu6jZzLVYqvT","returnOld":0}],"pipeline":"0.default","reqid":"5nIAAJCijWpxpKQV"}
please input line data to process:
```
###### 4 查询空间资源的视频元信息 [avinfo 配置](avinfo.md)  
```
➜ ~ java -jar qsuits-6.20.jar -i -process=avinfo -url-index=0
please input line data to process: 
http://p3l1d5mx4.bkt.clouddn.com/10.mp4
{"streams":[{"index":0,"codec_name":"h264","codec_long_name":"H.264 / AVC / MPEG-4 AVC / MPEG-4 part 10","profile":"High","codec_type":"video","codec_time_base":"1/60","codec_tag_string":"avc1","codec_tag":"0x31637661","width":720,"height":486,"coded_width":720,"coded_height":496,"has_b_frames":2,"sample_aspect_ratio":"1:1","display_aspect_ratio":"40:27","pix_fmt":"yuv420p","level":30,"chroma_location":"left","refs":1,"is_avc":"true","nal_length_size":"4","r_frame_rate":"30/1","avg_frame_rate":"30/1","time_base":"1/15360","start_pts":0,"start_time":"0.000000","duration_ts":92160,"duration":"6.000000","bit_rate":"695088","bits_per_raw_sample":"8","nb_frames":"180","disposition":{"default":1,"dub":0,"original":0,"comment":0,"lyrics":0,"karaoke":0,"forced":0,"hearing_impaired":0,"visual_impaired":0,"clean_effects":0,"attached_pic":0,"timed_thumbnails":0},"tags":{"creation_time":"2011-09-15T17:31:45.000000Z","language":"eng","handler_name":"VideoHandler"}}],"format":{"nb_streams":1,"nb_programs":0,"format_name":"mov,mp4,m4a,3gp,3g2,mj2","format_long_name":"QuickTime / MOV","start_time":"0.000000","duration":"6.000000","size":"524167","bit_rate":"698889","probe_score":100,"tags":{"major_brand":"isom","minor_version":"512","compatible_brands":"isomiso2avc1mp41","creation_time":"2011-09-15T17:31:45.000000Z","encoder":"Lavf57.71.100"}}}
please input line data to process: 
```
###### 5 查询资源的 qhash [qhash 配置](qhash.md)  
```
➜ ~ java -jar qsuits-6.20.jar -i -process=qhash -url-index=0
please input line data to process: 
http://p3l1d5mx4.bkt.clouddn.com/10.mp4
{"hash":"dc7a26a67763b478f0b05ec38b769349","fsize":524167}
```
###### 6 异步抓取资源到指定空间 [asyncfetch 配置](asyncfetch.md)  
```
➜ ~ java -jar qsuits-6.20.jar -i -process=asyncfetch -ak=------ -sk=------- -to-bucket=temp -url-index=0
please input line data to process: 
http://p3l1d5mx4.bkt.clouddn.com/10.mp4
200	{"id":"eyJ6b25lIjoiejAiLCJxdWV1ZSI6IlNJU1lQSFVTLUpPQlMtVjMiLCJwYXJ0X2lkIjozMCwib2Zmc2V0IjoxMjI3NTUwN30=","wait":14}
```
###### 7 修改空间资源的生命周期 [lifecycle 配置](lifecycle.md)  
```
➜ ~ java -jar qsuits-6.20.jar -i -process=lifecycle -ak=------ -sk=------- -bucket=temp -days=1           
please input line data to process: 
10.mp4
200	
```
###### 8 修改空间资源的状态（启用/禁用）[status 配置](status.md)  
```
➜ ~ java -jar qsuits-6.20.jar -i -process=status -ak=-------- -sk=-------- -bucket=temp -status=1
please input line data to process: 
10.mp4
https://rs.qbox.me/chstatus/dGVtcDoxMC5tcDQ=/status/1  	{ResponseInfo:com.qiniu.http.Response@453da22c,status:400, reqId:owYAAEhBB6FpqqQV, xlog:-, xvia:vdn-gdzh-tel-1-4, adress:rs.qbox.me/113.106.101.6:443, duration:0.000000 s, error:already disabled}  	{"error":"already disabled"}
```
###### 9 修改空间资源的存储类型（低频/标准）[type 配置](type.md)  
```
➜ ~ java -jar qsuits-6.20.jar -i -process=type -ak=--------- -sk=-------- -bucket=temp -type=1
please input line data to process: 
10.mp4
https://rs.qbox.me/chtype/dGVtcDoxMC5tcDQ=/type/1  	{ResponseInfo:com.qiniu.http.Response@442675e1,status:400, reqId:rG8AANO3CON7qqQV, xlog:-, xvia:vdn-gdzh-tel-1-7, adress:rs.qbox.me/113.106.101.4:443, duration:0.000000 s, error:already in line stat}  	{"error":"already in line stat"}
```
###### 10 复制资源到指定空间 [copy 配置](copy.md)  
```
➜ ~ java -jar qsuits-6.20.jar -i -process=copy -ak=-------- -sk=-------- -bucket=temp -to-bucket=ts-work
please input line data to process: 
10.mp4
200
```
###### 11 移动资源到指定空间 [move 配置](move.md)  
```
➜ ~ java -jar qsuits-6.20.jar -i -process=move -ak=------- -sk=-------- -bucket=temp -to-bucket=ts-work
please input line data to process: 
10.mp4
200
```
###### 12 对指定空间的资源进行重命名 [rename 配置](rename.md)  
```
➜ ~ java -jar qsuits-6.20.jar -i -process=rename -ak=------ -sk=------- -bucket=temp -add-prefix=1 -prefix-force=true
please input line data to process: 
10.mp4
200
```
###### 13 删除空间资源 [delete 配置](delete.md)  
```
➜ ~ java -jar qsuits-6.20.jar -i -process=delete -ak=---------- -sk=--------- -bucket=ts-work
please input line data to process: 
10.mp4
200
```
###### 14 查询空间资源的元信息 [stat 配置](stat.md)  
```
➜ ~ java -jar qsuits-6.20.jar -i -process=stat -ak=-------- -sk=-------- -bucket=temp
please input line data to process: 
110.mp4
110.mp4	FhbkdU00yYIPg13-Qu6jZzLVYqvT	524167	2019-06-03T19:06:42.137696400	video/mp4	1	1
please input line data to process:
```
###### 15 对设置了镜像源的空间资源进行镜像更新 [mirror 配置](mirror.md)  
```
➜ ~ java -jar qsuits-6.20.jar -i -process=mirror -ak=--------- -sk=---------- -bucket=temp
please input line data to process: 
10.mp4
https://iovip.qbox.me/prefetch/dGVtcDoxMC5tcDQ=  	{ResponseInfo:com.qiniu.http.Response@453da22c,status:478, reqId:Zd8AAAATETyxqqQV, xlog:X-Log, xvia:, adress:iovip.qbox.me/115.231.100.199:443, duration:0.000000 s, error:httpGet url failed: E502}  	{"error":"httpGet url failed: E502"}
```
###### 16 对私有空间资源进行私有签名 [privateurl 配置](privateurl.md)  
```
➜ ~ java -jar qsuits-6.20.jar -i -process=privateurl -ak=------- -sk=------- -url-index=0
please input line data to process: 
http://test.xxx.com/test.gif
http://test.xxx.com/test.gif?e=1559563838&token=XgP9wnGCGGX8FlS7zxfOQcPev6pFUBo0T:8am3Kt-djGQXy9MS2_lqvzTxkZI=
```
###### 17 对 m3u8 的资源进行读取导出其中的 ts 文件列表 [exportts 配置](exportts.md)  
```
➜ ~ java -jar qsuits-6.20.jar -i -process=exportts -url-index=0
please input line data to process: 
http://p3l28y6an.bkt.clouddn.com/csc-4.m3u8
http://p3l28y6an.bkt.clouddn.com/room_02/csc000000	60.08sec
http://p3l28y6an.bkt.clouddn.com/room_02/csc000001	60.0sec
http://p3l28y6an.bkt.clouddn.com/room_02/csc000002	60.0sec
http://p3l28y6an.bkt.clouddn.com/room_02/csc000003	60.0sec
http://p3l28y6an.bkt.clouddn.com/room_02/csc000004	60.0sec
http://p3l28y6an.bkt.clouddn.com/room_02/csc000005	60.0sec
http://p3l28y6an.bkt.clouddn.com/room_02/csc000006	60.0sec
http://p3l28y6an.bkt.clouddn.com/room_02/csc000007	60.0sec
http://p3l28y6an.bkt.clouddn.com/room_02/csc000008	60.0sec
http://p3l28y6an.bkt.clouddn.com/room_02/csc000009	7.8sec
```