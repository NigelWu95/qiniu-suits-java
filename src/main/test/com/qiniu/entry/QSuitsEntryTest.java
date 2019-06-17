package com.qiniu.entry;

import com.qiniu.config.ParamsConfig;
import com.qiniu.datasource.IDataSource;
import com.qiniu.datasource.InputSource;
import com.qiniu.interfaces.IEntryParam;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.util.ParamsUtils;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class QSuitsEntryTest {

    @Test
    @SuppressWarnings("unchecked")
    public void testEntry1() throws Exception {
        IEntryParam entryParam = new ParamsConfig("resources/.application.properties");
        QSuitsEntry qSuitsEntry = new QSuitsEntry(entryParam);
        ILineProcess<Map<String, String>> processor = qSuitsEntry.getProcessor();
        IDataSource dataSource = qSuitsEntry.getDataSource();
        if (dataSource != null) {
            dataSource.setProcessor(processor);
            dataSource.export();
        }
        if (processor != null) processor.closeResource();
    }

    @Test
    public void testEntry2() throws Exception {
        String[] args = new String[]{
                "-ak=ksjadasfdljdhsjaksdfdjfgksjdsasdfsghfhfg",
                "-sk=adsjkfadsfdgfhgjjhfgdfdfsdgfhgfdsrtyhvgh"
                , "-domain=xxx.com"
//                , "-line=\"ashdfasd\""
                , "-key=\"0000021-result\""
//                , "-process=exportts"
//                , "-url=http://xxx.com/1618060907DD4B1FA72E50491C08B356.m3u8"
//                , "-process=privateurl"
//                , "-url=http://xxx.com/test.gif"
//                , "-process=avinfo"
//                , "-process=qhash"
//                , "-process=asyncfetch"
                , "-to-bucket=ts-work"
                , "-url=http://xxx.com/1080P/%E8%B6%85%E6%B8%85%E9%87%91%E6%B5%B7%E6%B9%96%E7%BE%8E%E6%99%AF.mp4"
//                , "-url-index=ghl"
//                , "-parse=json"
                , "-bucket=ts-work"
//                , "-process=delete"
//                , "-process=stat"
//                , "-process=type"
//                , "-type=1"
//                , "-process=status"
//                , "-status=1"
//                , "-process=lifecycle"
//                , "-days=1"
//                , "-process=copy"
//                , "-process=move"
//                , "-process=rename"
//                , "-prefix-force=true"
//                , "-add-prefix=a/"
                , "-to-key=0000021-ret"
//                , "-process=pfop"
                , "-process=pfopresult"
                , "-persistentId=z0.5cf6460238b9f31ea6d3d29d"
                , "-force-public=true"
//                , "-process=pfopcmd"
                , "-scale=[999]"
                , "-avinfo-index=1"
                , "-avinfo={\"streams\":[{\"index\":0,\"codec_name\":\"h264\",\"codec_long_name\":\"H.264 / AVC / MPEG-4 AVC / MPEG-4 part 10\",\"profile\":\"Main\",\"codec_type\":\"video\",\"codec_time_base\":\"1/50\",\"codec_tag_string\":\"avc1\",\"codec_tag\":\"0x31637661\",\"width\":1920,\"height\":1080,\"coded_width\":1920,\"coded_height\":1088,\"has_b_frames\":2,\"pix_fmt\":\"yuv420p\",\"level\":40,\"chroma_location\":\"left\",\"refs\":1,\"is_avc\":\"true\",\"nal_length_size\":\"4\",\"r_frame_rate\":\"25/1\",\"avg_frame_rate\":\"25/1\",\"time_base\":\"1/12800\",\"start_pts\":0,\"start_time\":\"0.000000\",\"duration_ts\":2506240,\"duration\":\"195.800000\",\"bit_rate\":\"4829438\",\"bits_per_raw_sample\":\"8\",\"nb_frames\":\"4895\",\"disposition\":{\"default\":1,\"dub\":0,\"original\":0,\"comment\":0,\"lyrics\":0,\"karaoke\":0,\"forced\":0,\"hearing_impaired\":0,\"visual_impaired\":0,\"clean_effects\":0,\"attached_pic\":0,\"timed_thumbnails\":0},\"tags\":{\"language\":\"und\",\"handler_name\":\"VideoHandler\"}},{\"index\":1,\"codec_name\":\"aac\",\"codec_long_name\":\"AAC (Advanced Audio Coding)\",\"profile\":\"LC\",\"codec_type\":\"audio\",\"codec_time_base\":\"1/48000\",\"codec_tag_string\":\"mp4a\",\"codec_tag\":\"0x6134706d\",\"sample_fmt\":\"s16p\",\"sample_rate\":\"48000\",\"channels\":2,\"channel_layout\":\"stereo\",\"bits_per_sample\":0,\"r_frame_rate\":\"0/0\",\"avg_frame_rate\":\"0/0\",\"time_base\":\"1/48000\",\"start_pts\":0,\"start_time\":\"0.000000\",\"duration_ts\":9399264,\"duration\":\"195.818000\",\"bit_rate\":\"128006\",\"max_bit_rate\":\"128006\",\"nb_frames\":\"9181\",\"disposition\":{\"default\":1,\"dub\":0,\"original\":0,\"comment\":0,\"lyrics\":0,\"karaoke\":0,\"forced\":0,\"hearing_impaired\":0,\"visual_impaired\":0,\"clean_effects\":0,\"attached_pic\":0,\"timed_thumbnails\":0},\"tags\":{\"language\":\"und\",\"handler_name\":\"SoundHandler\"}}],\"format\":{\"nb_streams\":2,\"nb_programs\":0,\"format_name\":\"mov,mp4,m4a,3gp,3g2,mj2\",\"format_long_name\":\"QuickTime / MOV\",\"start_time\":\"0.000000\",\"duration\":\"195.852000\",\"size\":\"121481511\",\"bit_rate\":\"4962175\",\"probe_score\":100,\"tags\":{\"major_brand\":\"isom\",\"minor_version\":\"512\",\"compatible_brands\":\"isomiso2avc1mp41\",\"encoder\":\"Lavf56.18.101\"}}}"
//                , "-fops=avthumb/mp4"
                , "-cmd=avthumb/mp4"
                , "-saveas=temp:$(key)"
//                , "-pfop-config=resources/process.json"
                , "-f"
                , "-s"
                , "-i"
                , "-single"
        };
        Map<String, String> preSetMap = new HashMap<String, String>(){{
            put("f", "verify=false");
            put("s", "single=true");
            put("single", "single=true");
            put("i", "single=true");
        }};
        Map<String, String> paramsMap = ParamsUtils.toParamsMap(args, preSetMap);
        IEntryParam entryParam = new ParamsConfig(paramsMap);
        CommonParams commonParams = new CommonParams(paramsMap);
        QSuitsEntry qSuitsEntry = new QSuitsEntry(entryParam, commonParams);
        ILineProcess<Map<String, String>> processor;
        processor = qSuitsEntry.whichNextProcessor(true);
        processor.validCheck(commonParams.getMapLine());
        System.out.println(processor.processLine(commonParams.getMapLine()));
    }

    @Test
    public void testEntry3() throws Exception {
        String[] args = new String[]{
                "-interactive=true",
                "-ak=ksjadasfdljdhsjaksdfdjfgksjdsasdfsghfhfg",
                "-sk=adsjkfadsfdgfhgjjhfgdfdfsdgfhgfdsrtyhvgh"
//                , "-process=private"
                , "-process=pfop"
                , "-force-public=true"
                , "-bucket=temp"
//                , "-avinfo-index=1"
                , "-pfop-config=resources/process.json"
        };
        Map<String, String> paramsMap = ParamsUtils.toParamsMap(args, null);
        IEntryParam entryParam = new ParamsConfig(paramsMap);
        CommonParams commonParams = new CommonParams(paramsMap);
        QSuitsEntry qSuitsEntry = new QSuitsEntry(entryParam, commonParams);
        ILineProcess<Map<String, String>> processor = qSuitsEntry.whichNextProcessor(true);
        InputSource inputSource = qSuitsEntry.getScannerSource();
        inputSource.export(System.in, processor);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testEntryAliOss() throws Exception {
        IEntryParam entryParam = new ParamsConfig("resources/.ali.properties");
        CommonParams commonParams = new CommonParams(entryParam);
        QSuitsEntry qSuitsEntry = new QSuitsEntry(entryParam, commonParams);
        ILineProcess<Map<String, String>> processor = qSuitsEntry.getProcessor();
        IDataSource dataSource = qSuitsEntry.getDataSource();
        if (dataSource != null) {
            dataSource.setProcessor(processor);
            dataSource.export();
        }
        if (processor != null) processor.closeResource();
    }
}