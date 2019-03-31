package com.qiniu.process.qdora;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class PfopCommandTest {

    private PfopCommand pfopCommand;

    @Test
    @Before
    public void init() throws IOException {
        pfopCommand = new PfopCommand("avinfo", "resources" + System.getProperty("file.separator") + "pfop.json",
                true, true, "", "../temp3");
    }

    @Test
    public void testProcessLine() throws IOException, CloneNotSupportedException {
        List<Map<String,String>> testList = new ArrayList<Map<String, String>>(){{
            add(new HashMap<String, String>(){{
                put("key", "2965583.mp4");
                put("avinfo", "{streams: [{index: 0,codec_name: \"h264\",codec_long_name: \"H.264 / AVC / MPEG-4 AVC / MPEG-4 part 10\",profile: \"High\",codec_type: \"video\",codec_time_base: \"1/2\",codec_tag_string: \"avc1\",codec_tag: \"0x31637661\",width: 1280,height: 800,coded_width: 1280,coded_height: 800,has_b_frames: 1,pix_fmt: \"yuv420p\",level: 30,chroma_location: \"left\",refs: 1,is_avc: \"true\",nal_length_size: \"4\",r_frame_rate: \"1/1\",avg_frame_rate: \"1/1\",time_base: \"1/16384\",start_pts: 0,start_time: \"0.000000\",duration_ts: 13385728,duration: \"817.000000\",bit_rate: \"43708\",bits_per_raw_sample: \"8\",nb_frames: \"817\",disposition: {default: 1,dub: 0,original: 0,comment: 0,lyrics: 0,karaoke: 0,forced: 0,hearing_impaired: 0,visual_impaired: 0,clean_effects: 0,attached_pic: 0,timed_thumbnails: 0},tags: {language: \"chi\",handler_name: \"VideoHandler\"}},{index: 1,codec_name: \"aac\",codec_long_name: \"AAC (Advanced Audio Coding)\",profile: \"LC\",codec_type: \"audio\",codec_time_base: \"1/11025\",codec_tag_string: \"mp4a\",codec_tag: \"0x6134706d\",sample_fmt: \"s16p\",sample_rate: \"11025\",channels: 1,channel_layout: \"mono\",bits_per_sample: 0,r_frame_rate: \"0/0\",avg_frame_rate: \"0/0\",time_base: \"1/11025\",start_pts: 0,start_time: \"0.000000\",duration_ts: 8978947,duration: \"814.416961\",bit_rate: \"24055\",max_bit_rate: \"24055\",nb_frames: \"8771\",disposition: {default: 1,dub: 0,original: 0,comment: 0,lyrics: 0,karaoke: 0,forced: 0,hearing_impaired: 0,visual_impaired: 0,clean_effects: 0,attached_pic: 0,timed_thumbnails: 0},tags: {language: \"chi\",handler_name: \"SoundHandler\"}}],format: {nb_streams: 2,nb_programs: 0,format_name: \"mov,mp4,m4a,3gp,3g2,mj2\",format_long_name: \"QuickTime / MOV\",start_time: \"0.000000\",duration: \"817.000000\",size: \"6981769\",bit_rate: \"68364\",probe_score: 100,tags: {major_brand: \"isom\",minor_version: \"512\",compatible_brands: \"isomiso2avc1mp41\",encoder: \"Lavf57.71.100\"}}}");
            }});
        }};
        PfopCommand pfopCommand1 = pfopCommand.clone();
        pfopCommand1.processLine(testList);
        pfopCommand1.closeResource();
        System.out.println("finished.");
    }

    @After
    public void end() {
        pfopCommand.closeResource();
    }
}
