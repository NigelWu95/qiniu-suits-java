package com.qiniu.process.qiniu;

import com.qiniu.config.PropertiesFile;
import com.qiniu.http.Response;
import com.qiniu.storage.Configuration;
import com.qiniu.util.Auth;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.*;

public class CdnHelperTest {

    @Test
    public void queryRefresh() throws IOException {

        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        Auth auth = Auth.create(accessKey, secretKey);
        CdnHelper cdnHelper = new CdnHelper(auth, new Configuration());
        Response response = cdnHelper.queryRefresh(new String[]{
                "http://xxx.com/2.mp3",
//                "http://xxx.com/181224-观点峰会-1.mp3",
                "http://xxx.com/VoiceLibrary_MTQ2Njg2Mw"
        });
        System.out.println(response.bodyString());
    }
}