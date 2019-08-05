package com.qiniu.process.qai;

import com.qiniu.config.PropertiesFile;
import com.qiniu.util.Auth;
import org.junit.Test;

import java.io.IOException;

public class CensorManagerTest {

    @Test
    public void testCensor() throws IOException {

        PropertiesFile propertiesFile = new PropertiesFile("resources/.application.properties");
        String accessKey = propertiesFile.getValue("ak");
        String secretKey = propertiesFile.getValue("sk");
        Auth auth = Auth.create(accessKey, secretKey);
        CensorManager censorManager = new CensorManager(auth);
//        String result = censorManager.doVideoCensor("http://p3l1d5mx4.bkt.clouddn.com/-YVzTgC_I8zlDYIm8eCcPnA76pU=/ltSP7XPbPGviBNjXiZEHX7mpdm6o",
//                Scenes.PULP, 0, null, null, null);
//        System.out.println(result);
////        String
//                result = censorManager.doImageCensor("http://7xlv47.com1.z0.glb.clouddn.com/pulpsexy.jpg", Scenes.PULP);
//        System.out.println(result);
        System.out.println(censorManager.censorResult("5d4809a757d7d50008b66074").result);
    }

}