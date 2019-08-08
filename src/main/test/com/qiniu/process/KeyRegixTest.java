package com.qiniu.process;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.qiniu.config.JsonFile;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URL;

public class KeyRegixTest {

    private JsonFile jsonFile;

    @Before
    public void init() throws IOException {
        jsonFile = new JsonFile("resources" + System.getProperty("file.separator") + ".soyoung.json");
    }

    @Test
    public void testParse() throws IOException {
        JsonArray jsonArray = jsonFile.getElement("routers").getAsJsonArray();
        String line = "https://xxx.com/tieba/android/20140910/1/20140910083929514.jpg";
        URL url = new URL(line);
        String path = url.getPath();
        System.out.println(url.getFile());
        String key;
//        Pattern pattern;
//        Matcher matcher;
        String pattern;
        String repl;
        for (JsonElement jsonElement : jsonArray) {
//            pattern = Pattern.compile(jsonElement.getAsJsonObject().get("pattern").getAsString());
//            matcher = pattern.matcher(path);
//            if (matcher.matches()) {
//                System.out.println(repl);
//                System.out.println(matcher.group());
//                System.out.println(matcher.group(1));
//                System.out.println(matcher.group(2));
//                break;
//            }
            pattern = jsonElement.getAsJsonObject().get("pattern").getAsString();
            repl = jsonElement.getAsJsonObject().get("repl").getAsString().replace("[{}]", "");
            if (path.matches(pattern)) {
                key = path.replace(pattern, repl).substring(1);
                System.out.println(path + "\t" + key);
            }
        }
    }
}
