package com.qiniu.util;

import java.io.File;
import java.util.Properties;

public class LogUtils {

    public static final String LOG_PATH = "logs";
    public static final String INFO = "info";
    public static final String ERROR = "error";
    public static final String QSUITS = "qsuits";
    public static final String PROCEDURE = "procedure";
    public static final String LOG_EXT = "log";
    public static Properties properties = System.getProperties();

    public static String getLogPath(String logName) {
        if (properties.containsKey(logName)) {
            return properties.getProperty(logName);
        } else {
            StringBuilder logPath = new StringBuilder(String.join(FileUtils.pathSeparator, LOG_PATH, logName));
            int serial = -1;
            int position = logPath.length();
            String s = "";
            File logFile = new File(String.join(".", logPath, INFO));
            if (logFile.exists()) {
                do {
                    serial++;
                    logPath.delete(position, logPath.length()).append(serial);
                    logFile = new File(String.join(".", logPath, INFO));
                } while (logFile.exists());
                s = String.valueOf(serial);
            }
            System.setProperty(logName, String.join(FileUtils.pathSeparator, LOG_PATH, logName + s));
            System.setProperty(PROCEDURE, String.join(FileUtils.pathSeparator, LOG_PATH, PROCEDURE + s));
            return logPath.toString();
        }
    }
}
