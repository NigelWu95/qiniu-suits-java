package com.qiniu.entry;

import com.qiniu.datasource.IDataSource;
import com.qiniu.datasource.InputSource;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.util.ParamsUtils;
import com.qiniu.util.ProcessUtils;

import java.util.Map;
import java.util.Scanner;

public class EntryMain {

    public static boolean process_verify = true;

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        boolean single = false;
        boolean interactive = false;
        for (int i = 0; i < args.length; i++) {
            if (args[i].matches("-(s|-single|line=.?)")) {
                args[i] = "-single=true";
                single = true;
            } else if (args[i].matches("-(i|-interactive)")) {
                args[i] = "-interactive=true";
                interactive = true;
            } else if (args[i].matches("-f")) {
                args[i] = "-f=false";
                process_verify = false;
            }
        }
        QSuitsEntry qSuitsEntry = single ? new QSuitsEntry(ParamsUtils.toParamsMap(args)) : new QSuitsEntry(args);
        ILineProcess<Map<String, String>> processor = single || interactive ? qSuitsEntry.whichNextProcessor(true) :
                qSuitsEntry.getProcessor();
        if (process_verify && processor != null) {
            String process = processor.getProcessName();
            if (processor.getNextProcessor() != null) process = processor.getNextProcessor().getProcessName();
            if (ProcessUtils.isDangerous(process)) {
                System.out.println("your last process is " + process + ", are you sure? (y/n): ");
                Scanner scanner = new Scanner(System.in);
                String an = scanner.next();
                if (!an.equalsIgnoreCase("y") && !an.equalsIgnoreCase("yes")) {
                    return;
                }
            }
        }
        if (single) {
            CommonParams commonParams = qSuitsEntry.getCommonParams();
            if (processor != null) {
                processor.validCheck(commonParams.getMapLine());
                System.out.println(processor.processLine(commonParams.getMapLine()));
            }
        } else if (interactive) {
            InputSource inputSource = qSuitsEntry.getScannerSource();
            inputSource.export(System.in, processor);
        } else {
            IDataSource dataSource = qSuitsEntry.getDataSource();
            if (dataSource != null) {
                dataSource.setProcessor(processor);
                dataSource.export();
            }
        }
        if (processor != null) processor.closeResource();
    }
}
