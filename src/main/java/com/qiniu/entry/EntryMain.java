package com.qiniu.entry;

import com.qiniu.datasource.IDataSource;
import com.qiniu.datasource.ScannerSource;
import com.qiniu.interfaces.ILineProcess;
import com.qiniu.util.ParamsUtils;

import java.io.IOException;
import java.util.Map;
import java.util.Scanner;

public class EntryMain {

    public static boolean process_verify = true;

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        boolean interactive = false;
        boolean single = false;
        for (int i = 0; i < args.length; i++) {
            if (args[i].matches("-(i|-interactive)")) {
                args[i] = "-interactive=true";
                interactive = true;
            } else if (args[i].matches("-(s|-single|line=.?)")) {
                args[i] = "-single=true";
                single = true;
            }
        }
        QSuitsEntry qSuitsEntry = single ? new QSuitsEntry(ParamsUtils.toParamsMap(args)) : new QSuitsEntry(args);
        ILineProcess<Map<String, String>> processor;
        if (single) {
            processor = qSuitsEntry.whichNextProcessor(true);
            if (processor == null) throw new IOException("no process defined.");
            CommonParams commonParams = qSuitsEntry.getCommonParams();
            System.out.println(processor.processLine(commonParams.getMapLine()));
        } else if (interactive) {
            processor = qSuitsEntry.whichNextProcessor(true);
            if (processor == null) throw new IOException("no process defined.");
            ScannerSource scannerSource = qSuitsEntry.getScannerSource();
            Scanner scanner = new Scanner(System.in);
            scannerSource.export(scanner, processor);
        } else {
            processor = qSuitsEntry.getProcessor();
            if (process_verify && processor != null) {
                String process = processor.getProcessName();
                if (processor.getNextProcessor() != null) {
                    process += " and " + processor.getNextProcessor().getProcessName();
                }
                System.out.println("your current process is " + process + ", are you sure? (y/n): ");
                Scanner scanner = new Scanner(System.in);
                String an = scanner.next();
                if (!an.equalsIgnoreCase("y") && !an.equalsIgnoreCase("yes")) {
                    return;
                }
            }
            IDataSource dataSource = qSuitsEntry.getDataSource();
            if (dataSource != null) {
                dataSource.setProcessor(processor);
                dataSource.export();
            }
        }
        if (processor != null) processor.closeResource();
    }
}
