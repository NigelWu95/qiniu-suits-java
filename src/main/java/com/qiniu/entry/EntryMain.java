package com.qiniu.entry;

import com.qiniu.datasource.IDataSource;
import com.qiniu.interfaces.ILineProcess;
import java.util.Map;
import java.util.Scanner;

public class EntryMain {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        QSuitsEntry qSuitsEntry = new QSuitsEntry(args);
        ILineProcess<Map<String, String>> processor = qSuitsEntry.getProcessor();
        if (processor != null) {
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
        if (processor != null) processor.closeResource();
    }
}
