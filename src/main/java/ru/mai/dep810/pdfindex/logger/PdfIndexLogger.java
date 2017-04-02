package ru.mai.dep810.pdfindex.logger;

import java.util.logging.*;

public class PdfIndexLogger {
    private static FileHandler fh;
    private static SimpleFormatter formatter = new SimpleFormatter();

    static {
        try{
            fh = new FileHandler("./MyLogFile.log");
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

    public static Logger getLogger(String className) throws Exception{
        Logger logger = Logger.getLogger(className);
        logger.addHandler(fh);
        fh.setFormatter(formatter);
        return logger;
    }
}
