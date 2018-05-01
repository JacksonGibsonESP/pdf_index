package ru.mai.dep810.pdfindex;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.*;
import java.util.stream.Stream;

/**
 * Created by Никита on 10.12.2016.
 */

public class Main {
    public static void main(String[] args) throws Exception {
        Logger logger = LoggerFactory.getLogger(Main.class);
        logger.info("Application started");
        ElasticAdapter elastic = new ElasticAdapter();
        elastic.setHostName("localhost");
        elastic.setPort(9300);
        elastic.setPortREST(9200);
        elastic.setIndex("crystal");
        elastic.setSettingsResourceName("elasticsearch.yml");
        elastic.initClient();
        elastic.initialisePipeline();
        elastic.initialiseIndex();
        try(Stream<Path> paths = Files.walk(Paths.get(args[0]))) {
            paths.forEach(filePath -> {
                if (Files.isRegularFile(filePath)) {
                    try {
                        PathMatcher pdfMatcher = FileSystems.getDefault().getPathMatcher("glob:**.pdf");
                        PathMatcher docMatcher = FileSystems.getDefault().getPathMatcher("glob:**.doc");
                        PathMatcher docxMatcher = FileSystems.getDefault().getPathMatcher("glob:**.docx");
                        PathMatcher rtfMatcher = FileSystems.getDefault().getPathMatcher("glob:**.rtf");
                        PathMatcher htmlMatcher = FileSystems.getDefault().getPathMatcher("glob:**.html");
                        PathMatcher htmMatcher = FileSystems.getDefault().getPathMatcher("glob:**.htm");
                        PathMatcher odtMatcher = FileSystems.getDefault().getPathMatcher("glob:**.odt");
                        if (pdfMatcher.matches(filePath) || docMatcher.matches(filePath)
                                || docxMatcher.matches(filePath) || rtfMatcher.matches(filePath)
                                || htmlMatcher.matches(filePath) || htmMatcher.matches(filePath)
                                || odtMatcher.matches(filePath)) {
                            logger.info("Trying to add: " + filePath);
                            elastic.addDocumentToIndex(filePath.toString());
                        } else {
                            logger.info("Escaped: " + filePath);
                        }
                    } catch (Exception e) {
                        logger.error("Error: " + e);
                    }
                }
                else {
                    logger.info("Not regular file: " + filePath);
                }
            });
            elastic.deleteTrash();
            logger.info("Application successfully finished");
        }
        catch (Throwable e){
            logger.error(e.toString());
            logger.info("Application interrupted");
        }
    }
}
