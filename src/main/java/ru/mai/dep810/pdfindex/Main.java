package ru.mai.dep810.pdfindex;

import ru.mai.dep810.pdfindex.logger.PdfIndexLogger;

import java.nio.file.*;
import java.util.logging.Logger;
import java.util.stream.Stream;

/**
 * Created by Никита on 10.12.2016.
 */

/*
Install ingest attachment plugin and type this command to Kibana console before all:
PUT _ingest/pipeline/attachment
{
  "description" : "Extract attachment information",
  "processors" : [
    {
      "attachment" : {
        "field" : "data",
        "indexed_chars" : "-1"
      }
    }
  ]
}
*/

/*
This is the way to clean up all little htmls trash:
POST crystal/_delete_by_query
{
  "query": {
    "range" : {
            "attachment.content_length" : {
                "lte" : 1000
            }
    }
  }
}
*/

public class Main {
    public static void main(String[] args) throws Exception {
        ElasticAdapter elastic = new ElasticAdapter();
        elastic.setHostName("localhost");
        elastic.setPort(9300);
        elastic.setIndex("crystal");
        elastic.setSettingsPath("./src/main/resources/elasticsearch.yml");
        elastic.initClient();
        elastic.initialiseIndex();
        try(Stream<Path> paths = Files.walk(Paths.get(args[0]))) {
            Logger logger = PdfIndexLogger.getLogger(Main.class.getName());
            paths.forEach(filePath -> {
                if (Files.isRegularFile(filePath)) {
                    try {
                        PathMatcher pdfMatcher = FileSystems.getDefault().getPathMatcher("glob:**.pdf");
                        PathMatcher mhtMatcher = FileSystems.getDefault().getPathMatcher("glob:**.mht");
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
                        }
                        else if (mhtMatcher.matches(filePath)) {
                            logger.info("Trying to add: " + filePath);
                            elastic.addMHTDocumentToIndex(filePath.toString());
                        }
                        else {
                            logger.info("Escaped: " + filePath);
                        }
                    } catch (Exception e) {
                        logger.warning("Error: " + e);
                    }
                }
                else {
                    logger.warning("Not regular file: " + filePath);
                }
            });
        }
        catch (Throwable e){
            e.printStackTrace();
        }
    }
}
