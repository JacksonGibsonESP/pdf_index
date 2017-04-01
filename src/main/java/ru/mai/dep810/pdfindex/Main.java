package ru.mai.dep810.pdfindex;

import java.nio.file.*;
import java.util.stream.Stream;

/**
 * Created by Никита on 10.12.2016.
 */

/*
Install ingest attachment plugin and type this command to Kibana console beforeall:
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
            paths.forEach(filePath -> {
                if (Files.isRegularFile(filePath)) {
                    try {
                        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:**.pdf");
                        if (matcher.matches(filePath)) {
                            elastic.addDocumentToIndex(filePath.toString());
                            System.out.println(filePath + " added");
                        }
                        matcher = FileSystems.getDefault().getPathMatcher("glob:**.mht");
                        if (matcher.matches(filePath)) {
                            elastic.addMHTDocumentToIndex(filePath.toString());
                            System.out.println(filePath + " added");
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
    }
}
