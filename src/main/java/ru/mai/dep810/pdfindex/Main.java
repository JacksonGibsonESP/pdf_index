package ru.mai.dep810.pdfindex;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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
        "field" : "data"
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
        elastic.setSettingsPath("D:/Workspace/pdf_index/src/main/resources/elasticsearch.yml");
        elastic.initClient();
//        elastic.initialiseIndex();
//        elastic.addDocumentToIndex("D:\\Диплом\\PhysRevB.48.22.pdf");
//        elastic.addDocumentToIndex("D:\\Диплом\\Molecular-dynamics-of-binary-metal-nitrides-and-ternary-oxynitrides_2009_Physica-B-Condensed-Matter (1).pdf");
        try(Stream<Path> paths = Files.walk(Paths.get(args[0]))) {
            paths.forEach(filePath -> {
                if (Files.isRegularFile(filePath)) {
                    try {
                        elastic.addDocumentToIndex(filePath.toString());
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println(filePath + " added");
                }
            });
        }
    }
}
