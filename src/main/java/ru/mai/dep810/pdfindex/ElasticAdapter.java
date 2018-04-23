package ru.mai.dep810.pdfindex;

import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Date;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by Никита on 10.12.2016.
 */
public class ElasticAdapter {
    private String settingsResourceName;
    private int port;
    private String hostName;
    private Client client;
    private String index;
    private Logger logger = LoggerFactory.getLogger(ElasticAdapter.class);

    public String getSettingsResourceName() {
        return settingsResourceName;
    }

    public void setSettingsResourceName(String settingsResourceName) {
        this.settingsResourceName = settingsResourceName;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public String getIndex() {
        return index;
    }

    public void setIndex(String index) {
        this.index = index;
    }

    public void initClient() throws Exception {
        client = getObject();
    }

    public Client getObject() throws Exception {
        try {
            Settings settings = Settings.builder().loadFromStream(settingsResourceName,
                    this.getClass().getResourceAsStream(settingsResourceName), true).build();
            return new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(hostName), port));
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Cannot connect to ElasticSearch host");
        }
    }

    public void initialiseIndex() throws Exception {
        IndicesExistsResponse indicesExistsResponse = client.admin().indices().prepareExists(index).get();
        if (indicesExistsResponse.isExists()) {
            logger.info("Removing existing index: " + index);
            DeleteIndexResponse deleteIndexResponse = client.admin().indices().prepareDelete(index).get();
            if (deleteIndexResponse.isAcknowledged()) {
                logger.info("Index removed: " + index);
            }
        }

        indicesExistsResponse = client.admin().indices().prepareExists(index).get();
        if (!indicesExistsResponse.isExists()) {
            createIndex();
        }
    }

    private void createIndex() throws Exception {
        CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(index)
                .setSettings(getSettings())
                .addMapping("article", getMapping());
        CreateIndexResponse createIndexResponse = createIndexRequestBuilder.get();
        if (!createIndexResponse.isAcknowledged()) {
            throw new RuntimeException("Failed to create index " + index);
        }
        logger.info("Index created: " + index);
    }

    private XContentBuilder getSettings() throws Exception {
        String []filters = {"standard", "lowercase", "crystal_synonyms", "english_stopwords", "russian_stopwords", "english_stemmer",  "russian_stemmer"};
        return jsonBuilder()
                .startObject()
                    .field("index.analysis.analyzer.crystal.tokenizer", "standard")
                    .field("index.analysis.analyzer.crystal.filter", filters)
                    .field("index.analysis.filter.crystal_synonyms.type", "synonym")
                    .field("index.analysis.filter.crystal_synonyms.synonyms_path", "analysis/synonym.txt")
                    .field("index.analysis.filter.english_stopwords.type", "stop")
                    .field("index.analysis.filter.english_stopwords.stopwords", "_english_")
                    .field("index.analysis.filter.russian_stopwords.type", "stop")
                    .field("index.analysis.filter.russian_stopwords.stopwords", "_russian_")
                    .field("index.analysis.filter.russian_stemmer.type", "stemmer")
                    .field("index.analysis.filter.russian_stemmer.name", "russian")
                    .field("index.analysis.filter.english_stemmer.type", "stemmer")
                    .field("index.analysis.filter.english_stemmer.name", "english")
                .endObject();
    }

    private XContentBuilder getMapping() throws Exception {
        return jsonBuilder()
                .startObject()
                    .field("properties")
                        .startObject()
                            .field("attachment.content")
                                .startObject()
                                    .field("type", "text")
                                    .field("analyzer", "crystal")
                                .endObject()
                        .endObject()
                .endObject();
    }

    public void addDocumentToIndex(String path) throws Exception {
        File file = new File(path);
        String fileContents = readContent(file);

        IndexResponse indexResponse = client.prepareIndex(index, "article")
                .setPipeline("attachment")
                .setId(file.getName())
                .setSource(jsonBuilder()
                        .startObject()
                        .field("data", fileContents)
                        .field("inserted_at", new Date())
                        .field("path", path)
                        .endObject()
                )
                .execute()
                .actionGet();

        logger.info(indexResponse.status().toString());
    }

    private static String readContent(File file) throws Exception {

        ByteArrayOutputStream output = new ByteArrayOutputStream();
        InputStream input = new BufferedInputStream(new FileInputStream(file));

        int read = -1;

        while ((read = input.read()) != -1) {
            output.write(read);
        }

        input.close();

        return Base64.encodeBase64String(output.toByteArray());
    }

    public void deleteTrash(){
        BulkByScrollResponse response =
                DeleteByQueryAction.INSTANCE.newRequestBuilder(client)
                        .filter(QueryBuilders.rangeQuery("attachment.content_length").lte(1000))
                        .source(index)
                        .get();
        logger.info(String.valueOf(response.getDeleted()) + " documents deleted");
    }
}
