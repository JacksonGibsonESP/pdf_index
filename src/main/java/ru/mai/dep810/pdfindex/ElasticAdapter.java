package ru.mai.dep810.pdfindex;

import org.apache.commons.codec.binary.Base64;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

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
    private String settingsPath;
    private int port;
    private String hostName;
    private Client client;
    private String index;

    public String getSettingsPath() {
        return settingsPath;
    }

    public void setSettingsPath(String settingsPath) {
        this.settingsPath = settingsPath;
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
            Path path = FileSystems.getDefault().getPath(settingsPath);
            Settings settings = Settings.builder().loadFromPath(path).build();
            return new PreBuiltTransportClient(settings)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), port));
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("Cannot connect to ElasticSearch host");
        }
    }

    public void initialiseIndex() throws Exception {
        IndicesExistsResponse indicesExistsResponse = client.admin().indices().prepareExists(index).get();
        if (indicesExistsResponse.isExists()) {
            System.out.println("Removing existing index: " + index);
            DeleteIndexResponse deleteIndexResponse = client.admin().indices().prepareDelete(index).get();
            if (deleteIndexResponse.isAcknowledged()) {
                System.out.println("Index removed: " + index);
            }
        }

        indicesExistsResponse = client.admin().indices().prepareExists(index).get();
        if (!indicesExistsResponse.isExists()) {
            createIndex();
        }
    }

    private void createIndex() throws Exception {
        CreateIndexRequestBuilder createIndexRequestBuilder = client.admin().indices().prepareCreate(index);
        CreateIndexResponse createIndexResponse = createIndexRequestBuilder.get();
        if (!createIndexResponse.isAcknowledged()) {
            throw new RuntimeException("Failed to create index " + index);
        }
        System.out.println("Index created");
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

        System.out.println(indexResponse);
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
}
