package ru.mai.dep810.pdfindex;

import org.apache.commons.codec.binary.Base64;
import org.apache.james.mime4j.dom.Entity;
import org.apache.james.mime4j.dom.Message;
import org.apache.james.mime4j.dom.Multipart;
import org.apache.james.mime4j.dom.TextBody;
import org.apache.james.mime4j.dom.field.ContentTypeField;
import org.apache.james.mime4j.message.DefaultMessageBuilder;
import org.apache.james.mime4j.stream.MimeConfig;
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

    private static String parseMhtFile(File mhtFile) throws IOException {
        MimeConfig parserConfig  = new MimeConfig();
        parserConfig.setMaxHeaderLen(-1); // The default is a mere 10k
        parserConfig.setMaxLineLen(-1); // The default is only 1000 characters.
        parserConfig.setMaxHeaderCount(-1); // Disable the check for header count.
        DefaultMessageBuilder builder = new DefaultMessageBuilder();
        builder.setMimeEntityConfig(parserConfig);

        InputStream mhtStream = new FileInputStream(mhtFile);
        Message message = builder.parseMessage(mhtStream);

        Multipart multipart = (Multipart) message.getBody();
        StringBuilder result = new StringBuilder();
        String charSetName = "windows-1251";
        int ch;
        for (Entity e : multipart.getBodyParts()){
            if (((ContentTypeField) e.getHeader().getField("content-type")).getMimeType().equals("text/html")){
                charSetName = ((ContentTypeField) e.getHeader().getField("content-type")).getCharset();
                Reader reader = ((TextBody) e.getBody()).getReader();
                do {
                    ch = reader.read();
                    if (ch != -1) {
                        result.append((char) ch);
                    }
                } while (ch != -1);
            }
        }
        return Base64.encodeBase64String(result.toString().getBytes(charSetName));
    }

    public void addMHTDocumentToIndex(String path) throws Exception {
        File file = new File(path);
        String fileContents = parseMhtFile(file);

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
}
