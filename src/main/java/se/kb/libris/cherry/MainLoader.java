package se.kb.libris.cherry;
// Include the Dropbox SDK.
import com.dropbox.core.*;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.pdf.PDFParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.xml.sax.ContentHandler;
import org.apache.tika.sax.*;
import org.xml.sax.SAXException;

import java.io.*;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by lisa on 23/03/15.
 */
public class MainLoader {
    public MainLoader() {
        props = new Properties();
        try {

            props.load(this.getClass().getResourceAsStream("/dropbox.properties"));
        } catch (IOException ioe){
            System.err.println("Roev. Could not find properties file.");
            ioe.printStackTrace();
        }
    }

    Properties props;
    final static ObjectMapper mapper = new ObjectMapper();

    public String getToken() throws IOException, DbxException{
        final String APP_KEY = props.getProperty("client_id");
        final String APP_SECRET = props.getProperty("client_secret");

        DbxAppInfo appInfo = new DbxAppInfo(APP_KEY, APP_SECRET);

        DbxRequestConfig config = new DbxRequestConfig("CherryPie/0.1",
                Locale.getDefault().toString());
        DbxWebAuthNoRedirect webAuth = new DbxWebAuthNoRedirect(config, appInfo);

        // Have the user sign in and authorize your app.
        String authorizeUrl = webAuth.start();
        System.out.println("1. Go to: " + authorizeUrl);
        System.out.println("2. Click \"Allow\" (you might have to log in first)");
        System.out.println("3. Copy the authorization code.");
        String code = props.getProperty("code");
        // This will fail if the user enters an invalid authorization code.
        DbxAuthFinish authFinish = webAuth.finish(code);
        String accessToken = authFinish.accessToken;
        props.setProperty("accessToken", accessToken);
        return accessToken;
    }

    void start() throws DbxException, IOException, SAXException, TikaException{
        DbxRequestConfig config = new DbxRequestConfig("CherryPie/0.1",
                Locale.getDefault().toString());
        String accessToken = props.getProperty("accessToken");
        DbxClient client = new DbxClient(config, accessToken);
        System.out.println("Linked account: " + client.getAccountInfo().displayName);

        DbxEntry.WithChildren listing = client.getMetadataWithChildren("/excerpts");
        System.out.println("Files in the path:");


        List<TextData> textContentList = new ArrayList<TextData>();

        int counter = 0;

        for (DbxEntry child : listing.children) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            System.out.println("	" + child.name + ": " + child.toString());
                /*
            String parentId = findParentId(child.name);
            if (parentId != null) {

                client.getFile(child.name, null, out);
                textContentList.add(new TextData(child.name, parentId, tikaToRide(new ByteArrayInputStream(out.toByteArray()))));
                if (counter++ % 1000 == 0) {
                    // bulk store what we have every 1000 docs
                    bulkStore(textContentList);
                    textContentList.clear();
                }
            }
                */
        }
    }

    void bulkStore(List<TextData> data) {
        // create a bulk json and put it to ES
    }

    String findParentId(String isbn) {
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost("http://localhost:9200/cherry/book/_search");
        /* This is groovy.
        Map query = [
                "query": [
                        "term" : [ "isbn" : isbn ]
                ]
        ]
        post.setEntity(new StringEntity(mapper.writeValueAsString(query)));
        */
        // This is java
        try {
        post.setEntity(new StringEntity("{ \"query\": { \"term\": { \"isbn\" : \"" + isbn + "\" } } }"));
        HttpResponse response = client.execute(post);
        HashMap<String,Map> json = mapper.readValue(response.getEntity().getContent(), new TypeReference<Map<String, Object>>() {});
            List<Map> hits = (List<Map>)json.get("hits").get("hits");
            return (String)hits.get(0).get("_id");
        } catch (NoSuchElementException nsee) {
            return null;
        } catch (UnsupportedEncodingException uee) {
            System.err.println("Roevig encoding.");
        } catch (IOException ioe) {
            System.err.println("Failed connection");
        }
        return null;
    }


    String tikaToRide(InputStream is) throws IOException, SAXException, TikaException{
        String pdfContent = null;
        try {
            ContentHandler contenthandler = new BodyContentHandler();
            Metadata metadata = new Metadata();
            PDFParser pdfparser = new PDFParser();
            pdfparser.parse(is, contenthandler, metadata, new ParseContext());
            pdfContent = contenthandler.toString();
        }

        finally {
            if (is != null) is.close();
        }
        return pdfContent;
    }

    private class TextData {
        String identifier;
        String parentId;
        String textContent;
        TextData(String id, String parent, String text) {
            this.identifier = id;
            this.parentId = parent;
            this.textContent = text;
        }
    }



    public static void main(String[] args) throws Exception {
        MainLoader ml = new MainLoader();
        //String token = ml.getToken();
        //System.out.println(token);

        ml.start();


    }

}
