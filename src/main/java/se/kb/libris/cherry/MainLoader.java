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
import org.apache.tika.parser.txt.CharsetDetector;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.xml.sax.ContentHandler;
import org.apache.tika.sax.*;
import org.xml.sax.SAXException;
import static java.lang.System.*;

import java.io.*;
import java.lang.reflect.Array;
import java.text.Normalizer;
import java.util.*;

/**
 * Created by lisa on 23/03/15.
 */
public class MainLoader {

    final static ObjectMapper mapper = new ObjectMapper();


    void startFiles() throws DbxException, IOException, SAXException, TikaException{
        System.out.println("Files in the path:");
        File basedir = new File("/tmp/archive");

        List<TextData> textContentList = new ArrayList<TextData>();
        int counter = 0;

        for (File child : basedir.listFiles()) {

            System.out.println("	" + child.getName());
            String isbn = child.getName().substring(0, child.getName().lastIndexOf(".pdf"));
            String parentId = findParentId(isbn);
            //String parentId = findParentId(child.name);
            if (parentId != null) {

                textContentList.add(new TextData(isbn, parentId, tikaToRide(new FileInputStream(child))));
                //textContentList.add(new TextData(child.name, parentId, tikaToRide(new FileInputStream("/tmp/pdffen.pdf"))));

                if (++counter % 1 == 0) {
                    // bulk store what we have every 1000 docs
                    bulkStore(textContentList);
                    textContentList.clear();
                    break;
                }
            }

        }
    }

    String findParentId(String isbn) {
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost("http://hp01.libris.kb.se:9200/cherry/record/_search");
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
            ContentHandler contenthandler = new BodyContentHandler(1010241024);
            Metadata metadata = new Metadata();
            PDFParser pdfparser = new PDFParser();
            pdfparser.parse(is, contenthandler, metadata, new ParseContext());
            pdfContent = normalizeString(contenthandler.toString()); //.replaceAll("[^\\p{Print}&&\\p{Alnum}]|[\\x6d\\t\\n\\x0B\\f\\r\\x85\\u2028\\u2029]", " ").replace((char)160, ' ').trim();

        }

        finally {
            if (is != null) is.close();
        }
        return pdfContent;
    }

    String normalizeString(String inString) throws UnsupportedEncodingException {
        CharsetDetector cs = new CharsetDetector();
        cs.setText(inString.getBytes());
        String sourceEncoding = cs.detect().getName();
        out.println("source encoding: "+sourceEncoding);

        return inString.replaceAll("[\\h\\v]", " ").replaceAll("\\s+", " ").trim();
    }

    void bulkStore(List<TextData> data) throws IOException {
        StringBuilder esdoc = new StringBuilder();
        for (TextData item : data){
            String idString = "smakprov:" + item.identifier;
            esdoc.append("{\"index\":{\"_id\":\""+idString+"\",\"parent\":\""+item.parentId+"\",\"_type\":\"excerpt\"}}\n");
            Map textMap = new HashMap<String,Object>();
            textMap.put("text", item.textContent);
            textMap.put("@type", "Excerpt");
            Map annotationSource = new HashMap<String, String>();
            annotationSource.put("name", "Smakprov");
            annotationSource.put("url", "http://www.smakprov.se/bok/"+item.identifier);
            textMap.put("annotationSource", annotationSource);
            esdoc.append(mapper.writeValueAsString(textMap)+"\n");
            //esdoc.append("{\"text\":\""+item.textContent+"\"}\n");
            System.out.println(esdoc.toString());
        }
        HttpClient client = HttpClientBuilder.create().build();
        HttpPost post = new HttpPost("http://localhost:9200/cherry/_bulk");
        post.setEntity(new StringEntity(esdoc.toString(), "UTF-8"));
        HttpResponse response = client.execute(post);
        HashMap<String,Map> json = mapper.readValue(response.getEntity().getContent(), new TypeReference<Map<String, Object>>() {});
        String result = mapper.writeValueAsString(json);
        System.out.println("Bulk finished");
        System.out.println(result);
        System.out.println("After result");

    }


    public static void main(String[] args) throws Exception {
        MainLoader ml = new MainLoader();
        //String token = ml.getToken();
        //System.out.println(token);

        ml.startFiles();


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

}
