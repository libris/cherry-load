package se.kb.libris.cherry;


import java.util.Properties;


import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;


public class Main {
    Properties props;

    public Main() {
        // Load properties
        props = new Properties();
    }

    void start() {
        DbxClient client = new DbxClient(config, accessToken);
        System.out.println("Linked account: " + client.getAccountInfo().displayName);

        DbxEntry.WithChildren listing = client.getMetadataWithChildren("/directory/");
        System.out.println("Files in the path:");


        List<TextData> textContentList = new ArrayList<TextData>();

        int counter = 0;

        for (DbxEntry child : listing.children) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();

            String parentId = findParentId(child.name);
            if (parentId != null) {
                System.out.println("	" + child.name + ": " + child.toString());
                client.getFile(child.name, null, out);
                textContentList.add(new TextData(child.name, parentId, tikaToRide(new ByteArrayInputStream(out.toByteArray()))));
                if (counter++ % 1000 == 0) {
                    // bulk store what we have every 1000 docs
                    bulkStore(textContentList);
                    textContentList.clear();
                }
            }
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
        post.setEntity(new StringEntity("{ \"query\": { \"term\": { \"isbn\" : \"" + isbn + "\" } } }");
        HttpResponse response = client.execute(post);
        HashMap<String,Object> json = mapper.readValue(response.getEntity().getContent(), new TypeReference<Map<String, Object>>() {});
        try {
            return json.get("hits").get("hits").first().get("_id");
        } catch (NoSuchElementException nsee) {
            return null
        }
    }


    String tikaToRide(InputStream is) {
        try {
            ContentHandler contenthandler = new BodyContentHandler();
            Metadata metadata = new Metadata();
            PDFParser pdfparser = new PDFParser();
            pdfparser.parse(is, contenthandler, metadata, new ParseContext());
            return contenthandler.toString();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            if (is != null) is.close();
        }
    }

    private class TextData {
        String identifier;
        String parentId;
        String textContent;
        DocumentInfo(String id, String parent, String text) {
            this.identifier = id;
            this.parentId = parent;
            this.textContent = text;
        }
    }

}
