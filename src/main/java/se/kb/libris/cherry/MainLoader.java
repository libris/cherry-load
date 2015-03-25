package se.kb.libris.cherry;
// Include the Dropbox SDK.
import com.dropbox.core.*;
import java.io.*;
import java.util.Locale;
import java.util.Properties;

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

    public void start() throws IOException, DbxException{
        // Get your app key and secret from the Dropbox developers website.
        final String APP_KEY = props.getProperty("user");
        final String APP_SECRET = props.getProperty("password");

        DbxAppInfo appInfo = new DbxAppInfo(APP_KEY, APP_SECRET);

        DbxRequestConfig config = new DbxRequestConfig("JavaTutorial/1.0",//vad vi kallar appen
                Locale.getDefault().toString());
        DbxWebAuthNoRedirect webAuth = new DbxWebAuthNoRedirect(config, appInfo);

        // Have the user sign in and authorize your app.
        String authorizeUrl = webAuth.start();
        System.out.println("1. Go to: " + authorizeUrl);
        System.out.println("2. Click \"Allow\" (you might have to log in first)");
        System.out.println("3. Copy the authorization code.");
        String code = new BufferedReader(new InputStreamReader(System.in)).readLine().trim();

        // This will fail if the user enters an invalid authorization code.
        DbxAuthFinish authFinish = webAuth.finish(code);
        String accessToken = authFinish.accessToken;

        DbxClient client = new DbxClient(config, accessToken);

        System.out.println("Linked account: " + client.getAccountInfo().displayName);


        DbxEntry.WithChildren listing = client.getMetadataWithChildren("/");
        System.out.println("Files in the root path:");
        for (DbxEntry child : listing.children) {
            System.out.println("	" + child.name + ": " + child.toString());

        }

        FileOutputStream outputStream = new FileOutputStream("magnum-opus.txt");
        try {
            DbxEntry.File downloadedFile = client.getFile("/magnum-opus.txt", null,
                    outputStream);
            System.out.println("Metadata: " + downloadedFile.toString());
        } finally {
            outputStream.close();
        }
    }



    public static void main(String[] args) throws IOException, DbxException {
        MainLoader ml = new MainLoader();
        ml.start();
    }

}
