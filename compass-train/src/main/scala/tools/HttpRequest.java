package tools;

import scala.collection.mutable.ArrayBuffer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.URL;
import java.net.URLConnection;


public class HttpRequest {
    /**
     * Get result from server using http request.
     * This is a helper function to get result from iFlytek open source NLP engine.
     *
     * @param url
     *            URL sending request to
     * @param param
     *            parameters sending with
     *
     * @return URL result from server
     */
    public static String sendGet(String url, String param) {
        String result ="";
        BufferedReader in = null;
        try {
            String urlNameString = url ;
            URL realUrl = new URL(urlNameString);

            // Open url connnection
            URLConnection connection = realUrl.openConnection();

            // Traverse all value
            connection.connect();

            in = new BufferedReader(new InputStreamReader(connection.getInputStream(),"utf-8"));

            String line;
            while ((line = in.readLine()) != null) {
                result += line;
            }
        }

        catch (Exception e) {
            System.out.println("GET exception" + e);
            e.printStackTrace();
            result="";
        }


        // Close http stream.
        finally {
            try {
                if (in != null) {
                    in.close();
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        }
        return result;
    }

}