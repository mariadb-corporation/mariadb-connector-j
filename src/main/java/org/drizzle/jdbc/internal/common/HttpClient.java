package org.drizzle.jdbc.internal.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;

public class HttpClient {
    private final URL url;
    private final HttpURLConnection connection;
    public HttpClient(String url) throws IOException {
        this.url = new URL(url);
        connection = (HttpURLConnection) this.url.openConnection();
        
    }

    public String put(InputStream is) throws IOException {
        connection.setRequestMethod("PUT");
        connection.setDoOutput(true);
        OutputStream os = connection.getOutputStream();
        byte [] buffer = new byte[10000];
        while(true) {
            int readBytes = is.read(buffer);
            if(readBytes == -1) {
                break;
            }
            os.write(buffer, 0, readBytes);
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(connection.getInputStream()));
        String toReturn = br.readLine();
        br.close();
        return toReturn;
    }

    public InputStream get() throws IOException {
        connection.setRequestMethod("GET");

        connection.setDoOutput(true);
        connection.connect();

        return connection.getInputStream(); 
    }
    public void close() {
        connection.disconnect();
    }
}
