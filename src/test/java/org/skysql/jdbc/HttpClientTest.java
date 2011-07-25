package org.skysql.jdbc;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.skysql.jdbc.internal.common.HttpClient;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

import static org.junit.Assert.assertEquals;

/**
 * Created by IntelliJ IDEA. User: marcuse Date: Aug 9, 2010 Time: 9:04:39 PM To change this template use File |
 * Settings | File Templates.
 */
@Ignore
public class HttpClientTest {
    //@Test
    public void testGet() throws IOException {

        HttpHandler handler = new HttpHandler() {
            public void handle(HttpExchange httpExchange) throws IOException {
                if (httpExchange.getRequestMethod().equalsIgnoreCase("GET")) {
                    Headers responseHeaders = httpExchange.getResponseHeaders();
                    responseHeaders.set("Content-Type", "text/plain");
                    httpExchange.sendResponseHeaders(200, 0);
                    OutputStream responseBody = httpExchange.getResponseBody();
                    responseBody.write("hhhhh".getBytes());
                    responseBody.close();
                }
            }
        };
        InetSocketAddress isa = new InetSocketAddress(9998);
        HttpServer server = HttpServer.create(isa,0);
        server.createContext("/apa", handler);
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();

        HttpClient httpClient = new HttpClient("http://localhost:9998/apa");
        InputStream is = httpClient.get();

        byte[] b = new byte[is.available()];
        is.read(b);
        assertEquals("hhhhh", new String(b));
        server.stop(0);
    }

   // @Test
    public void testPut() throws IOException {

        HttpHandler handler = new HttpHandler() {
            public void handle(HttpExchange httpExchange) throws IOException {
                if (httpExchange.getRequestMethod().equalsIgnoreCase("PUT")) {
                    InputStream is = httpExchange.getRequestBody();
                    byte [] buf = new byte[1000];
                    int len = is.read(buf);
                    String s = new String(buf, 0, len);
                    Headers responseHeaders = httpExchange.getResponseHeaders();
                    responseHeaders.set("Content-Type", "text/plain");
                    httpExchange.sendResponseHeaders(200, 0);
                    OutputStream responseBody = httpExchange.getResponseBody();
                    responseBody.write(s.toLowerCase().getBytes());
                    responseBody.close();
                }
            }
        };
        InetSocketAddress isa = new InetSocketAddress(9999);
        HttpServer server = HttpServer.create(isa,0);
        server.createContext("/apa", handler);
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();

        HttpClient httpClient = new HttpClient("http://localhost:9999/apa");
        ByteArrayInputStream bais = new ByteArrayInputStream("HEJ HEJ".getBytes());
        String ret = httpClient.put(bais);
        assertEquals("hej hej", ret);
        server.stop(0);

    }
}
