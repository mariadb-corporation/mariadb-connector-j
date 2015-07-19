package org.mariadb.jdbc.failover;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.BindException;
import java.net.ServerSocket;
import java.net.Socket;

public class TcpProxySocket implements Runnable {
    protected final static Logger log = LoggerFactory.getLogger(TcpProxySocket.class);

    String host;
    int remoteport;
    int localport;
    boolean stop = false;
    Socket client = null, server = null;
    ServerSocket ss;

    public TcpProxySocket(String host, int remoteport) throws IOException {
        this.host = host;
        this.remoteport = remoteport;
        ss = new ServerSocket(0);
        this.localport = ss.getLocalPort();
    }

    public int getLocalPort() {
        return ss.getLocalPort();
    }

    public boolean isClosed() {
        return ss.isClosed();
    }

    public void kill() {
        stop = true;
        try {
            if (server != null) server.close();
        } catch (IOException e) { }
        try {
            if (client != null) client.close();
        } catch (IOException e) { }
        try {
            ss.close();
        } catch (IOException e) { }
    }

    @Override
    public void run() {

        stop = false;
        try {
            try {
                if (ss.isClosed()) ss = new ServerSocket(localport);
            } catch (BindException b) {
                //in case for testing crash and reopen too quickly
                try {
                    Thread.sleep(100);
                } catch (InterruptedException i) { }
                if (ss.isClosed()) ss = new ServerSocket(localport);
            }
            final byte[] request = new byte[1024];
            byte[] reply = new byte[4096];
            while (!stop) {
                try {
                    client = ss.accept();
                    final InputStream from_client = client.getInputStream();
                    final OutputStream to_client = client.getOutputStream();
                    try {
                        server = new Socket(host, remoteport);
                    } catch (IOException e) {
                        PrintWriter out = new PrintWriter(new OutputStreamWriter(to_client));
                        out.println("Proxy server cannot connect to " + host + ":" +
                                remoteport + ":\n" + e);
                        out.flush();
                        client.close();
                        continue;
                    }
                    final InputStream from_server = server.getInputStream();
                    final OutputStream to_server = server.getOutputStream();
                    new Thread() {
                        public void run() {
                            int bytes_read;
                            try {
                                while ((bytes_read = from_client.read(request)) != -1) {
                                    to_server.write(request, 0, bytes_read);
                                    log.trace(bytes_read + "to_server--->" + new String(request, "UTF-8") + "<---");
                                    to_server.flush();
                                }
                            } catch (IOException e) {
                            }
                            try {
                                to_server.close();
                            } catch (IOException e) { }
                        }
                    }.start();
                    int bytes_read;
                    try {
                        while ((bytes_read = from_server.read(reply)) != -1) {
                            try {
                                Thread.sleep(1);
                                log.trace(bytes_read + " to_client--->" + new String(reply, "UTF-8") + "<---");
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                            }
                            to_client.write(reply, 0, bytes_read);
                            to_client.flush();
                        }
                    } catch (IOException e) {
                    }
                    to_client.close();
                } catch (IOException e) {
                    //System.err.println("ERROR socket : "+e);
                }
                finally {
                    try {
                        if (server != null) server.close();
                        if (client != null) client.close();
                    } catch (IOException e) {
                    }
                }
            }
        } catch ( IOException e) {
            e.printStackTrace();
        }
    }
}
