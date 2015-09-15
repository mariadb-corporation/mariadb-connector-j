package org.mariadb.jdbc.internal.mysql;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.net.SocketAddress;

public class NamedPipeSocket extends Socket {
    String host;
    String name;

    RandomAccessFile file;
    InputStream is;
    OutputStream os;

    public NamedPipeSocket(String host, String name) {
        this.host = host;
        this.name = name;
    }

    @Override
    public void close() throws IOException {
        if (file != null) {
            file.close();
            file = null;
        }
    }

    @Override
    public void connect(SocketAddress endpoint) throws IOException {
        connect(endpoint, 0);
    }

    public void connect(SocketAddress endpoint, int timeout) throws IOException {
        String filename;
        if (host == null || host.equals("localhost")) {
            filename = "\\\\.\\pipe\\" + name;
        } else {
            filename = "\\\\" + host + "\\pipe\\" + name;
        }
        file = new RandomAccessFile(filename, "rw");

        is = new InputStream() {
            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return file.read(b, off, len);
            }

            @Override
            public int read() throws IOException {
                return file.read();
            }

            @Override
            public int read(byte[] b) throws IOException {
                return file.read(b);
            }
        };

        os = new OutputStream() {
            @Override
            public void write(byte[] b, int off, int len) throws IOException {
                file.write(b, off, len);
            }

            @Override
            public void write(int b) throws IOException {
                file.write(b);
            }

            @Override
            public void write(byte[] b) throws IOException {
                file.write(b);
            }
        };
    }

    public InputStream getInputStream() {
        return is;
    }

    public OutputStream getOutputStream() {
        return os;
    }

    public void setTcpNoDelay(boolean b) {
    }

    public void setKeepAlive(boolean b) {
    }

    public void setReceiveBufferSize(int size) {
    }

    public void setSendBufferSize(int size) {
    }

    public void setSoLinger(boolean b, int i) {
    }

    @Override
    public void setSoTimeout(int timeout) {
    }

    public void shutdownInput() {
    }

    public void shutdownOutput() {
    }
}
