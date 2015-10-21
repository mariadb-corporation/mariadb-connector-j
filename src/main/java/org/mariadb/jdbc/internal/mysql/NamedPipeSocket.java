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

    /**
     * Name pipe connection.
     * @param endpoint endPoint
     * @param timeout timeout
     * @throws IOException exception
     */
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
            public int read(byte[] bytes, int off, int len) throws IOException {
                return file.read(bytes, off, len);
            }

            @Override
            public int read() throws IOException {
                return file.read();
            }

            @Override
            public int read(byte[] bytes) throws IOException {
                return file.read(bytes);
            }
        };

        os = new OutputStream() {
            @Override
            public void write(byte[] bytes, int off, int len) throws IOException {
                file.write(bytes, off, len);
            }

            @Override
            public void write(int value) throws IOException {
                file.write(value);
            }

            @Override
            public void write(byte[] bytes) throws IOException {
                file.write(bytes);
            }
        };
    }

    public InputStream getInputStream() {
        return is;
    }

    public OutputStream getOutputStream() {
        return os;
    }

    public void setTcpNoDelay(boolean bool) {
    }

    public void setKeepAlive(boolean bool) {
    }

    public void setReceiveBufferSize(int size) {
    }

    public void setSendBufferSize(int size) {
    }

    public void setSoLinger(boolean bool, int value) {
    }

    @Override
    public void setSoTimeout(int timeout) {
    }

    public void shutdownInput() {
    }

    public void shutdownOutput() {
    }
}
