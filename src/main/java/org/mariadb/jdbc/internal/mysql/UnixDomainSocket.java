package org.mariadb.jdbc.internal.mysql;

import com.sun.jna.LastErrorException;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.Structure;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Arrays;


public class UnixDomainSocket extends Socket {
    public static final int AF_UNIX = 1;
    public static final int SOCK_STREAM = Platform.isSolaris() ? 2 : 1;
    public static final int PROTOCOL = 0;

    static {
        if (Platform.isSolaris()) {
            System.loadLibrary("nsl");
            System.loadLibrary("socket");
        }
        if (!Platform.isWindows() && !Platform.isWindowsCE()) {
            Native.register("c");
        }
    }

    String path;
    InputStream is;
    OutputStream os;
    SockAddr sockaddr;
    int fd;

    public UnixDomainSocket(String path) throws IOException {
        if (Platform.isWindows() || Platform.isWindowsCE()) {
            throw new IOException("Unix domain sockets are not supported on Windows");
        }
        this.path = path;
        sockaddr = new SockAddr(path);
        try {
            fd = socket(AF_UNIX, SOCK_STREAM, PROTOCOL);
        } catch (LastErrorException lee) {
            throw new IOException("native socket() failed : " + formatError(lee));
        }
    }

    public static native int socket(int domain, int type, int protocol) throws LastErrorException;

    public static native int connect(int sockfd, SockAddr sockaddr, int addrlen) throws LastErrorException;

    public static native int recv(int fd, byte[] buffer, int count, int flags) throws LastErrorException;

    public static native int send(int fd, byte[] buffer, int count, int flags) throws LastErrorException;

    public static native int close(int fd) throws LastErrorException;

    public static native String strerror(int errno);

    static String formatError(LastErrorException lee) {
        try {
            return strerror(lee.getErrorCode());
        } catch (Throwable t) {
            return lee.getMessage();
        }
    }

    @Override
    public void close() throws IOException {
        try {
            close(fd);
            fd = -1;
        } catch (LastErrorException lee) {
            throw new IOException("native close() failed : " + formatError(lee));
        }
    }

    @Override
    public void connect(SocketAddress endpoint) throws IOException {
        connect(endpoint, 0);
    }

    public void connect(SocketAddress endpoint, int timeout) throws IOException {
        try {
            int ret = connect(fd, sockaddr, sockaddr.size());
            if (ret != 0) {
                throw new IOException(strerror(Native.getLastError()));
            }
        } catch (LastErrorException lee) {
            throw new IOException("native connect() failed : " + formatError(lee));
        }
        is = new UnixSocketInputStream();
        os = new UnixSocketOutputStream();
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

    public void setSoTimeout(int timeout) {
    }

    public void shutdownInput() {
    }

    public void shutdownOutput() {
    }

    public static class SockAddr extends Structure {
        public short sun_family;
        public byte[] sun_path;

        public SockAddr(String sunPath) {
            sun_family = AF_UNIX;
            byte[] arr = sunPath.getBytes();
            sun_path = new byte[arr.length + 1];
            System.arraycopy(arr, 0, sun_path, 0, Math.min(sun_path.length - 1, arr.length));
            allocateMemory();
        }

        protected java.util.List getFieldOrder() {
            return Arrays.asList(new String[]{"sun_family", "sun_path"});
        }

    }

    class UnixSocketInputStream extends InputStream {

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            try {
                if (off > 0) {
                    int bytes = 0;
                    int size = (len < 10240) ? len : 10240;
                    byte[] data = new byte[size];
                    do {
                        size = (len < 10240) ? len : 10240;
                        size = recv(fd, data, size, 0);
                        if (size > 0) {
                            System.arraycopy(data, 0, b, off, size);
                            bytes += size;
                            off += size;
                            len -= size;
                        }
                    } while ((len > 0) && (size > 0));
                    return bytes;
                } else {
                    return recv(fd, b, len, 0);
                }
            } catch (LastErrorException lee) {
                throw new IOException("native read() failed : " + formatError(lee));
            }
        }

        @Override
        public int read() throws IOException {
            byte[] b = new byte[1];
            int bytesRead = read(b);
            if (bytesRead == 0) return -1;
            return b[0] & 0xff;
        }

        @Override
        public int read(byte[] b) throws IOException {
            return read(b, 0, b.length);
        }
    }

    class UnixSocketOutputStream extends OutputStream {

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            int bytes = 0;
            try {
                if (off > 0) {
                    int size = (len < 10240) ? len : 10240;
                    byte[] data = new byte[size];
                    do {
                        size = (len < 10240) ? len : 10240;
                        System.arraycopy(b, off, data, 0, size);
                        bytes = send(fd, data, size, 0);
                        if (bytes > 0) {
                            off += bytes;
                            len -= bytes;
                        }
                    } while ((len > 0) && (bytes > 0));
                } else {
                    bytes = send(fd, b, len, 0);
                }

                if (bytes != len) throw new IOException("can't write " + len + "bytes");
            } catch (LastErrorException lee) {
                throw new IOException("native write() failed : " + formatError(lee));
            }
        }

        @Override
        public void write(int b) throws IOException {
            write(new byte[]{(byte) b});
        }

        @Override
        public void write(byte[] b) throws IOException {
            write(b, 0, b.length);
        }
    }
}
